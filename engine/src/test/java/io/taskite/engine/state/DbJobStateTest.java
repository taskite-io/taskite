/*
 * Copyright (c) 2026 lzgabel and Taskite contributors
 *
 * Licensed under the Taskite Community License, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     https://github.com/taskite-io/taskite/blob/main/LICENSE
 *
 * The Software is source-available for non-commercial use and limited evaluation
 * use. Other commercial use requires a separate written license. The Software is
 * provided "AS IS", without warranties or conditions of any kind.
 */
package io.taskite.engine.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.taskite.db.impl.DbCompositeKey;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbNil;
import io.taskite.db.impl.DbString;
import io.taskite.engine.state.immutable.JobState.State;
import io.taskite.engine.state.job.DbJobState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exercises {@link DbJobState} over a real {@link PartitionStorage} + transactional RocksDB, so the
 * composite / foreign / tenant-aware key paths and the read/write value round-trip run end-to-end.
 */
final class DbJobStateTest {

  @TempDir Path tempDir;

  private PartitionStorage storage;
  private StateStoreProcessingStore store;
  private DbJobState jobState;

  @BeforeEach
  void setUp() {
    storage = new PartitionStorage(tempDir.resolve("runtime"));
    store = new StateStoreProcessingStore(storage);
    jobState = new DbJobState(store);
    // Open one transaction for the test body so writes accumulate and reads see them, mirroring how
    // the stream processor owns the transaction across a command apply.
    store.getCurrentTransaction();
  }

  @AfterEach
  void tearDown() throws Exception {
    store.getCurrentTransaction().rollback();
    storage.close();
  }

  @Test
  void createMakesJobActivatableAndVisitable() {
    jobState.create(1L, job("payment", "worker-a", "tenant-a"));

    assertTrue(jobState.exists(1L));
    assertEquals(State.ACTIVATABLE, jobState.getState(1L));

    final List<Long> visited = new ArrayList<>();
    jobState.forEachActivatableJobs(
        typeBuffer("payment"),
        List.of("tenant-a"),
        (key, record) -> {
          visited.add(key);
          return true;
        });
    assertEquals(List.of(1L), visited);
  }

  @Test
  void activatableScanAllowsNestedReadsAndActivationWithoutChangingItsPrefix() {
    jobState.create(1, job("payment", "", "tenant-a").setPriority(10));
    jobState.create(2, job("payment", "", "tenant-a").setPriority(5));
    jobState.create(3, job("invoice", "", "tenant-b").setPriority(20));
    final List<Long> visited = new ArrayList<>();
    jobState.forEachActivatableJobs(
        typeBuffer("payment"),
        List.of("tenant-a"),
        (key, record) -> {
          final var activation = record.copy().setWorkerId("worker").setWorkerLeaseToken(42);
          assertEquals(State.ACTIVATABLE, jobState.getState(3));
          assertEquals("invoice", jobState.getJob(3).getType());
          jobState.activate(key, activation);
          visited.add(key);
          return true;
        });
    assertEquals(List.of(1L, 2L), visited);
    assertEquals(List.of(1L, 2L), jobState.findActivatedByWorker("tenant-a", "worker", 42, 10));
    final List<Long> remaining = new ArrayList<>();
    jobState.forEachActivatableJobs(
        typeBuffer("invoice"),
        List.of("tenant-b"),
        (key, record) -> {
          remaining.add(key);
          return true;
        });
    assertEquals(List.of(3L), remaining);
  }

  @Test
  void activateStoresRecordAndStateAndClearsActivatable() {
    jobState.activate(2L, job("invoice", "worker-b", "tenant-a"));

    assertEquals(State.ACTIVATED, jobState.getState(2L));
    final JobRecord stored = jobState.getJob(2L);
    assertEquals("invoice", stored.getType());
    assertEquals("worker-b", stored.getWorker());

    final List<Long> visited = new ArrayList<>();
    jobState.forEachActivatableJobs(
        typeBuffer("invoice"),
        List.of("tenant-a"),
        (key, record) -> {
          visited.add(key);
          return true;
        });
    assertTrue(visited.isEmpty(), "an activated job must not be activatable");
  }

  @Test
  void activateCreatedJobRemovesOnlyThatJobFromActivatableIndex() {
    jobState.create(6L, job("payment", "worker-create", "tenant-a"));
    jobState.create(7L, job("payment", "worker-create", "tenant-a"));

    jobState.activate(6L, job("payment", "worker-a", "tenant-a"));

    assertEquals(State.ACTIVATED, jobState.getState(6L));
    assertEquals(State.ACTIVATABLE, jobState.getState(7L));

    final List<Long> visited = new ArrayList<>();
    jobState.forEachActivatableJobs(
        typeBuffer("payment"),
        List.of("tenant-a"),
        (key, record) -> {
          visited.add(key);
          return true;
        });
    assertEquals(List.of(7L), visited);
  }

  @Test
  void forEachActivatableFiltersByTenant() {
    jobState.create(3L, job("shared", "worker", "tenant-a"));
    jobState.create(4L, job("shared", "worker", "tenant-b"));

    final List<Long> onlyTenantB = new ArrayList<>();
    jobState.forEachActivatableJobs(
        typeBuffer("shared"),
        List.of("tenant-b"),
        (key, record) -> {
          onlyTenantB.add(key);
          return true;
        });
    assertEquals(List.of(4L), onlyTenantB);
  }

  @Test
  void subscriptionIndexIncludesEveryLifecycleStateAndCopiesRecordsBeforeNestedReads() {
    jobState.create(1, job("shared", "", "tenant-a").setSubscriptionKey(42));
    jobState.activate(2, job("shared", "worker", "tenant-a").setSubscriptionKey(42));
    jobState.update(
        3, job("shared", "", "tenant-a").setSubscriptionKey(42).setRetries(0), State.FAILED);
    jobState.update(4, job("shared", "", "tenant-a").setSubscriptionKey(42), State.ERROR_THROWN);
    jobState.create(5, job("shared", "", "tenant-a").setSubscriptionKey(41));
    jobState.create(6, job("other", "", "tenant-a").setSubscriptionKey(43));
    jobState.create(7, job("shared", "", "tenant-a"));

    final List<Long> visited = new ArrayList<>();
    final List<JobRecord> records = new ArrayList<>();
    jobState.forEachJobs(
        42,
        (key, record) -> {
          assertEquals("other", jobState.getJob(6).getType());
          assertEquals(List.of(6L), subscriptionJobs(43));
          assertEquals("shared", record.getType());
          assertEquals("tenant-a", record.getTenantId());
          assertEquals(42, record.getSubscriptionKey());
          visited.add(key);
          records.add(record);
          return true;
        });
    assertEquals(List.of(1L, 2L, 3L, 4L), visited);
    records.getFirst().setJobType("changed");
    assertEquals("shared", jobState.getJob(1).getType());
    assertEquals("shared", records.get(1).getType());

    visited.clear();
    jobState.forEachJobs(
        42,
        (key, ignored) -> {
          visited.add(key);
          return visited.size() < 2;
        });
    assertEquals(List.of(1L, 2L), visited);
    assertEquals(List.of(5L), subscriptionJobs(41));
    assertEquals(List.of(6L), subscriptionJobs(43));
    assertTrue(subscriptionJobs(0).isEmpty());
    assertTrue(subscriptionJobs(44).isEmpty());
  }

  @Test
  void subscriptionIndexCanRemoveCurrentJobsAcrossCommittedAndUncommittedRecords()
      throws Exception {
    jobState.create(1, job("shared", "", "tenant-a").setSubscriptionKey(42));
    jobState.activate(2, job("shared", "", "tenant-a").setSubscriptionKey(42).setDeadline(100));
    jobState.create(3, job("shared", "", "tenant-a").setSubscriptionKey(43));
    store.getCurrentTransaction().commit();
    store.getCurrentTransaction();
    jobState.update(
        4,
        job("shared", "", "tenant-a").setSubscriptionKey(42).setRetries(2).setRecurringTime(200),
        State.FAILED);

    final List<Long> visited = new ArrayList<>();
    jobState.forEachJobs(
        42,
        (key, record) -> {
          visited.add(key);
          jobState.complete(key, record);
          return true;
        });
    assertEquals(List.of(1L, 2L, 4L), visited);
    assertFalse(jobState.exists(1));
    assertFalse(jobState.exists(2));
    assertFalse(jobState.exists(4));
    assertTrue(jobState.exists(3));
    assertTrue(jobState.findTimedOutJobs(Long.MAX_VALUE, 10).isEmpty());
    assertTrue(jobState.findBackedOffJobs(Long.MAX_VALUE, 10).isEmpty());
    assertTrue(subscriptionJobs(42).isEmpty());
    assertEquals(List.of(3L), subscriptionJobs(43));
    final var indexKey = new DbCompositeKey<>(new DbLong(), new DbLong());
    final var index =
        store.createColumnFamily(ColumnFamilies.JOBS_BY_SUBSCRIPTION, indexKey, DbNil.INSTANCE);
    indexKey.first().wrapLong(42);
    index.whileEqualPrefix(
        indexKey.first(),
        (key, ignored) -> {
          fail("deleted job left a subscription index entry");
          return false;
        });
  }

  @Test
  void subscriptionIndexTracksOwnershipUpdatesLifecycleAndCompletion() {
    jobState.create(1, job("task", "", "tenant-a").setSubscriptionKey(42));
    jobState.create(2, job("task", "", "tenant-a").setSubscriptionKey(42));
    jobState.update(1, jobState.getJob(1).setSubscriptionKey(43), State.ACTIVATABLE);
    assertEquals(List.of(2L), subscriptionJobs(42));
    assertEquals(List.of(1L), subscriptionJobs(43));
    jobState.activate(1, jobState.getJob(1));
    assertEquals(List.of(1L), subscriptionJobs(43));
    jobState.update(1, jobState.getJob(1).setRetries(0), State.FAILED);
    assertEquals(List.of(1L), subscriptionJobs(43));
    jobState.update(1, jobState.getJob(1).setSubscriptionKey(0), State.ACTIVATABLE);
    assertTrue(subscriptionJobs(43).isEmpty());
    jobState.update(1, jobState.getJob(1).setSubscriptionKey(42), State.ERROR_THROWN);
    assertEquals(List.of(1L, 2L), subscriptionJobs(42));
    jobState.complete(1, jobState.getJob(1));
    jobState.complete(2, jobState.getJob(2));
    assertTrue(subscriptionJobs(42).isEmpty());
  }

  @Test
  void subscriptionIndexRollsBackAndRecoversAfterReopen() throws Exception {
    jobState.create(1, job("task", "", "tenant-a").setSubscriptionKey(42));
    jobState.create(2, job("task", "", "tenant-a").setSubscriptionKey(43));
    store.getCurrentTransaction().commit();
    store.getCurrentTransaction();
    jobState.update(1, jobState.getJob(1).setSubscriptionKey(43), State.ACTIVATABLE);
    jobState.complete(2, jobState.getJob(2));
    assertTrue(subscriptionJobs(42).isEmpty());
    assertEquals(List.of(1L), subscriptionJobs(43));
    store.getCurrentTransaction().rollback();
    assertEquals(List.of(1L), subscriptionJobs(42));
    assertEquals(List.of(2L), subscriptionJobs(43));
    storage.close();
    storage = new PartitionStorage(tempDir.resolve("runtime"));
    store = new StateStoreProcessingStore(storage);
    jobState = new DbJobState(store);
    assertEquals(List.of(1L), subscriptionJobs(42));
    assertEquals(List.of(2L), subscriptionJobs(43));
    store.getCurrentTransaction();
    jobState.complete(1, jobState.getJob(1));
    jobState.update(2, jobState.getJob(2).setSubscriptionKey(42), State.ACTIVATABLE);
    store.getCurrentTransaction().commit();
    storage.close();
    storage = new PartitionStorage(tempDir.resolve("runtime"));
    store = new StateStoreProcessingStore(storage);
    jobState = new DbJobState(store);
    assertFalse(jobState.exists(1));
    assertEquals(List.of(2L), subscriptionJobs(42));
    assertTrue(subscriptionJobs(43).isEmpty());
  }

  private List<Long> subscriptionJobs(final long subscriptionKey) {
    final List<Long> result = new ArrayList<>();
    jobState.forEachJobs(
        subscriptionKey,
        (key, ignored) -> {
          result.add(key);
          return true;
        });
    return result;
  }

  @Test
  void activationOrdersAllSignedPrioritiesThenJobKeysAcrossTenants() {
    jobState.create(1, job("shared", "w", "a").setPriority(Integer.MIN_VALUE));
    jobState.create(2, job("shared", "w", "b").setPriority(-1));
    jobState.create(3, job("shared", "w", "a").setPriority(0));
    jobState.create(4, job("shared", "w", "b").setPriority(1));
    jobState.create(6, job("shared", "w", "a").setPriority(Integer.MAX_VALUE));
    jobState.create(5, job("shared", "w", "b").setPriority(Integer.MAX_VALUE));
    jobState.create(7, job("different", "w", "a").setPriority(Integer.MAX_VALUE));
    jobState.create(8, job("shared", "w", "excluded").setPriority(Integer.MAX_VALUE));

    assertEquals(List.of(5L, 6L, 4L, 3L, 2L, 1L), activatable("shared", List.of("a", "b")));
    assertEquals(List.of(6L, 3L, 1L), activatable("shared", List.of("a")));
    final List<Long> first = new ArrayList<>();
    jobState.forEachActivatableJobs(
        typeBuffer("shared"),
        List.of("a", "b"),
        (key, record) -> {
          first.add(key);
          return first.size() < 2;
        });
    assertEquals(List.of(5L, 6L), first);
  }

  @Test
  void removesOldPriorityTypeAndTenantKeysOnUpdateAndCompletion() {
    jobState.create(1, job("old", "w", "a").setPriority(10));
    jobState.create(2, job("old", "w", "a").setPriority(10));
    jobState.update(1, job("new", "w", "b").setPriority(-5), State.ACTIVATABLE);
    assertEquals(List.of(2L), activatable("old", List.of("a")));
    assertEquals(List.of(1L), activatable("new", List.of("b")));
    jobState.update(1, jobState.getJob(1).setPriority(20), State.ACTIVATABLE);
    assertEquals(List.of(1L), activatable("new", List.of("b")));
    jobState.activate(1, jobState.getJob(1));
    assertTrue(activatable("new", List.of("b")).isEmpty());
    jobState.update(1, jobState.getJob(1), State.ACTIVATABLE);
    assertEquals(20, jobState.getJob(1).getPriority());
    assertEquals(List.of(1L), activatable("new", List.of("b")));
    jobState.complete(1, jobState.getJob(1));
    jobState.complete(2, jobState.getJob(2));
    assertTrue(activatable("new", List.of("b")).isEmpty());
    assertTrue(activatable("old", List.of("a")).isEmpty());
  }

  @Test
  void priorityIndexRollsBackAndRecoversAfterReopen() throws Exception {
    jobState.create(1, job("task", "w", null).setPriority(-1));
    jobState.create(2, job("task", "w", "").setPriority(100));
    store.getCurrentTransaction().commit();
    store.getCurrentTransaction();
    jobState.update(1, jobState.getJob(1).copy().setPriority(200), State.ACTIVATABLE);
    jobState.activate(2, jobState.getJob(2));
    assertEquals(List.of(1L), activatable("task", List.of("<default>")));
    store.getCurrentTransaction().rollback();
    assertEquals(List.of(2L, 1L), activatable("task", List.of("<default>")));
    storage.close();
    storage = new PartitionStorage(tempDir.resolve("runtime"));
    store = new StateStoreProcessingStore(storage);
    jobState = new DbJobState(store);
    assertEquals(List.of(2L, 1L), activatable("task", List.of("<default>")));
    assertEquals(-1, jobState.getJob(1).getPriority());
    assertEquals(100, jobState.getJob(2).getPriority());
    store.getCurrentTransaction();
    jobState.complete(2, jobState.getJob(2));
    assertEquals(List.of(1L), activatable("task", List.of("<default>")));
  }

  private List<Long> activatable(final String type, final List<String> tenants) {
    final List<Long> result = new ArrayList<>();
    jobState.forEachActivatableJobs(
        typeBuffer(type),
        tenants,
        (key, record) -> {
          result.add(key);
          return true;
        });
    return result;
  }

  @Test
  void completeRemovesJobAndState() {
    jobState.activate(5L, job("payment", "worker-a", "tenant-a"));
    jobState.complete(5L, job("payment", "worker-a", "tenant-a"));

    assertFalse(jobState.exists(5L));
    assertNull(jobState.getJob(5L));
    assertEquals(State.NOT_FOUND, jobState.getState(5L));
  }

  @Test
  void shouldOrderAndBoundDueIndexesAcrossTenants() {
    jobState.activate(3, job("payment", "w", "tenant-b").setDeadline(300));
    jobState.activate(2, job("payment", "w", "tenant-a").setDeadline(100));
    jobState.activate(1, job("payment", "w", "tenant-b").setDeadline(100));
    jobState.update(
        4, job("payment", "w", "tenant-a").setRetries(2).setRecurringTime(150), State.FAILED);
    assertEquals(
        List.of(1L), jobState.findTimedOutJobs(100, 1).stream().map(j -> j.key()).toList());
    assertEquals(
        List.of(1L, 2L), jobState.findTimedOutJobs(299, 10).stream().map(j -> j.key()).toList());
    assertEquals("tenant-b", jobState.findTimedOutJobs(100, 1).getFirst().tenantId());
    assertTrue(jobState.findBackedOffJobs(149, 10).isEmpty());
    assertEquals(4L, jobState.findBackedOffJobs(150, 10).getFirst().key());
    assertEquals(100, jobState.getNextTimeoutDueDate());
    assertEquals(150, jobState.getNextBackoffDueDate());
  }

  @Test
  void shouldRemoveOldDatesOnRenewalFailureRetryAndCompletion() {
    final JobRecord record = job("payment", "w", "tenant-a").setRetries(2).setDeadline(100);
    jobState.activate(1, record);
    jobState.activate(1, jobState.getJob(1).copy().setDeadline(200));
    assertTrue(jobState.findTimedOutJobs(199, 10).isEmpty());
    assertEquals(200, jobState.getNextTimeoutDueDate());
    assertEquals(Long.MAX_VALUE, jobState.getNextBackoffDueDate());
    jobState.update(1, record.copy().setDeadline(0).setRecurringTime(300), State.FAILED);
    assertTrue(jobState.findTimedOutJobs(Long.MAX_VALUE, 10).isEmpty());
    assertEquals(Long.MAX_VALUE, jobState.getNextTimeoutDueDate());
    assertEquals(300, jobState.getNextBackoffDueDate());
    jobState.update(1, jobState.getJob(1).copy().setRetries(0), State.FAILED);
    assertEquals(Long.MAX_VALUE, jobState.getNextTimeoutDueDate());
    assertEquals(Long.MAX_VALUE, jobState.getNextBackoffDueDate());
    jobState.update(1, record.copy().setRecurringTime(400), State.FAILED);
    jobState.update(1, record.copy().setRecurringTime(0), State.ACTIVATABLE);
    assertTrue(jobState.findBackedOffJobs(Long.MAX_VALUE, 10).isEmpty());
    jobState.activate(1, record);
    jobState.complete(1, record);
    assertEquals(Long.MAX_VALUE, jobState.getNextTimeoutDueDate());
    assertEquals(Long.MAX_VALUE, jobState.getNextBackoffDueDate());
  }

  @Test
  void shouldRollbackIndexesAtomicallyAndRecoverAfterReopen() throws Exception {
    jobState.activate(
        1,
        job("payment", "w", "tenant-a").setWorkerId("w").setWorkerLeaseToken(42).setDeadline(100));
    store.getCurrentTransaction().commit();
    store.getCurrentTransaction();
    jobState.update(1, jobState.getJob(1).copy().setRetries(2).setRecurringTime(200), State.FAILED);
    assertEquals(0, jobState.getJob(1).getWorkerLeaseToken());
    assertTrue(jobState.findActivatedByWorker("tenant-a", "w", 42, 10).isEmpty());
    store.getCurrentTransaction().rollback();
    assertEquals(100, jobState.getNextTimeoutDueDate());
    assertEquals(Long.MAX_VALUE, jobState.getNextBackoffDueDate());
    assertEquals(42, jobState.getJob(1).getWorkerLeaseToken());
    assertEquals(List.of(1L), jobState.findActivatedByWorker("tenant-a", "w", 42, 10));
    storage.close();
    storage = new PartitionStorage(tempDir.resolve("runtime"));
    store = new StateStoreProcessingStore(storage);
    jobState = new DbJobState(store);
    assertEquals(1L, jobState.findTimedOutJobs(100, 10).getFirst().key());
    assertTrue(jobState.findBackedOffJobs(Long.MAX_VALUE, 10).isEmpty());
    assertEquals(42, jobState.getJob(1).getWorkerLeaseToken());
    store.getCurrentTransaction();
    jobState.update(1, jobState.getJob(1).copy().setDeadline(0), State.ACTIVATABLE);
    store.getCurrentTransaction().commit();
    storage.close();
    storage = new PartitionStorage(tempDir.resolve("runtime"));
    store = new StateStoreProcessingStore(storage);
    jobState = new DbJobState(store);
    assertEquals(State.ACTIVATABLE, jobState.getState(1));
    assertEquals("", jobState.getJob(1).getWorker());
    assertEquals("", jobState.getJob(1).getWorkerId());
    assertEquals(0, jobState.getJob(1).getWorkerLeaseToken());
    assertTrue(jobState.findActivatedByWorker("tenant-a", "w", 42, 10).isEmpty());
    assertEquals(Long.MAX_VALUE, jobState.getNextTimeoutDueDate());
    assertEquals(Long.MAX_VALUE, jobState.getNextBackoffDueDate());
  }

  private static JobRecord job(final String type, final String worker, final String tenantId) {
    return new JobRecord()
        .setJobType(type)
        .setWorker(worker)
        .setTenantId(tenantId)
        .setVariablesJson("{}");
  }

  @Test
  void workerOwnershipIndexTracksYieldFailureCompletionAndTenantIsolation() {
    final var original =
        job("task", "same-name", "a").setWorkerId("w").setWorkerLeaseToken(42).setDeadline(1000);
    jobState.activate(1, original);
    jobState.activate(2, original.copy().setTenantId("b"));
    jobState.activate(3, original.copy().setWorkerLeaseToken(43));
    assertEquals(List.of(1L), jobState.findActivatedByWorker("a", "w", 42, 1));
    assertEquals(List.of(2L), jobState.findActivatedByWorker("b", "w", 42, 10));
    jobState.update(1, original.copy().setDeadline(0), State.ACTIVATABLE);
    assertTrue(jobState.findActivatedByWorker("a", "w", 42, 10).isEmpty());
    assertEquals("", jobState.getJob(1).getWorker());
    assertEquals("", jobState.getJob(1).getWorkerId());
    assertEquals(0, jobState.getJob(1).getWorkerLeaseToken());
    jobState.activate(1, original.copy().setWorkerLeaseToken(44));
    assertEquals(List.of(1L), jobState.findActivatedByWorker("a", "w", 44, 10));
    jobState.update(1, jobState.getJob(1).copy().setRetries(0), State.FAILED);
    assertTrue(jobState.findActivatedByWorker("a", "w", 44, 10).isEmpty());
    jobState.complete(2, original.copy().setTenantId("b"));
    assertTrue(jobState.findActivatedByWorker("b", "w", 42, 10).isEmpty());
    assertEquals(List.of(3L), jobState.findActivatedByWorker("a", "w", 43, 10));
  }

  @Test
  void ownershipMigrationIsBoundedTransactionalAndResumesAfterReopen() throws Exception {
    for (long key = 1; key <= 105; key++) {
      jobState.activate(key, job("task", "w", "a").setWorkerId("w").setWorkerLeaseToken(42));
    }
    // Simulate an older snapshot: JOBS and JOB_STATES exist, ownership index is absent.
    var session =
        new DbCompositeKey<>(new DbString(), new DbCompositeKey<>(new DbString(), new DbLong()));
    session.first().wrapString("a");
    session.second().first().wrapString("w");
    session.second().second().wrapLong(42);
    var indexKey = new DbCompositeKey<>(session, new DbLong());
    var index =
        store.createColumnFamily(ColumnFamilies.JOB_ACTIVATED_BY_WORKER, indexKey, DbNil.INSTANCE);
    for (long key = 1; key <= 105; key++) {
      indexKey.second().wrapLong(key);
      index.deleteIfExists(indexKey);
    }
    store.getCurrentTransaction().commit();
    assertFalse(jobState.workerIndexReady());
    assertTrue(jobState.findActivatedByWorker("a", "w", 42, 200).isEmpty());
    store.getCurrentTransaction();
    jobState.migrateWorkerIndex(100);
    assertEquals(100, jobState.findActivatedByWorker("a", "w", 42, 200).size());
    assertFalse(jobState.workerIndexReady());
    store.getCurrentTransaction().rollback();
    assertTrue(jobState.findActivatedByWorker("a", "w", 42, 200).isEmpty());
    store.getCurrentTransaction();
    jobState.migrateWorkerIndex(100);
    store.getCurrentTransaction().commit();
    storage.close();
    storage = new PartitionStorage(tempDir.resolve("runtime"));
    store = new StateStoreProcessingStore(storage);
    jobState = new DbJobState(store);
    store.getCurrentTransaction();
    jobState.migrateWorkerIndex(100);
    assertTrue(jobState.workerIndexReady());
    assertEquals(105, jobState.findActivatedByWorker("a", "w", 42, 200).size());
    jobState.migrateWorkerIndex(100);
    assertEquals(105, jobState.findActivatedByWorker("a", "w", 42, 200).size());
  }

  private static UnsafeBuffer typeBuffer(final String type) {
    // matches DbString.wrapString: raw UTF-8 bytes with no length prefix
    return new UnsafeBuffer(type.getBytes(StandardCharsets.UTF_8));
  }
}
