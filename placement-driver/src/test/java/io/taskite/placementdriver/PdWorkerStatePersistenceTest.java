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
package io.taskite.placementdriver;

import static org.junit.jupiter.api.Assertions.*;

import io.taskite.db.impl.DbBytes;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbString;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.placementdriver.worker.registry.WorkerRegistryKey;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.impl.record.value.placementdriver.job.JobYieldPlan;
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.protocol.record.value.broker.JobYieldReason;
import io.taskite.protocol.record.value.broker.JobYieldStatus;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class PdWorkerStatePersistenceTest {
  @TempDir Path directory;

  @Test
  void persistsPlansAndPendingIndexWithoutLosingProgressAcrossRestart() throws Exception {
    try (var storage = new PartitionStorage(directory.resolve("runtime"))) {
      var store = new StateStoreProcessingStore(storage);
      writeState(store);
      store.markAsProcessed(77);
      var state = new MutableProcessingState(store);
      var plans = state.getJobYieldState();
      assertEquals(
          List.of(10L, 30L),
          plans.pendingPlans(0, 10).stream().map(JobYieldPlan::yieldId).toList());
      assertEquals(
          List.of(10L), plans.pendingPlans(0, 1).stream().map(JobYieldPlan::yieldId).toList());
      assertEquals(
          List.of(30L), plans.pendingPlans(10, 1).stream().map(JobYieldPlan::yieldId).toList());
      assertEquals(
          List.of(20L, 30L),
          plans.plansAfter("demo", 10, 2).stream().map(JobYieldPlan::yieldId).toList());
      assertTrue(plans.plansAfter("missing", 0, 10).isEmpty());
      assertTrue(plans.plan("other", 10).isEmpty());
      var target = plans.plan("demo", 10).orElseThrow().partitions().getFirst();
      assertEquals(7, target.releasedJobCount());
      assertEquals(3, target.attempts());
      assertEquals(3000, target.nextAttemptAtEpochMillis());
      assertEquals("retry", target.lastError());
      assertEquals(9007199254741036L, plans.plan("demo", 10).orElseThrow().workerLeaseToken());
      assertEquals(2000, plans.plan("demo", 20).orElseThrow().completedAtEpochMillis());
      assertEquals(77, store.getLastProcessedPosition());
      store.getCurrentTransaction().commit();
    }
    try (var storage = new PartitionStorage(directory.resolve("runtime"))) {
      var store = new StateStoreProcessingStore(storage);
      var plans = new MutableProcessingState(store).getJobYieldState();
      assertEquals(4, plans.plans().size());
      assertEquals(
          "demo",
          store
              .createColumnFamily(
                  ColumnFamilies.PD_PENDING_JOB_YIELDS, new DbLong(), new DbString())
              .get(id(10))
              .toString());
      assertEquals("demo-suffix", plans.plansAfter("demo-suffix", 0, 1).getFirst().clusterId());
      assertEquals(77, store.getLastProcessedPosition());
    }
  }

  @Test
  void rollbackRestoresPlanProgressAndPendingIndexTogether() throws Exception {
    try (var storage = new PartitionStorage(directory.resolve("runtime"))) {
      var store = new StateStoreProcessingStore(storage);
      writeState(store);
      var plans = new MutableProcessingState(store).getJobYieldState();
      store.getCurrentTransaction();
      plans.updateProgress(completed(10), true);
      assertEquals(
          List.of(30L), plans.pendingPlans(0, 10).stream().map(JobYieldPlan::yieldId).toList());
      assertEquals(2000, plans.plan("demo", 10).orElseThrow().completedAtEpochMillis());
      plans.deleteForWorker("demo", "tenant", "worker");
      assertTrue(plans.plansAfter("demo", 0, 10).isEmpty());
      assertTrue(plans.pendingPlans(0, 10).isEmpty());
      assertEquals(1, plans.plansAfter("demo-suffix", 0, 10).size());
      store.getCurrentTransaction().rollback();
      assertEquals(4, plans.plans().size());
      assertEquals(
          List.of(10L, 30L),
          plans.pendingPlans(0, 10).stream().map(JobYieldPlan::yieldId).toList());
      assertEquals(0, plans.plan("demo", 10).orElseThrow().completedAtEpochMillis());
      assertEquals(
          7, plans.plan("demo", 10).orElseThrow().partitions().getFirst().releasedJobCount());
    }
  }

  @Test
  void importingCheckpointRestoresPlansAndPendingIndexInExistingState() throws Exception {
    try (var storage = new PartitionStorage(directory.resolve("runtime"))) {
      var store = new StateStoreProcessingStore(storage);
      writeState(store);
      storage.createCheckpoint(directory.resolve("checkpoint"));
      var plans = new MutableProcessingState(store).getJobYieldState();
      plans.deleteForWorker("demo", "tenant", "worker");
      store.getCurrentTransaction().commit();
      assertEquals(1, plans.plans().size());
      assertTrue(plans.pendingPlans(0, 10).isEmpty());
      storage.importFromCheckpoint(directory.resolve("checkpoint"));
      assertEquals(4, plans.plans().size());
      assertEquals(
          List.of(10L, 30L),
          plans.pendingPlans(0, 10).stream().map(JobYieldPlan::yieldId).toList());
    }
  }

  @Test
  void workerNameUpdateKeepsCanonicalIdentityAndDeadlineConsistentAcrossRollbackAndRestart()
      throws Exception {
    var identity = new WorkerRegistryKey("demo", "tenant", "worker");
    var policy =
        new RegistrationPolicy(
            Duration.ofSeconds(30), Duration.ofSeconds(4), Duration.ofMinutes(1));
    try (var storage = new PartitionStorage(directory.resolve("runtime"))) {
      var store = new StateStoreProcessingStore(storage);
      var state = new MutableProcessingState(store, () -> 0, () -> 0, 50, policy);
      var workers = state.getWorkerRegistrationState();
      var renewal =
          new WorkerRegistrationRecord()
              .setClusterId("demo")
              .setTenantIds(List.of("tenant"))
              .setWorkerId("worker")
              .setName("payments")
              .setWorkerLeaseToken(42)
              .setLeaseDeadlineEpochMillis(5000);
      workers.registerWorker(renewal, "tenant", Instant.ofEpochMilli(1000));
      var registrations =
          store.createColumnFamily(
              ColumnFamilies.PD_WORKER_REGISTRATIONS, new DbString(), new DbBytes());
      assertTrue(registrations.exists(text(identity.dbKey())));

      store.getCurrentTransaction();
      renewal.setName("payments-v2").setLeaseDeadlineEpochMillis(6000);
      workers.renewWorker(renewal, "tenant", Instant.ofEpochMilli(2000));
      assertEquals(1, workers.registrations().size());
      assertEquals("payments-v2", workers.registration(identity).orElseThrow().name());
      var deadlines = state.getRegistrationDeadlineState().findDue(Long.MAX_VALUE, 10);
      assertEquals(1, deadlines.size());
      assertEquals(identity, deadlines.getFirst().workerKey());
      assertEquals(6000, deadlines.getFirst().dueDate());
      store.getCurrentTransaction().rollback();
      assertEquals("payments", workers.registration(identity).orElseThrow().name());
      deadlines = state.getRegistrationDeadlineState().findDue(Long.MAX_VALUE, 10);
      assertEquals(1, deadlines.size());
      assertEquals(5000, deadlines.getFirst().dueDate());

      store.getCurrentTransaction();
      workers.renewWorker(renewal, "tenant", Instant.ofEpochMilli(2000));
      store.getCurrentTransaction().commit();
    }
    try (var storage = new PartitionStorage(directory.resolve("runtime"))) {
      var restored =
          new MutableProcessingState(
              new StateStoreProcessingStore(storage), () -> 0, () -> 0, 50, policy);
      assertEquals(1, restored.getWorkerRegistrationState().registrations().size());
      var worker = restored.getWorkerRegistrationState().registration(identity).orElseThrow();
      assertEquals("payments-v2", worker.name());
      assertEquals("worker", worker.workerId());
      assertEquals(42, worker.workerLeaseToken());
      assertEquals(6000, worker.leaseDeadlineEpochMillis());
      var deadlines = restored.getRegistrationDeadlineState().findDue(Long.MAX_VALUE, 10);
      assertEquals(1, deadlines.size());
      assertEquals(identity, deadlines.getFirst().workerKey());
      assertEquals(6000, deadlines.getFirst().dueDate());
    }
  }

  private static void writeState(StateStoreProcessingStore store) throws Exception {
    var plans = new MutableProcessingState(store).getJobYieldState();
    var target =
        new JobYieldPlan.Partition(1, JobYieldStatus.PENDING, 7, 3, 2000, 3000, 0, "retry", "");
    for (var plan :
        List.of(
            plan(10, "demo", List.of(target), 0),
            plan(20, "demo", List.of(target), 0),
            plan(30, "demo", List.of(target), 0),
            plan(40, "demo-suffix", List.of(), 2000))) {
      plans.create(plan);
    }
    plans.updateProgress(completed(20), true);
    store.getCurrentTransaction().commit();
  }

  private static JobYieldRecord completed(long id) {
    return new JobYieldRecord()
        .setClusterId("demo")
        .setYieldId(id)
        .setPartitionId(1)
        .setReleasedJobCount(7)
        .setTimestampEpochMillis(2000);
  }

  private static JobYieldPlan plan(
      long id, String clusterId, List<JobYieldPlan.Partition> targets, long completedAt) {
    return new JobYieldPlan(
        id,
        clusterId,
        "tenant",
        "worker",
        9007199254741036L,
        JobYieldReason.WORKER_LEASE_EXPIRED,
        1000,
        completedAt,
        new WorkerRegistrationRecord().setWorkerId("worker").setName("payments"),
        targets);
  }

  private static DbLong id(long id) {
    var key = new DbLong();
    key.wrapLong(id);
    return key;
  }

  private static DbString text(String value) {
    var text = new DbString();
    text.wrapString(value);
    return text;
  }
}
