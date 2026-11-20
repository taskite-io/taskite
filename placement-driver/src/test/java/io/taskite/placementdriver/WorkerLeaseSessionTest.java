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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.taskite.db.impl.DbBytes;
import io.taskite.db.impl.DbCompositeKey;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbString;
import io.taskite.placementdriver.common.codec.WorkerRegistrationCodec;
import io.taskite.placementdriver.processing.job.JobYieldAcknowledgeProcessor;
import io.taskite.placementdriver.processing.job.JobYieldUpdateProgressProcessor;
import io.taskite.placementdriver.processing.worker.registry.WorkerDeleteProcessor;
import io.taskite.placementdriver.processing.worker.registry.WorkerDeregisterProcessor;
import io.taskite.placementdriver.processing.worker.registry.WorkerMarkOfflineProcessor;
import io.taskite.placementdriver.processing.worker.registry.WorkerRegisterProcessor;
import io.taskite.placementdriver.processing.worker.registry.WorkerRenewProcessor;
import io.taskite.placementdriver.state.appliers.WorkerDeletedApplier;
import io.taskite.placementdriver.state.appliers.WorkerMarkedOfflineApplier;
import io.taskite.placementdriver.state.appliers.WorkerRegisteredApplier;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.placementdriver.worker.registry.WorkerRegistryKey;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.encoding.MsgPackConverter;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.impl.record.value.placementdriver.job.JobYieldPlan;
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.WorkerIntent;
import io.taskite.protocol.record.value.broker.JobYieldReason;
import io.taskite.protocol.record.value.broker.JobYieldStatus;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContextImpl;
import io.taskite.streamplatform.api.state.EventAppliers;
import io.taskite.streamplatform.api.writer.*;
import io.taskite.streamplatform.state.PersistedKeyGenerator;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

final class WorkerLeaseSessionTest {
  @TempDir Path directory;

  @Test
  void offlineChecksExpectedTokenAndPersistsCapturedPlansForReplay() {
    try (final var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 2, 1));
      new WorkerRegisteredApplier(state)
          .applyState(42, worker().setWorkerLeaseToken(42).setLeaseDeadlineEpochMillis(2000));
      final var writers = writers();
      final var keys = new AtomicLong(42);
      final var context =
          new TypedRecordProcessorContextImpl(
              1, keys::incrementAndGet, writers, state, new EventAppliers());
      final var processor = new WorkerMarkOfflineProcessor(context, Duration.ofSeconds(1));
      processor.processRecord(
          command(
              worker().setExpectedLastRenewalEpochMillis(1000).setExpectedWorkerLeaseToken(41),
              2000));
      verify(writers.rejection()).appendRejection(eq(RejectionType.INVALID_STATE), anyString());
      verifyNoInteractions(writers.state());
      processor.processRecord(
          command(
              worker().setExpectedLastRenewalEpochMillis(1000).setExpectedWorkerLeaseToken(42),
              2000));
      final var capture = ArgumentCaptor.forClass(WorkerRegistrationRecord.class);
      verify(writers.state())
          .appendFollowUpEvent(eq(44L), eq(WorkerIntent.MARKED_OFFLINE), capture.capture());
      final var event = new WorkerRegistrationRecord();
      final var serialized = new UnsafeBuffer(new byte[capture.getValue().getLength()]);
      capture.getValue().write(serialized, 0);
      event.wrap(serialized);
      assertEquals(
          List.of(43L, 44L), event.getYieldPlans().stream().map(JobYieldPlan::yieldId).toList());
      assertEquals(
          List.of(1, 2),
          event.getYieldPlans().getFirst().partitions().stream()
              .map(JobYieldPlan.Partition::partitionId)
              .toList());
      // Replay must use the captured topology even when the live configuration has changed.
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 3, 1));
      new WorkerMarkedOfflineApplier(state).applyState(44, event);
      assertEquals(2, state.getJobYieldState().pendingPlans(0, 10).size());
      assertEquals(2, state.getJobYieldState().plan("demo", 43).orElseThrow().partitions().size());
      new WorkerMarkedOfflineApplier(state).applyState(44, event);
      assertEquals(2, state.getJobYieldState().plans().size());
      assertEquals(
          42,
          state
              .getWorkerRegistrationState()
              .registration(new WorkerRegistryKey("demo", "a", "worker"))
              .orElseThrow()
              .workerLeaseToken());
    }
  }

  @Test
  void durableProgressRetainsMaximumCountAndCompletedTargetsRejectLateProgress() throws Exception {
    try (final var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      final var plans = state.getJobYieldState();
      plans.create(plan(10, "demo", List.of(JobYieldPlan.Partition.pending(1))));
      plans.create(plan(20, "other", List.of()));
      plans.create(plan(30, "demo", List.of()));
      assertEquals(1000, plans.plan("other", 20).orElseThrow().completedAtEpochMillis());
      assertEquals(
          List.of(10L), plans.pendingPlans(0, 1).stream().map(JobYieldPlan::yieldId).toList());
      assertEquals(
          List.of(30L),
          plans.plansAfter("demo", 10, 1).stream().map(JobYieldPlan::yieldId).toList());
      final var progress =
          new JobYieldRecord()
              .setYieldId(10)
              .setClusterId("demo")
              .setTenantId("a")
              .setWorkerId("worker")
              .setWorkerLeaseToken(42)
              .setPartitionId(1)
              .setReason(JobYieldReason.WORKER_LEASE_EXPIRED)
              .setAttempts(2)
              .setTimestampEpochMillis(2000)
              .setReleasedJobCount(5);
      plans.updateProgress(progress, false);
      plans.updateProgress(
          progress
              .copy()
              .setReleasedJobCount(3)
              .setTimestampEpochMillis(3000)
              .setStatus(JobYieldStatus.COMPLETED),
          true);
      assertEquals(
          5, plans.plan("demo", 10).orElseThrow().partitions().getFirst().releasedJobCount());
      assertTrue(plans.pendingPlans(0, 10).isEmpty());
      final var writers = writers();
      final var context =
          new TypedRecordProcessorContextImpl(1, () -> 99, writers, state, new EventAppliers());
      new JobYieldUpdateProgressProcessor(context).processRecord(yieldCommand(progress));
      new JobYieldAcknowledgeProcessor(context)
          .processRecord(yieldCommand(progress.copy().setStatus(JobYieldStatus.COMPLETED)));
      verifyNoInteractions(writers.state());
      new JobYieldAcknowledgeProcessor(context)
          .processRecord(
              yieldCommand(
                  progress.copy().setWorkerLeaseToken(41).setStatus(JobYieldStatus.COMPLETED)));
      verify(writers.rejection()).appendRejection(eq(RejectionType.INVALID_ARGUMENT), anyString());
      store.getCurrentTransaction().commit();
      final var recovered = new MutableProcessingState(new StateStoreProcessingStore(storage));
      assertEquals(
          3000,
          recovered.getJobYieldState().plan("demo", 10).orElseThrow().completedAtEpochMillis());
      assertEquals(
          List.of(10L, 30L),
          recovered.getJobYieldState().plansAfter("demo", 0, 10).stream()
              .map(JobYieldPlan::yieldId)
              .toList());
    }
  }

  @Test
  void tokenHighWaterSurvivesRecoveryAndGuardsPartitionKeyExhaustion() throws Exception {
    try (final var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var generator =
          new PersistedKeyGenerator(Protocol.PLACEMENT_DRIVER_CLUSTER_KEY, 1, store);
      final long first = generator.nextKey();
      store.getCurrentTransaction().commit();
      final var recoveredStore = new StateStoreProcessingStore(storage);
      final var recovered =
          new PersistedKeyGenerator(Protocol.PLACEMENT_DRIVER_CLUSTER_KEY, 1, recoveredStore);
      assertEquals(first + 1, recovered.nextKey());
      recovered.setKeyIfHigher(first);
      assertEquals(first + 2, recovered.nextKey());
      recovered.setKeyIfHigher(
          Protocol.encodeKey(Protocol.PLACEMENT_DRIVER_CLUSTER_KEY, 1, Protocol.MAXIMUM_SEQUENCE));
      assertThrows(IllegalStateException.class, recovered::nextKey);
    }
  }

  private static JobYieldPlan plan(
      final long id, final String cluster, final List<JobYieldPlan.Partition> partitions) {
    return plan(id, cluster, "a", "worker", 42, partitions);
  }

  private static JobYieldPlan plan(
      final long id,
      final String cluster,
      final String tenant,
      final String workerId,
      final long token,
      final List<JobYieldPlan.Partition> partitions) {
    return new JobYieldPlan(
        id,
        cluster,
        tenant,
        workerId,
        token,
        JobYieldReason.WORKER_LEASE_EXPIRED,
        1000,
        0,
        worker(),
        partitions);
  }

  @Test
  void expiredWorkerDeletionRemovesAllSessionHistoryAndIndexesAcrossRestart() throws Exception {
    var deletion =
        worker()
            .setTenantIds(List.of("a", "a"))
            .setExpectedWorkerLeaseToken(43)
            .setExpectedStatusChangedAtEpochMillis(2000);
    try (final var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      final var plans = state.getJobYieldState();
      final var pending = List.of(JobYieldPlan.Partition.pending(1));
      new WorkerRegisteredApplier(state).applyState(43, worker().setWorkerLeaseToken(43));
      final var identity = new WorkerRegistryKey("demo", "a", "worker");
      state.getWorkerRegistrationState().markWorkerOffline(identity, Instant.ofEpochMilli(2000));
      plans.create(plan(10, "demo", "a", "worker", 42, List.of()));
      plans.create(plan(20, "demo", "a", "worker", 43, pending));
      plans.create(plan(30, "demo", "b", "worker", 42, pending));
      plans.create(plan(40, "other", "a", "worker", 42, pending));
      plans.create(plan(50, "demo", "a", "other-worker", 42, pending));
      plans.create(plan(60, "demo-suffix", "a", "worker", 42, pending));
      store.getCurrentTransaction().commit();
      store.getCurrentTransaction();
      final var writers = writers();
      final var context =
          new TypedRecordProcessorContextImpl(1, () -> 99, writers, state, new EventAppliers());
      final var processor = new WorkerDeleteProcessor(context, Duration.ofSeconds(1));
      processor.processRecord(command(deletion, 2999));
      verify(writers.rejection()).appendRejection(eq(RejectionType.INVALID_STATE), anyString());
      verifyNoInteractions(writers.state());
      assertEquals(6, plans.plans().size());
      processor.processRecord(command(deletion, 3000));
      final var capture = ArgumentCaptor.forClass(WorkerRegistrationRecord.class);
      verify(writers.state())
          .appendFollowUpEvent(eq(0L), eq(WorkerIntent.DELETED), capture.capture());
      // Replay uses the event identity, including all former tokens and display names.
      deletion = capture.getValue();
      new WorkerDeletedApplier(state).applyState(0, deletion);
      assertTrue(state.getWorkerRegistrationState().registration(identity).isEmpty());
      assertTrue(
          state
              .getWorkerRegistrationState()
              .registration(new WorkerRegistryKey("demo", "b", "worker"))
              .isPresent());
      assertTrue(plans.plan("demo", 10).isEmpty());
      assertTrue(plans.plan("demo", 20).isEmpty());
      assertEquals(
          List.of(30L, 40L, 50L, 60L),
          plans.pendingPlans(0, 10).stream().map(JobYieldPlan::yieldId).toList());
      assertEquals(
          List.of(30L, 50L),
          plans.plansAfter("demo", 0, 10).stream().map(JobYieldPlan::yieldId).toList());
      assertEquals(
          4,
          store
              .createColumnFamily(
                  ColumnFamilies.PD_PENDING_JOB_YIELDS, new DbLong(), new DbString())
              .count());
      assertEquals(
          4,
          store
              .createColumnFamily(
                  ColumnFamilies.PD_JOB_YIELDS,
                  new DbCompositeKey<>(new DbString(), new DbLong()),
                  new DbBytes())
              .count());
      clearInvocations(writers.state(), writers.rejection());
      final var late =
          new JobYieldRecord()
              .setYieldId(20)
              .setClusterId("demo")
              .setTenantId("a")
              .setWorkerId("worker")
              .setWorkerLeaseToken(43)
              .setPartitionId(1)
              .setAttempts(1)
              .setTimestampEpochMillis(4000)
              .setReason(JobYieldReason.WORKER_LEASE_EXPIRED)
              .setStatus(JobYieldStatus.COMPLETED);
      new JobYieldAcknowledgeProcessor(context).processRecord(yieldCommand(late));
      new JobYieldUpdateProgressProcessor(context).processRecord(yieldCommand(late));
      verifyNoInteractions(writers.state());
      verify(writers.rejection(), times(2))
          .appendRejection(eq(RejectionType.INVALID_ARGUMENT), anyString());
      assertTrue(plans.plan("demo", 20).isEmpty());
      store.getCurrentTransaction().commit();
    }
    try (final var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      new WorkerDeletedApplier(state).applyState(0, deletion);
      assertEquals(
          List.of(30L, 40L, 50L, 60L),
          state.getJobYieldState().plans().stream().map(JobYieldPlan::yieldId).sorted().toList());
      assertTrue(
          state
              .getWorkerRegistrationState()
              .registration(new WorkerRegistryKey("demo", "a", "worker"))
              .isEmpty());
    }
  }

  @Test
  void workerAndHistoryDeletionRollbackTogether() throws Exception {
    try (final var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      new WorkerRegisteredApplier(state).applyState(42, worker().setWorkerLeaseToken(42));
      state.getJobYieldState().create(plan(10, "demo", List.of()));
      state.getJobYieldState().create(plan(20, "demo", List.of(JobYieldPlan.Partition.pending(1))));
      store.getCurrentTransaction().commit();
      store.getCurrentTransaction();
      new WorkerDeletedApplier(state).applyState(0, worker());
      assertTrue(state.getWorkerRegistrationState().registrations().isEmpty());
      assertTrue(state.getJobYieldState().plans().isEmpty());
      store.getCurrentTransaction().rollback();
      assertEquals(2, state.getWorkerRegistrationState().registrations().size());
      assertEquals(2, state.getJobYieldState().plans().size());
      assertEquals(1, state.getJobYieldState().pendingPlans(0, 10).size());
      assertEquals(2, state.getJobYieldState().plansAfter("demo", 0, 10).size());
    }
  }

  @Test
  void staleRetentionDeletePreservesNewSessionAndOldHistory() {
    try (final var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      new WorkerRegisteredApplier(state).applyState(43, worker().setWorkerLeaseToken(43));
      state.getJobYieldState().create(plan(10, "demo", List.of()));
      state
          .getJobYieldState()
          .create(plan(20, "demo", "a", "worker", 43, List.of(JobYieldPlan.Partition.pending(1))));
      final var writers = writers();
      final var context =
          new TypedRecordProcessorContextImpl(1, () -> 99, writers, state, new EventAppliers());
      final var processor = new WorkerDeleteProcessor(context, Duration.ofSeconds(1));
      final var stale =
          worker()
              .setTenantIds(List.of("a"))
              .setExpectedWorkerLeaseToken(42)
              .setExpectedStatusChangedAtEpochMillis(2000);
      processor.processRecord(command(stale, 5000));
      state
          .getWorkerRegistrationState()
          .markWorkerOffline(
              new WorkerRegistryKey("demo", "a", "worker"), Instant.ofEpochMilli(2000));
      processor.processRecord(command(stale, 5000));
      processor.processRecord(
          command(
              worker()
                  .setTenantIds(List.of("a"))
                  .setExpectedWorkerLeaseToken(43)
                  .setExpectedStatusChangedAtEpochMillis(1000),
              5000));
      verify(writers.rejection(), times(3))
          .appendRejection(eq(RejectionType.INVALID_STATE), anyString());
      verifyNoInteractions(writers.state());
      assertEquals(2, state.getJobYieldState().plans().size());
      assertEquals(
          43,
          state
              .getWorkerRegistrationState()
              .registration(new WorkerRegistryKey("demo", "a", "worker"))
              .orElseThrow()
              .workerLeaseToken());
    }
  }

  private static TypedRecord<JobYieldRecord> yieldCommand(final JobYieldRecord value) {
    return new TypedRecord<>() {
      @Override
      public long getKey() {
        return value.getYieldId();
      }

      @Override
      public JobYieldRecord getValue() {
        return value;
      }

      @Override
      public String getRequestStreamId() {
        return "stream";
      }

      @Override
      public String getRequestId() {
        return "progress";
      }

      @Override
      public int getLength() {
        return value.getLength();
      }

      @Override
      public long getTimestamp() {
        return 4000;
      }
    };
  }

  @Test
  void registerRetryRejectsPartiallyEndedTenantSession() {
    try (final var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 1, 1));
      new WorkerRegisteredApplier(state)
          .applyState(42, worker().setWorkerLeaseToken(42).setLeaseDeadlineEpochMillis(5000));
      state
          .getWorkerRegistrationState()
          .markWorkerOffline(
              new WorkerRegistryKey("demo", "b", "worker"), Instant.ofEpochMilli(2000));
      final var writers = writers();
      final var keys = new AtomicLong(42);
      final var context =
          new TypedRecordProcessorContextImpl(
              1, keys::incrementAndGet, writers, state, new EventAppliers());
      new WorkerRegisterProcessor(context).processRecord(command(worker(), 3000));
      verify(writers.rejection())
          .appendRejection(eq(RejectionType.STALE_WORKER_SESSION), anyString());
      verifyNoInteractions(writers.state());
      assertEquals(42, keys.get());
    }
  }

  @Test
  void registrationRetryKeepsTokenWithoutExtendingLease() {
    try (final var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 1, 1));
      new WorkerRegisteredApplier(state)
          .applyState(42, worker().setWorkerLeaseToken(42).setLeaseDeadlineEpochMillis(5000));
      final var writers = writers();
      final var keys = new AtomicLong(42);
      final var context =
          new TypedRecordProcessorContextImpl(
              1, keys::incrementAndGet, writers, state, new EventAppliers());
      final var processor = new WorkerRegisterProcessor(context);
      processor.processRecord(command(worker().setTenantIds(List.of("b", "a")), 3000));
      processor.processRecord(command(worker(), 4000));
      final var capture = ArgumentCaptor.forClass(WorkerRegistrationRecord.class);
      verify(writers.response(), times(2))
          .writeEventOnCommand(eq(42L), eq(WorkerIntent.REGISTERED), capture.capture(), any());
      assertEquals(42, capture.getValue().getWorkerLeaseToken());
      assertEquals(5000, capture.getValue().getLeaseDeadlineEpochMillis());
      verifyNoInteractions(writers.state(), writers.rejection());
      assertEquals(42, keys.get());
    }
  }

  @Test
  void registrationRejectsChangedTenantsWithoutAllocatingKeys() {
    try (final var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 1, 1));
      new WorkerRegisteredApplier(state)
          .applyState(42, worker().setWorkerLeaseToken(42).setLeaseDeadlineEpochMillis(5000));
      final var writers = writers();
      final var keys = new AtomicLong(42);
      final var context =
          new TypedRecordProcessorContextImpl(
              1, keys::incrementAndGet, writers, state, new EventAppliers());
      final var processor = new WorkerRegisterProcessor(context);
      for (final var request :
          List.of(
              worker().setTenantIds(List.of("a")),
              worker().setTenantIds(List.of("c")),
              worker().setTenantIds(List.of("a", "b", "c")))) {
        processor.processRecord(command(request, 3000));
      }
      verify(writers.rejection(), times(3))
          .appendRejection(eq(RejectionType.STALE_WORKER_SESSION), anyString());
      verifyNoInteractions(writers.state());
      verify(writers.response(), never())
          .writeEventOnCommand(anyLong(), eq(WorkerIntent.REGISTERED), any(), any());
      assertEquals(42, keys.get());
      assertTrue(state.getJobYieldState().plans().isEmpty());
      assertTrue(
          state.getWorkerRegistrationState().registrations().stream()
              .allMatch(current -> current.workerLeaseToken() == 42));
    }
  }

  @Test
  void registrationRetryAfterRestartUsesCurrentLeaseAndRejectsExpiration() throws Exception {
    try (final var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 1, 1));
      new WorkerRegisteredApplier(state)
          .applyState(42, worker().setWorkerLeaseToken(42).setLeaseDeadlineEpochMillis(5000));
      state
          .getWorkerRegistrationState()
          .renewWorker(
              worker().setWorkerLeaseToken(42).setLeaseDeadlineEpochMillis(6000),
              "a",
              Instant.ofEpochMilli(2000));
      store.getCurrentTransaction().commit();
    }
    try (final var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      final var writers = writers();
      final var keys = new AtomicLong(42);
      final var context =
          new TypedRecordProcessorContextImpl(
              1, keys::incrementAndGet, writers, state, new EventAppliers());
      final var processor = new WorkerRegisterProcessor(context);
      processor.processRecord(command(worker(), 3000));
      final var capture = ArgumentCaptor.forClass(WorkerRegistrationRecord.class);
      verify(writers.response())
          .writeEventOnCommand(eq(42L), eq(WorkerIntent.REGISTERED), capture.capture(), any());
      assertEquals(5000, capture.getValue().getLeaseDeadlineEpochMillis());
      processor.processRecord(command(worker(), 5000));
      verify(writers.rejection())
          .appendRejection(eq(RejectionType.STALE_WORKER_SESSION), anyString());
      verifyNoInteractions(writers.state());
      assertEquals(42, keys.get());
    }
  }

  @Test
  void renewAndDeregisterRejectOldTokenAndExpiredLeaseAtomically() {
    try (final var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 1, 1));
      new WorkerRegisteredApplier(state)
          .applyState(42, worker().setWorkerLeaseToken(42).setLeaseDeadlineEpochMillis(5000));
      final var writers = writers();
      final var context =
          new TypedRecordProcessorContextImpl(1, () -> 99, writers, state, new EventAppliers());
      final var renew = new WorkerRenewProcessor(context, Duration.ofSeconds(5));
      renew.processRecord(command(worker().setWorkerLeaseToken(41), 3000));
      verify(writers.rejection())
          .appendRejection(eq(RejectionType.STALE_WORKER_SESSION), anyString());
      verifyNoInteractions(writers.state());
      clearInvocations(writers.rejection(), writers.response());
      renew.processRecord(command(worker().setWorkerLeaseToken(42), 5000));
      verify(writers.rejection())
          .appendRejection(eq(RejectionType.STALE_WORKER_SESSION), anyString());
      clearInvocations(writers.rejection(), writers.response());
      new WorkerDeregisterProcessor(context)
          .processRecord(command(worker().setWorkerLeaseToken(41), 3000));
      verify(writers.rejection())
          .appendRejection(eq(RejectionType.STALE_WORKER_SESSION), anyString());
      verifyNoInteractions(writers.state());
      clearInvocations(writers.rejection(), writers.response());
      renew.processRecord(command(worker().setWorkerLeaseToken(42), 3000));
      final var capture = ArgumentCaptor.forClass(WorkerRegistrationRecord.class);
      verify(writers.state())
          .appendFollowUpEvent(eq(0L), eq(WorkerIntent.RENEWED), capture.capture());
      assertEquals(42, capture.getValue().getWorkerLeaseToken());
      assertEquals(8000, capture.getValue().getLeaseDeadlineEpochMillis());
    }
  }

  @Test
  void deregistrationRejectsPartialTenantSetWithoutEndingTheSession() {
    try (final var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 1, 1));
      new WorkerRegisteredApplier(state)
          .applyState(42, worker().setWorkerLeaseToken(42).setLeaseDeadlineEpochMillis(5000));
      final var writers = writers();
      final var keys = new AtomicLong(42);
      final var context =
          new TypedRecordProcessorContextImpl(
              1, keys::incrementAndGet, writers, state, new EventAppliers());
      new WorkerDeregisterProcessor(context)
          .processRecord(
              command(worker().setWorkerLeaseToken(42).setTenantIds(List.of("a")), 3000));
      verify(writers.rejection())
          .appendRejection(eq(RejectionType.STALE_WORKER_SESSION), anyString());
      verifyNoInteractions(writers.state());
      assertEquals(42, keys.get());
      assertEquals(2, state.getWorkerRegistrationState().registrations().size());
      new WorkerRenewProcessor(context, Duration.ofSeconds(5))
          .processRecord(command(worker().setWorkerLeaseToken(42), 3000));
      verify(writers.state()).appendFollowUpEvent(eq(0L), eq(WorkerIntent.RENEWED), any());
    }
  }

  @Test
  void registrationReplayAndStateDecodeIgnoreDiscardedRequestIdentity() {
    final var original = worker().setWorkerLeaseToken(42).setLeaseDeadlineEpochMillis(5000);
    final var event = new WorkerRegistrationRecord();
    event.wrap(
        new UnsafeBuffer(
            ("{\"registrationRequestId\":\"old-request\"," + original.toJson().substring(1))
                .getBytes(StandardCharsets.UTF_8)));
    assertEquals("worker", event.getWorkerId());
    assertEquals(42, event.getWorkerLeaseToken());
    assertFalse(event.toJson().contains("registrationRequestId"));
    try (final var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      new WorkerRegisteredApplier(state).applyState(42, event);
      final var stored =
          state
              .getWorkerRegistrationState()
              .registration(new WorkerRegistryKey("demo", "a", "worker"))
              .orElseThrow();
      final var encoded = MsgPackConverter.convertToJson(WorkerRegistrationCodec.toBytes(stored));
      assertFalse(encoded.contains("registrationRequestId"));
      final var restored =
          WorkerRegistrationCodec.fromBytes(
              ("{\"registrationRequestId\":\"old-request\"," + encoded.substring(1))
                  .getBytes(StandardCharsets.UTF_8));
      assertEquals(stored, restored);
    }
  }

  private static WorkerRegistrationRecord worker() {
    return new WorkerRegistrationRecord()
        .setClusterId("demo")
        .setTenantIds(List.of("a", "b"))
        .setWorkerId("worker")
        .setName("payments")
        .setJobType("payment")
        .setMaxJobsActive(10)
        .setConcurrency(2)
        .setRenewalTimeoutMillis(1000)
        .setEventTimestampEpochMillis(1000);
  }

  private static TypedRecord<WorkerRegistrationRecord> command(
      final WorkerRegistrationRecord value, final long timestamp) {
    return new TypedRecord<>() {
      @Override
      public long getKey() {
        return 0;
      }

      @Override
      public WorkerRegistrationRecord getValue() {
        return value;
      }

      @Override
      public String getRequestStreamId() {
        return "stream";
      }

      @Override
      public String getRequestId() {
        return "request-" + timestamp;
      }

      @Override
      public int getLength() {
        return value.getLength();
      }

      @Override
      public long getTimestamp() {
        return timestamp;
      }
    };
  }

  private static Writers writers() {
    final var writers = mock(Writers.class);
    when(writers.state()).thenReturn(mock(StateWriter.class));
    when(writers.response()).thenReturn(mock(TypedResponseWriter.class));
    when(writers.rejection()).thenReturn(mock(TypedRejectionWriter.class));
    return writers;
  }
}
