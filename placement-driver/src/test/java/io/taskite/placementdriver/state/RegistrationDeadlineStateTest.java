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
package io.taskite.placementdriver.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.RegistrationPolicy;
import io.taskite.placementdriver.registry.RegistrationDeadlineType;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.placementdriver.worker.registry.WorkerStatus;
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class RegistrationDeadlineStateTest {
  private static final Instant T0 = Instant.ofEpochMilli(1_000);
  private static final Instant T1 = Instant.ofEpochMilli(2_000);
  private static final Duration BROKER_LEASE_TIMEOUT = Duration.ofMillis(800);
  private static final Duration WORKER_LEASE_TIMEOUT = Duration.ofMillis(900);
  private static final Duration OFFLINE_RETENTION = Duration.ofSeconds(1);

  @TempDir Path directory;

  @Test
  void brokerLifecycleAtomicallyReplacesAndRemovesLeaseDeadline() {
    try (var storage = new PartitionStorage(directory.resolve("broker"))) {
      final var state = state(storage);
      state
          .getBrokerRegistrationState()
          .registerBroker("demo", "broker-0", "127.0.0.1", 26501, 26502, "1", 500, T0, List.of());

      var deadlines = state.getRegistrationDeadlineState().findDue(Long.MAX_VALUE, 10);
      assertEquals(1, deadlines.size());
      assertEquals(RegistrationDeadlineType.BROKER_LEASE, deadlines.getFirst().type());
      assertEquals(1_800, deadlines.getFirst().dueDate());

      state.getBrokerRegistrationState().renewBroker("demo", "broker-0", T1, List.of());
      deadlines = state.getRegistrationDeadlineState().findDue(Long.MAX_VALUE, 10);
      assertEquals(1, deadlines.size());
      assertEquals(2_800, deadlines.getFirst().dueDate());

      state.getBrokerRegistrationState().markBrokerOffline("demo", "broker-0", T1);
      assertEquals(Long.MAX_VALUE, state.getRegistrationDeadlineState().getNextDueDate());
    }
  }

  @Test
  void workerLifecycleSwitchesBetweenLeaseAndOfflineRetentionDeadline() {
    try (var storage = new PartitionStorage(directory.resolve("worker"))) {
      final var state = state(storage);
      final WorkerRegistrationRecord heartbeat = heartbeat();
      state.getWorkerRegistrationState().registerWorker(heartbeat, "tenant-a", T0);

      var deadline = state.getRegistrationDeadlineState().findDue(Long.MAX_VALUE, 10).getFirst();
      assertEquals(RegistrationDeadlineType.WORKER_LEASE, deadline.type());
      assertEquals(1_900, deadline.dueDate());

      state
          .getWorkerRegistrationState()
          .markWorkerOffline(
              state.getWorkerRegistrationState().registrations().getFirst().key(), T1);
      deadline = state.getRegistrationDeadlineState().findDue(Long.MAX_VALUE, 10).getFirst();
      assertEquals(RegistrationDeadlineType.WORKER_OFFLINE_RETENTION, deadline.type());
      assertEquals(3_000, deadline.dueDate());

      state.getWorkerRegistrationState().renewWorker(heartbeat, "tenant-a", T1);
      deadline = state.getRegistrationDeadlineState().findDue(Long.MAX_VALUE, 10).getFirst();
      assertEquals(RegistrationDeadlineType.WORKER_LEASE, deadline.type());
      assertEquals(2_900, deadline.dueDate());
      assertEquals(
          WorkerStatus.ONLINE,
          state.getWorkerRegistrationState().registrations().getFirst().status());

      state
          .getWorkerRegistrationState()
          .removeWorker(state.getWorkerRegistrationState().registrations().getFirst().key());
      assertTrue(state.getRegistrationDeadlineState().findDue(Long.MAX_VALUE, 10).isEmpty());
    }
  }

  @Test
  void rebuildRestoresDerivedDeadlinesFromRegistrations() {
    try (var storage = new PartitionStorage(directory.resolve("rebuild"))) {
      final var state = state(storage);
      state
          .getBrokerRegistrationState()
          .registerBroker("demo", "broker-0", "127.0.0.1", 26501, 26502, "1", 500, T0, List.of());
      state.getWorkerRegistrationState().registerWorker(heartbeat(), "tenant-a", T0);
      final var brokers = state.getBrokerRegistrationState().registrations("demo");
      final var workers = state.getWorkerRegistrationState().registrations();

      state.getRegistrationDeadlineState().rebuild(List.of(), List.of());
      assertEquals(Long.MAX_VALUE, state.getRegistrationDeadlineState().getNextDueDate());

      state.getRegistrationDeadlineState().rebuild(brokers, workers);
      final var deadlines = state.getRegistrationDeadlineState().findDue(Long.MAX_VALUE, 10);
      assertEquals(2, deadlines.size());
      assertEquals(List.of(1_800L, 1_900L), deadlines.stream().map(d -> d.dueDate()).toList());
    }
  }

  private static MutableProcessingState state(final PartitionStorage storage) {
    return new MutableProcessingState(
        new StateStoreProcessingStore(storage),
        () -> 0,
        () -> 0,
        50,
        new RegistrationPolicy(BROKER_LEASE_TIMEOUT, WORKER_LEASE_TIMEOUT, OFFLINE_RETENTION));
  }

  private static WorkerRegistrationRecord heartbeat() {
    return new WorkerRegistrationRecord()
        .setClusterId("demo")
        .setTenantIds(List.of("tenant-a"))
        .setWorkerId("instance-1")
        .setName("payments")
        .setJobType("payment")
        .setMaxJobsActive(32)
        .setConcurrency(4)
        .setRenewalTimeoutMillis(300);
  }
}
