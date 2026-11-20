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

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.taskite.placementdriver.state.appliers.WorkerDeletedApplier;
import io.taskite.placementdriver.state.appliers.WorkerDeregisteredApplier;
import io.taskite.placementdriver.state.appliers.WorkerRegisteredApplier;
import io.taskite.placementdriver.state.appliers.WorkerRenewedApplier;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.placementdriver.worker.registry.WorkerRegistration;
import io.taskite.placementdriver.worker.registry.WorkerStatus;
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class WorkerRegistrationStateTest {
  private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
  private static final Instant T1 = Instant.parse("2026-01-01T00:00:10Z");

  @TempDir Path tempDir;

  @Test
  void expandsTenantsAndRefreshesWorkerConfiguration() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("workers"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      final WorkerRegistrationRecord renewal = renewal();
      new WorkerRegisteredApplier(state, () -> T0).applyState(1, renewal);

      final List<WorkerRegistration> registered =
          state.getWorkerRegistrationState().registrations();
      assertEquals(2, registered.size());
      assertEquals(
          List.of("tenant-a", "tenant-b"),
          registered.stream().map(WorkerRegistration::tenantId).toList());
      assertEquals(WorkerStatus.ONLINE, registered.getFirst().status());
      assertEquals(T0, registered.getFirst().registeredAt());
      assertEquals(32, registered.getFirst().maxJobsActive());

      renewal.setActiveJobs(7).setMaxJobsActive(48).setConcurrency(8);
      new WorkerRenewedApplier(state, () -> T1).applyState(2, renewal);

      final WorkerRegistration renewed =
          state.getWorkerRegistrationState().registrations().getFirst();
      assertEquals(T0, renewed.registeredAt());
      assertEquals(T1, renewed.lastRenewal());
      assertEquals(7, renewed.activeJobs());
      assertEquals(48, renewed.maxJobsActive());
      assertEquals(8, renewed.concurrency());
    }
  }

  @Test
  void deregisterMarksEveryTenantOfflineWithoutDeletingHistory() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("deregister"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      final WorkerRegistrationRecord renewal = renewal();
      new WorkerRegisteredApplier(state, () -> T0).applyState(1, renewal);
      new WorkerDeregisteredApplier(state, () -> T1).applyState(2, renewal);

      final List<WorkerRegistration> registrations =
          state.getWorkerRegistrationState().registrations();
      assertEquals(2, registrations.size());
      registrations.forEach(
          worker -> {
            assertEquals(WorkerStatus.OFFLINE, worker.status());
            assertEquals(T0, worker.lastRenewal());
            assertEquals(T1, worker.statusChangedAt());
          });
    }
  }

  @Test
  void deletedEventRemovesOnlyTheTargetWorkerInstances() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("delete"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      final WorkerRegistrationRecord renewal = renewal();
      new WorkerRegisteredApplier(state, () -> T0).applyState(1, renewal);

      new WorkerDeletedApplier(state).applyState(2, renewal.setTenantIds(List.of("tenant-a")));

      final List<WorkerRegistration> registrations =
          state.getWorkerRegistrationState().registrations();
      assertEquals(1, registrations.size());
      assertEquals("tenant-b", registrations.getFirst().tenantId());
    }
  }

  private static WorkerRegistrationRecord renewal() {
    return new WorkerRegistrationRecord()
        .setClusterId("demo")
        .setTenantIds(List.of("tenant-a", "tenant-b"))
        .setWorkerId("instance-1")
        .setName("payments")
        .setJobType("payment")
        .setMaxJobsActive(32)
        .setActiveJobs(3)
        .setConcurrency(4)
        .setTimeoutMillis(30_000)
        .setRequestTimeoutMillis(10_000)
        .setPollIntervalMillis(100)
        .setFetchVariables(List.of("orderId"))
        .setClientVersion("0.1.0")
        .setRenewalTimeoutMillis(30_000);
  }
}
