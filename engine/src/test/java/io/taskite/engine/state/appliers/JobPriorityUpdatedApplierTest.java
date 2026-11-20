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
package io.taskite.engine.state.appliers;

import static org.assertj.core.api.Assertions.assertThat;

import io.taskite.engine.state.immutable.JobState.State;
import io.taskite.engine.state.mutable.MutableJobState;
import io.taskite.engine.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.util.BufferUtil;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class JobPriorityUpdatedApplierTest {
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"activatable", "activated", "backed-off", "exhausted", "error"})
  void shouldRollbackReplayAndRecoverPriorityWithoutChangingLifecycleOrIndexes(
      final String scenario) throws Exception {
    // given
    final State lifecycle =
        switch (scenario) {
          case "activatable" -> State.ACTIVATABLE;
          case "activated" -> State.ACTIVATED;
          case "error" -> State.ERROR_THROWN;
          default -> State.FAILED;
        };
    final var initial =
        new JobRecord()
            .setJobType("work")
            .setTenantId("tenant-a")
            .setWorker("worker")
            .setWorkerId("owner")
            .setWorkerLeaseToken(42)
            .setRetries(scenario.equals("exhausted") ? 0 : 2)
            .setPriority(-1)
            .setDeadline(scenario.equals("activated") ? 65_000 : 0)
            .setTimeout(60_000)
            .setRetryBackoff(scenario.equals("backed-off") ? 1000 : 0)
            .setRecurringTime(scenario.equals("backed-off") ? 70_000 : 0)
            .setErrorMessage("temporary")
            .setVariablesMap(Map.of("amount", 42));
    final JobRecord committed;
    try (final var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      final var jobs = state.getJobState();
      final var appliers = BrokerEventAppliers.create(state);
      store.getCurrentTransaction();
      jobs.update(1, initial, lifecycle);
      jobs.create(2, new JobRecord().setJobType("work").setTenantId("tenant-a").setPriority(5));
      jobs.create(
          3,
          new JobRecord()
              .setJobType("work")
              .setTenantId("tenant-b")
              .setPriority(Integer.MAX_VALUE));
      final var before = jobs.getJob(1).copy();
      committed = before.copy().setPriority(Integer.MAX_VALUE);
      store.getCurrentTransaction().commit();
      store.getCurrentTransaction();

      // when
      appliers.applyState(1, JobIntent.PRIORITY_UPDATED, committed, 1);
      appliers.applyState(1, JobIntent.PRIORITY_UPDATED, committed, 1);

      // then
      assertStateAndIndexes(jobs, lifecycle, committed);
      store.getCurrentTransaction().rollback();
      store.getCurrentTransaction();
      assertStateAndIndexes(jobs, lifecycle, before);

      // when
      appliers.applyState(1, JobIntent.PRIORITY_UPDATED, committed, 1);
      store.getCurrentTransaction().commit();
    }

    // then
    try (final var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      store.getCurrentTransaction();
      assertStateAndIndexes(state.getJobState(), lifecycle, committed);
      store.getCurrentTransaction().rollback();
    }
  }

  private static void assertStateAndIndexes(
      final MutableJobState jobs, final State lifecycle, final JobRecord expected) {
    assertThat(jobs.getState(1)).isEqualTo(lifecycle);
    assertThat(jobs.getJob(1).toJson()).isEqualTo(expected.toJson());
    final List<Long> activatable = new ArrayList<>();
    jobs.forEachActivatableJobs(
        BufferUtil.wrapString("work"),
        List.of("tenant-a"),
        (key, record) -> {
          activatable.add(key);
          return true;
        });
    assertThat(activatable)
        .containsExactlyElementsOf(
            lifecycle == State.ACTIVATABLE
                ? expected.getPriority() > 5 ? List.of(1L, 2L) : List.of(2L, 1L)
                : List.of(2L));
    final boolean active = lifecycle == State.ACTIVATED;
    assertThat(jobs.findActivatedByWorker("tenant-a", "owner", 42, 10))
        .containsExactlyElementsOf(active ? List.of(1L) : List.of());
    assertThat(jobs.findTimedOutJobs(65_000, 10).stream().map(due -> due.key()).toList())
        .containsExactlyElementsOf(active ? List.of(1L) : List.of());
    assertThat(jobs.getNextTimeoutDueDate()).isEqualTo(active ? 65_000 : Long.MAX_VALUE);
    final boolean backedOff = lifecycle == State.FAILED && expected.getRetries() > 0;
    assertThat(jobs.findBackedOffJobs(70_000, 10).stream().map(due -> due.key()).toList())
        .containsExactlyElementsOf(backedOff ? List.of(1L) : List.of());
    assertThat(jobs.getNextBackoffDueDate()).isEqualTo(backedOff ? 70_000 : Long.MAX_VALUE);
    assertThat(jobs.findTimedOutJobs(64_999, 10)).isEmpty();
    assertThat(jobs.findBackedOffJobs(69_999, 10)).isEmpty();
  }
}
