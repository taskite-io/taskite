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

import io.taskite.engine.state.immutable.IncidentState;
import io.taskite.engine.state.immutable.JobState;
import io.taskite.engine.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.broker.IncidentRecord;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.impl.record.value.broker.VariableRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class JobSubscriptionAppliersTest {
  @TempDir Path directory;

  @Test
  void deletesEveryInFlightJobAndIncidentOnlyForTheSubscriptionKey() throws Exception {
    try (final var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      store.getCurrentTransaction();
      populate(state);
      store.getCurrentTransaction().commit();
      store.getCurrentTransaction();

      deleteSubscription(state);

      assertDeletedScope(state);
      assertUnrelatedScope(state);
      store.getCurrentTransaction().commit();
    }
    try (final var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      store.getCurrentTransaction();
      assertDeletedScope(state);
      assertUnrelatedScope(state);
      // A replay cannot cancel independent jobs created after the original deletion.
      state.getJobState().create(301, job("charge", "tenant"));
      deleteSubscription(state);
      assertThat(state.getJobState().exists(301)).isTrue();
      store.getCurrentTransaction().rollback();
    }
  }

  @Test
  void rollsBackJobsIncidentsVariablesIndexesAndSubscriptionTogether() throws Exception {
    try (final var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      store.getCurrentTransaction();
      populate(state);
      store.getCurrentTransaction().commit();
      store.getCurrentTransaction();

      deleteSubscription(state);
      assertDeletedScope(state);
      store.getCurrentTransaction().rollback();
      store.getCurrentTransaction();

      assertThat(state.getJobSubscriptionState().get(42)).isNotNull();
      assertThat(state.getJobSubscriptionState().findSubscriptionKey("charge", "tenant"))
          .isEqualTo(42);
      assertThat(jobs(state, 42)).containsExactly(101L, 102L, 103L, 104L, 105L);
      assertThat(state.getJobState().getState(101)).isEqualTo(JobState.State.ACTIVATABLE);
      assertThat(state.getJobState().getState(102)).isEqualTo(JobState.State.ACTIVATED);
      assertThat(state.getJobState().getState(103)).isEqualTo(JobState.State.FAILED);
      assertThat(state.getJobState().getState(104)).isEqualTo(JobState.State.ERROR_THROWN);
      assertThat(state.getJobState().getState(105)).isEqualTo(JobState.State.FAILED);
      assertThat(state.getJobState().findActivatedByWorker("tenant", "worker", 7, 10))
          .containsExactly(102L, 201L);
      assertThat(state.getJobState().findTimedOutJobs(1000, 10))
          .extracting(JobState.DueJob::key)
          .containsExactly(102L);
      assertThat(state.getJobState().findBackedOffJobs(2000, 10))
          .extracting(JobState.DueJob::key)
          .containsExactly(103L);
      for (long key = 101; key <= 105; key++) {
        assertThat(state.getVariableState().getVariables(key)).containsEntry("amount", 42);
      }
      assertThat(state.getIncidentState().getJobIncidentKey(104)).isEqualTo(404);
      assertThat(state.getIncidentState().getJobIncidentKey(105)).isEqualTo(405);
      assertThat(state.getIncidentState().getIncidentCount()).isEqualTo(3);
      assertUnrelatedScope(state);
      store.getCurrentTransaction().rollback();
    }
  }

  @Test
  void repeatingDeletionDoesNotDeleteTheNewSubscriptionForTheSameTypeAndTenant() throws Exception {
    try (final var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      store.getCurrentTransaction();
      populate(state);
      deleteSubscription(state);
      assertDeletedScope(state);
      assertUnrelatedScope(state);
      state
          .getJobSubscriptionState()
          .create(43, new JobSubscriptionRecord().setJobType("charge").setTenantId("tenant"));
      state.getJobState().create(303, job("charge", "tenant").setSubscriptionKey(43));
      deleteSubscription(state);
      assertThat(state.getJobSubscriptionState().get(42)).isNull();
      assertThat(state.getJobSubscriptionState().findSubscriptionKey("charge", "tenant"))
          .isEqualTo(43);
      assertThat(jobs(state, 42)).isEmpty();
      assertThat(jobs(state, 43)).containsExactly(201L, 303L);
      assertThat(state.getJobState().exists(303)).isTrue();
      assertThat(state.getIncidentState().getIncident(501)).isNotNull();
      store.getCurrentTransaction().rollback();
    }
  }

  private static void populate(final MutableProcessingState state) {
    state
        .getJobSubscriptionState()
        .create(42, new JobSubscriptionRecord().setJobType("charge").setTenantId("tenant"));
    state.getJobState().create(101, job("charge", "tenant").setSubscriptionKey(42));
    state
        .getJobState()
        .activate(
            102,
            job("charge", "tenant")
                .setSubscriptionKey(42)
                .setDeadline(1000)
                .setWorkerId("worker")
                .setWorkerLeaseToken(7));
    state
        .getJobState()
        .update(
            103,
            job("charge", "tenant").setSubscriptionKey(42).setRetries(2).setRecurringTime(2000),
            JobState.State.FAILED);
    state
        .getJobState()
        .update(
            104,
            job("charge", "tenant").setSubscriptionKey(42).setRetries(0),
            JobState.State.ERROR_THROWN);
    state
        .getJobState()
        .update(
            105,
            job("charge", "tenant").setSubscriptionKey(42).setRetries(0),
            JobState.State.FAILED);
    state
        .getJobState()
        .activate(
            201,
            job("charge", "tenant")
                .setSubscriptionKey(43)
                .setDeadline(3000)
                .setWorkerId("worker")
                .setWorkerLeaseToken(7));
    state.getJobState().create(202, job("charge", "tenant"));
    for (final long key : List.of(101L, 102L, 103L, 104L, 105L, 201L, 202L)) {
      state
          .getVariableState()
          .createVariable(
              1000 + key, new VariableRecord().setJobKey(key).setName("amount").setValue(42));
      state
          .getVariableState()
          .createVariable(
              2000 + key, new VariableRecord().setJobKey(key).setName("name").setValue("payment"));
    }
    for (final long key : List.of(104L, 105L, 201L)) {
      state
          .getIncidentState()
          .createIncident(
              key + 300,
              new IncidentRecord().setJobKey(key).setTenantId("tenant").setErrorMessage("failed"));
    }
  }

  private static JobRecord job(final String type, final String tenant) {
    return new JobRecord().setJobType(type).setTenantId(tenant).setVariablesJson("{}");
  }

  private static void deleteSubscription(final MutableProcessingState state) {
    BrokerEventAppliers.create(state)
        .applyState(
            42,
            JobSubscriptionIntent.DELETED,
            new JobSubscriptionDeletionRecord().setSubscriptionKey(42).setTenantId("tenant"),
            1);
  }

  private static List<Long> jobs(final MutableProcessingState state, final long subscriptionKey) {
    final List<Long> result = new ArrayList<>();
    state
        .getJobState()
        .forEachJobs(
            subscriptionKey,
            (key, ignored) -> {
              result.add(key);
              return true;
            });
    return result;
  }

  private static void assertDeletedScope(final MutableProcessingState state) {
    assertThat(state.getJobSubscriptionState().get(42)).isNull();
    assertThat(state.getJobSubscriptionState().findSubscriptionKey("charge", "tenant")).isZero();
    assertThat(jobs(state, 42)).isEmpty();
    for (long key = 101; key <= 105; key++) {
      assertThat(state.getJobState().getJob(key)).isNull();
      assertThat(state.getJobState().getState(key)).isEqualTo(JobState.State.NOT_FOUND);
      assertThat(state.getVariableState().getVariables(key)).isEmpty();
      assertThat(state.getIncidentState().getJobIncidentKey(key))
          .isEqualTo(IncidentState.MISSING_INCIDENT);
    }
    assertThat(state.getIncidentState().getIncident(404)).isNull();
    assertThat(state.getIncidentState().getIncident(405)).isNull();
    assertThat(state.getJobState().findActivatedByWorker("tenant", "worker", 7, 10))
        .containsExactly(201L);
    assertThat(state.getJobState().findTimedOutJobs(Long.MAX_VALUE, 10))
        .extracting(JobState.DueJob::key)
        .containsExactly(201L);
    assertThat(state.getJobState().findBackedOffJobs(Long.MAX_VALUE, 10)).isEmpty();
    final List<Long> activatable = new ArrayList<>();
    state
        .getJobState()
        .forEachActivatableJobs(
            io.taskite.util.BufferUtil.wrapString("charge"),
            List.of("tenant"),
            (key, ignored) -> {
              activatable.add(key);
              return true;
            });
    assertThat(activatable).containsExactly(202L);
  }

  private static void assertUnrelatedScope(final MutableProcessingState state) {
    assertThat(state.getJobState().getState(201)).isEqualTo(JobState.State.ACTIVATED);
    assertThat(state.getJobState().getState(202)).isEqualTo(JobState.State.ACTIVATABLE);
    assertThat(jobs(state, 43)).containsExactly(201L);
    assertThat(state.getJobState().getJob(202).getSubscriptionKey()).isZero();
    assertThat(state.getIncidentState().getJobIncidentKey(201)).isEqualTo(501);
    assertThat(state.getIncidentState().getIncident(501)).isNotNull();
    for (final long key : List.of(201L, 202L)) {
      assertThat(state.getVariableState().getVariables(key))
          .containsEntry("amount", 42)
          .containsEntry("name", "payment");
    }
  }

  @Test
  void physicalDeletionSurvivesRecoveryAndAllowsRecreation() throws Exception {
    final var subscription = new JobSubscriptionRecord().setJobType("charge").setTenantId("tenant");
    try (final var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      final var appliers = BrokerEventAppliers.create(state);
      store.getCurrentTransaction();
      appliers.applyState(42, JobSubscriptionIntent.CREATED, subscription, 1);
      assertThat(state.getJobSubscriptionState().get(42).getJobType()).isEqualTo("charge");
      appliers.applyState(
          42,
          JobSubscriptionIntent.DELETED,
          new JobSubscriptionDeletionRecord().setSubscriptionKey(42),
          1);
      assertThat(state.getJobSubscriptionState().get(42)).isNull();
      store.getCurrentTransaction().commit();
    }
    try (final var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      store.getCurrentTransaction();
      assertThat(state.getJobSubscriptionState().get(42)).isNull();
      BrokerEventAppliers.create(state)
          .applyState(42, JobSubscriptionIntent.CREATED, subscription, 1);
      assertThat(state.getJobSubscriptionState().get(42).getJobType()).isEqualTo("charge");
      store.getCurrentTransaction().commit();
    }
    try (final var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      store.getCurrentTransaction();
      assertThat(state.getJobSubscriptionState().get(42).getJobType()).isEqualTo("charge");
      store.getCurrentTransaction().rollback();
    }
  }
}
