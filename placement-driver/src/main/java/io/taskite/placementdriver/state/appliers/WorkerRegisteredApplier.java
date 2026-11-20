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
package io.taskite.placementdriver.state.appliers;

import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.placementdriver.worker.registry.WorkerRegistration;
import io.taskite.placementdriver.worker.registry.WorkerStatus;
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.protocol.record.intent.placementdriver.WorkerIntent;
import io.taskite.streamplatform.api.state.TypedEventApplier;
import java.time.Instant;
import java.util.function.Function;
import java.util.function.Supplier;

public final class WorkerRegisteredApplier
    implements TypedEventApplier<WorkerIntent, WorkerRegistrationRecord> {
  private final MutableProcessingState state;
  private final Function<WorkerRegistrationRecord, Instant> eventTime;

  public WorkerRegisteredApplier(final MutableProcessingState state) {
    this(state, WorkerRegistrationEventTime::fromRecord);
  }

  public WorkerRegisteredApplier(
      final MutableProcessingState state, final Supplier<Instant> nowSupplier) {
    this(state, ignored -> nowSupplier.get());
  }

  private WorkerRegisteredApplier(
      final MutableProcessingState state,
      final Function<WorkerRegistrationRecord, Instant> eventTime) {
    this.state = state;
    this.eventTime = eventTime;
  }

  @Override
  public void applyState(final long key, final WorkerRegistrationRecord value) {
    final Instant now = eventTime.apply(value);
    if (value.getWorkerLeaseToken() > 0) {
      state.getWorkerRegistrationState().registrations().stream()
          .filter(
              current ->
                  current.clusterId().equals(value.getClusterId())
                      && current.workerId().equals(value.getWorkerId())
                      && current.status() == WorkerStatus.ONLINE)
          .map(WorkerRegistration::key)
          .toList()
          .forEach(
              workerKey -> state.getWorkerRegistrationState().markWorkerOffline(workerKey, now));
    }
    value
        .getYieldPlans()
        .forEach(
            plan -> {
              state.getJobYieldState().create(plan);
            });
    value.getTenantIds().stream()
        .distinct()
        .forEach(
            tenantId -> state.getWorkerRegistrationState().registerWorker(value, tenantId, now));
  }
}
