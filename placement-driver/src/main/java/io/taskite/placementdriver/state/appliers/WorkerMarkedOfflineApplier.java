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
import io.taskite.placementdriver.worker.registry.WorkerRegistryKey;
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.protocol.record.intent.placementdriver.WorkerIntent;
import io.taskite.streamplatform.api.state.TypedEventApplier;
import java.time.Instant;
import java.util.function.Function;
import java.util.function.Supplier;

public final class WorkerMarkedOfflineApplier
    implements TypedEventApplier<WorkerIntent, WorkerRegistrationRecord> {
  private final MutableProcessingState state;
  private final Function<WorkerRegistrationRecord, Instant> eventTime;

  public WorkerMarkedOfflineApplier(final MutableProcessingState state) {
    this(state, WorkerRegistrationEventTime::fromRecord);
  }

  public WorkerMarkedOfflineApplier(
      final MutableProcessingState state, final Supplier<Instant> nowSupplier) {
    this(state, ignored -> nowSupplier.get());
  }

  private WorkerMarkedOfflineApplier(
      final MutableProcessingState state,
      final Function<WorkerRegistrationRecord, Instant> eventTime) {
    this.state = state;
    this.eventTime = eventTime;
  }

  @Override
  public void applyState(final long key, final WorkerRegistrationRecord value) {
    final Instant now = eventTime.apply(value);
    value.getYieldPlans().forEach(state.getJobYieldState()::create);
    value.getTenantIds().stream()
        .distinct()
        .map(tenantId -> new WorkerRegistryKey(value.getClusterId(), tenantId, value.getWorkerId()))
        .forEach(workerKey -> state.getWorkerRegistrationState().markWorkerOffline(workerKey, now));
  }
}
