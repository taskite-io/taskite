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

public final class WorkerDeletedApplier
    implements TypedEventApplier<WorkerIntent, WorkerRegistrationRecord> {
  private final MutableProcessingState state;

  public WorkerDeletedApplier(final MutableProcessingState state) {
    this.state = state;
  }

  @Override
  public void applyState(final long key, final WorkerRegistrationRecord value) {
    value.getTenantIds().stream()
        .distinct()
        .map(tenantId -> new WorkerRegistryKey(value.getClusterId(), tenantId, value.getWorkerId()))
        .forEach(
            workerKey -> {
              state
                  .getJobYieldState()
                  .deleteForWorker(
                      workerKey.clusterId(), workerKey.tenantId(), workerKey.workerId());
              state.getWorkerRegistrationState().removeWorker(workerKey);
            });
  }
}
