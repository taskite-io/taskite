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
package io.taskite.engine.processing.common;

import io.taskite.engine.state.immutable.JobState;
import io.taskite.engine.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import java.util.Objects;

/** Broker-local session checks; Gateway and processors never query PD synchronously. */
public final class WorkerJobLeaseCheck {
  private final ProcessingState state;

  public WorkerJobLeaseCheck(ProcessingState state) {
    this.state = state;
  }

  public boolean ready() {
    return !state.workerJobLeaseTokenEnabled()
        || (state.getJobState().workerIndexReady() && state.getJobYieldState().sessionsReady());
  }

  public boolean canActivate(String tenantId, String workerId, long token) {
    return !state.workerJobLeaseTokenEnabled()
        || (state.getJobState().workerIndexReady()
            && state.getJobYieldState().sessionsReady()
            && tenantId != null
            && !tenantId.isBlank()
            && workerId != null
            && !workerId.isBlank()
            && token > 0
            && !state.getJobYieldState().isRevoked(tenantId, workerId, token));
  }

  public boolean canFinish(long key, JobRecord stored, JobRecord request) {
    if (!state.workerJobLeaseTokenEnabled()) {
      return true;
    }
    if (state.getJobState().getState(key) != JobState.State.ACTIVATED) {
      return false;
    }
    if (stored.getWorkerLeaseToken() == 0) {
      return request.getWorkerLeaseToken() == 0 && request.getWorkerId().isBlank();
    }
    return state.getJobState().workerIndexReady()
        && state.getJobYieldState().sessionsReady()
        && request.getWorkerLeaseToken() > 0
        && stored.getWorkerLeaseToken() == request.getWorkerLeaseToken()
        && Objects.equals(stored.getWorkerId(), request.getWorkerId())
        && !state
            .getJobYieldState()
            .isRevoked(stored.getTenantId(), stored.getWorkerId(), stored.getWorkerLeaseToken());
  }
}
