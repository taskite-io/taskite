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
package io.taskite.engine.state.mutable;

import io.taskite.engine.state.immutable.ProcessingState;
import io.taskite.engine.state.incident.DbIncidentState;
import io.taskite.engine.state.job.DbJobState;
import io.taskite.engine.state.job.DbJobSubscriptionState;
import io.taskite.engine.state.job.DbJobYieldState;
import io.taskite.engine.state.variable.DbVariableState;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.Objects;

public final class MutableProcessingState implements ProcessingState {
  private final MutableJobState jobs;
  private final MutableJobSubscriptionState subscriptions;
  private final MutableIncidentState incidents;
  private final MutableVariableState variables;
  private final DbJobYieldState yields;
  private final boolean workerJobLeaseTokenEnabled;

  public MutableProcessingState(final ProcessingStateStore storage) {
    this(storage, true);
  }

  public MutableProcessingState(final ProcessingStateStore storage, final boolean enabled) {
    Objects.requireNonNull(storage, "storage");
    jobs = new DbJobState(storage);
    subscriptions = new DbJobSubscriptionState(storage);
    incidents = new DbIncidentState(storage);
    variables = new DbVariableState(storage);
    yields = new DbJobYieldState(storage);
    workerJobLeaseTokenEnabled = enabled;
  }

  @Override
  public MutableJobState getJobState() {
    return jobs;
  }

  @Override
  public MutableJobSubscriptionState getJobSubscriptionState() {
    return subscriptions;
  }

  @Override
  public MutableIncidentState getIncidentState() {
    return incidents;
  }

  @Override
  public MutableVariableState getVariableState() {
    return variables;
  }

  @Override
  public DbJobYieldState getJobYieldState() {
    return yields;
  }

  @Override
  public boolean workerJobLeaseTokenEnabled() {
    return workerJobLeaseTokenEnabled;
  }
}
