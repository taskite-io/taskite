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
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.streamplatform.api.state.TypedEventApplier;

public final class JobYieldAcknowledgedApplier
    implements TypedEventApplier<JobYieldIntent, JobYieldRecord> {
  private final MutableProcessingState state;

  public JobYieldAcknowledgedApplier(MutableProcessingState state) {
    this.state = state;
  }

  @Override
  public void applyState(long key, JobYieldRecord value) {
    state.getJobYieldState().updateProgress(value, true);
  }
}
