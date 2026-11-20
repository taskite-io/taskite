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

import io.taskite.engine.state.immutable.JobState;
import io.taskite.engine.state.mutable.MutableIncidentState;
import io.taskite.engine.state.mutable.MutableJobState;
import io.taskite.protocol.impl.record.value.broker.IncidentRecord;
import io.taskite.protocol.record.intent.broker.IncidentIntent;
import io.taskite.streamplatform.api.state.TypedEventApplier;

final class IncidentResolvedApplier implements TypedEventApplier<IncidentIntent, IncidentRecord> {

  private final MutableIncidentState incidentState;
  private final MutableJobState jobState;

  IncidentResolvedApplier(
      final MutableIncidentState incidentState, final MutableJobState jobState) {
    this.incidentState = incidentState;
    this.jobState = jobState;
  }

  @Override
  public void applyState(final long key, final IncidentRecord value) {
    if (value.getJobKey() > 0) {
      final var job = jobState.getJob(value.getJobKey());
      if (job != null && job.getRetries() > 0) {
        jobState.update(value.getJobKey(), job.copy(), JobState.State.ACTIVATABLE);
      }
    }
    incidentState.deleteIncident(key);
  }
}
