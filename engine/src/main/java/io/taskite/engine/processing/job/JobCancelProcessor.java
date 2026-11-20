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
package io.taskite.engine.processing.job;

import io.taskite.engine.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.IncidentIntent;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.writer.Writers;

public final class JobCancelProcessor implements TypedRecordProcessor<JobRecord> {
  private final ProcessingState state;
  private final Writers writers;

  public JobCancelProcessor(final ProcessingState state, final Writers writers) {
    this.state = state;
    this.writers = writers;
  }

  @Override
  public void processRecord(final TypedRecord<JobRecord> command) {
    final var job = state.getJobState().getJob(command.getKey());
    if (job == null || !job.getTenantId().equals(command.getValue().getTenantId())) {
      final var reason =
          "No job '%d' found for tenant '%s'"
              .formatted(command.getKey(), command.getValue().getTenantId());
      writers.rejection().appendRejection(RejectionType.NOT_FOUND, reason);
      writers.response().writeRejectionOnCommand(command, RejectionType.NOT_FOUND, reason);
      return;
    }
    final var canceled = job.copy();
    final long incidentKey = state.getIncidentState().getJobIncidentKey(command.getKey());
    if (incidentKey > 0) {
      writers
          .state()
          .appendFollowUpEvent(
              incidentKey,
              ValueType.INCIDENT,
              IncidentIntent.RESOLVED,
              state.getIncidentState().getIncident(incidentKey));
    }
    writers.state().appendFollowUpEvent(command.getKey(), JobIntent.CANCELED, canceled);
    writers.response().writeEventOnCommand(command.getKey(), JobIntent.CANCELED, canceled, command);
  }
}
