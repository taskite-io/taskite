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
package io.taskite.engine.processing.incident;

import io.taskite.engine.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.broker.IncidentRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.broker.IncidentIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.writer.Writers;

public final class IncidentResolveProcessor implements TypedRecordProcessor<IncidentRecord> {
  private static final String INCIDENT_NOT_FOUND =
      "Expected to resolve incident with key '%d' for tenant '%s', but no such incident was found";
  private static final String JOB_NOT_FOUND =
      "Expected to resolve incident with key '%d' for tenant '%s', but its job with key '%d' was not found";
  private static final String JOB_HAS_NO_RETRIES =
      "Expected to resolve incident with key '%d' for tenant '%s' for job with key '%d' with positive retries, but"
          + " its retries are '%d'; update job retries before resolving the incident";

  private final ProcessingState state;
  private final Writers writers;

  public IncidentResolveProcessor(final ProcessingState state, final Writers writers) {
    this.state = state;
    this.writers = writers;
  }

  @Override
  public void processRecord(final TypedRecord<IncidentRecord> command) {
    final var incident = state.getIncidentState().getIncident(command.getKey());
    if (incident == null || !incident.getTenantId().equals(command.getValue().getTenantId())) {
      reject(
          command,
          RejectionType.NOT_FOUND,
          String.format(INCIDENT_NOT_FOUND, command.getKey(), command.getValue().getTenantId()));
      return;
    }
    if (incident.getJobKey() > 0) {
      final var job = state.getJobState().getJob(incident.getJobKey());
      if (job == null || job.getRetries() <= 0) {
        reject(
            command,
            RejectionType.INVALID_STATE,
            job == null
                ? String.format(
                    JOB_NOT_FOUND,
                    command.getKey(),
                    command.getValue().getTenantId(),
                    incident.getJobKey())
                : String.format(
                    JOB_HAS_NO_RETRIES,
                    command.getKey(),
                    command.getValue().getTenantId(),
                    incident.getJobKey(),
                    job.getRetries()));
        return;
      }
    }
    writers.state().appendFollowUpEvent(command.getKey(), IncidentIntent.RESOLVED, incident);
    writers
        .response()
        .writeEventOnCommand(command.getKey(), IncidentIntent.RESOLVED, incident, command);
  }

  private void reject(
      final TypedRecord<IncidentRecord> command, final RejectionType type, final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(command, type, reason);
  }
}
