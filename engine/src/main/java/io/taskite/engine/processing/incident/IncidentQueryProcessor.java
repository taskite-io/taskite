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

import io.taskite.engine.state.immutable.IncidentState;
import io.taskite.engine.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.broker.IncidentRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.IncidentIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.writer.Writers;
import java.util.Objects;

/** Queries the current incident associated with a job key. */
public final class IncidentQueryProcessor implements TypedRecordProcessor<IncidentRecord> {
  private static final String NOT_FOUND_MESSAGE =
      "Expected to query incident with key '%d' for tenant '%s', but no such incident was found";

  private final IncidentState incidents;
  private final Writers writers;

  public IncidentQueryProcessor(final ProcessingState state, final Writers writers) {
    incidents = state.getIncidentState();
    this.writers = writers;
  }

  @Override
  public void processRecord(final TypedRecord<IncidentRecord> command) {
    final long ownerKey = command.getKey();
    final long incidentKey = incidents.getJobIncidentKey(ownerKey);
    final IncidentRecord incident = incidents.getIncident(incidentKey);
    if (incident == null
        || !Objects.equals(incident.getTenantId(), command.getValue().getTenantId())) {
      final String reason = NOT_FOUND_MESSAGE.formatted(ownerKey, command.getValue().getTenantId());
      writers.rejection().appendRejection(RejectionType.NOT_FOUND, reason);
      writers.response().writeRejectionOnCommand(command, RejectionType.NOT_FOUND, reason);
      return;
    }

    writers
        .state()
        .appendFollowUpEvent(incidentKey, ValueType.INCIDENT, IncidentIntent.QUERIED, incident);
    writers.response().writeEventOnCommand(incidentKey, IncidentIntent.QUERIED, incident, command);
  }
}
