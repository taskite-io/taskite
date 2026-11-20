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
package io.taskite.engine.processing.variable;

import io.taskite.engine.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.broker.VariableDocumentRecord;
import io.taskite.protocol.record.*;
import io.taskite.protocol.record.intent.broker.VariableDocumentIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.Writers;

public final class VariableDocumentUpdateProcessor
    implements TypedRecordProcessor<VariableDocumentRecord> {
  private final ProcessingState state;
  private final Writers writers;
  private final KeyGenerator keys;
  private final VariableBehavior variables;

  public VariableDocumentUpdateProcessor(
      final ProcessingState state, final Writers writers, final KeyGenerator keys) {
    this.state = state;
    this.writers = writers;
    this.keys = keys;
    variables = new VariableBehavior(state.getVariableState(), writers.state(), keys);
  }

  @Override
  public void processRecord(final TypedRecord<VariableDocumentRecord> command) {
    final var request = command.getValue();
    final var job = state.getJobState().getJob(request.getJobKey());
    if (job == null || !job.getTenantId().equals(request.getTenantId())) {
      final var reason =
          "No job '%d' found for tenant '%s'".formatted(request.getJobKey(), request.getTenantId());
      writers.rejection().appendRejection(RejectionType.NOT_FOUND, reason);
      writers.response().writeRejectionOnCommand(command, RejectionType.NOT_FOUND, reason);
      return;
    }
    variables.merge(request.getJobKey(), request.getTenantId(), request.getVariables());
    final long key = keys.nextKey();
    writers
        .state()
        .appendFollowUpEvent(
            key, ValueType.VARIABLE_DOCUMENT, VariableDocumentIntent.UPDATED, request);
    writers.response().writeEventOnCommand(key, VariableDocumentIntent.UPDATED, request, command);
  }
}
