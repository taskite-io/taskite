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

import io.taskite.engine.state.immutable.VariableState;
import io.taskite.protocol.impl.record.value.broker.VariableRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.VariableIntent;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.StateWriter;
import java.util.Map;
import java.util.Objects;

/** Produces job-variable events; persistence belongs to the intent-specific appliers. */
public final class VariableBehavior {
  private final VariableState state;
  private final StateWriter writer;
  private final KeyGenerator keys;

  public VariableBehavior(
      final VariableState state, final StateWriter writer, final KeyGenerator keys) {
    this.state = state;
    this.writer = writer;
    this.keys = keys;
  }

  public void merge(final long jobKey, final String tenantId, final Map<String, Object> variables) {
    variables.forEach(
        (name, value) -> {
          final var existing = state.getVariableLocal(jobKey, name);
          if (existing != null && Objects.deepEquals(existing.value(), value)) {
            return;
          }
          final var record =
              new VariableRecord()
                  .setJobKey(jobKey)
                  .setTenantId(tenantId)
                  .setName(name)
                  .setValue(value);
          writer.appendFollowUpEvent(
              existing == null ? keys.nextKey() : existing.key(),
              ValueType.VARIABLE,
              existing == null ? VariableIntent.CREATED : VariableIntent.UPDATED,
              record);
        });
  }
}
