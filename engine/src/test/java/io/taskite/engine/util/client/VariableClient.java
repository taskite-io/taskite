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
package io.taskite.engine.util.client;

import io.taskite.protocol.impl.record.value.broker.VariableDocumentRecord;
import io.taskite.protocol.record.Record;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.VariableDocumentIntent;
import io.taskite.util.BufferUtil;
import java.util.Map;
import java.util.Objects;
import org.agrona.DirectBuffer;

public final class VariableClient {
  private final CommandWriter writer;
  private final VariableDocumentRecord value = new VariableDocumentRecord();

  public VariableClient(final CommandWriter writer) {
    this.writer = Objects.requireNonNull(writer, "writer");
  }

  public VariableClient withJobKey(final long key) {
    value.setJobKey(key);
    return this;
  }

  public VariableClient withTenantId(final String tenant) {
    value.setTenantId(tenant);
    return this;
  }

  public VariableClient withDocument(final Map<String, Object> variables) {
    value.setVariables(Variables.copy(variables));
    return this;
  }

  public VariableClient withDocument(final String json) {
    return withDocument(Variables.read(json));
  }

  public VariableClient withDocument(final DirectBuffer json) {
    return withDocument(BufferUtil.bufferAsString(json));
  }

  public VariableClient withVariable(final String name, final Object variable) {
    return withDocument(Variables.add(value.getVariables(), name, variable));
  }

  public Record<VariableDocumentRecord> update() {
    return writer.send(
        value.getJobKey(),
        ValueType.VARIABLE_DOCUMENT,
        VariableDocumentIntent.UPDATE,
        value,
        VariableDocumentIntent.UPDATED,
        null,
        VariableDocumentRecord.class);
  }

  public RejectionClient expectRejection() {
    return new RejectionClient();
  }

  public final class RejectionClient {
    private RejectionClient() {}

    public Record<?> update() {
      return writer.reject(
          value.getJobKey(),
          ValueType.VARIABLE_DOCUMENT,
          VariableDocumentIntent.UPDATE,
          value,
          VariableDocumentIntent.UPDATED,
          null);
    }
  }
}
