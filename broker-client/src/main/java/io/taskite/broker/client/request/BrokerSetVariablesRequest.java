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
package io.taskite.broker.client.request;

import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.record.value.broker.VariableDocumentRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.VariableDocumentIntent;
import io.taskite.util.BufferWriter;
import java.util.Map;
import org.agrona.DirectBuffer;

/** Routes a variable-document update by job key. */
public final class BrokerSetVariablesRequest extends BrokerExecuteCommand<VariableDocumentRecord> {

  private final VariableDocumentRecord requestDto = new VariableDocumentRecord();

  public BrokerSetVariablesRequest(final long jobKey, final Map<String, Object> variables) {
    super(ValueType.VARIABLE_DOCUMENT, VariableDocumentIntent.UPDATE);
    if (jobKey <= 0) {
      throw new IllegalArgumentException("jobKey must be positive");
    }
    request.setPartitionId(Protocol.decodePartitionId(jobKey));
    requestDto.setJobKey(jobKey).setVariables(variables);
  }

  @Override
  public BufferWriter getRequestWriter() {
    return requestDto;
  }

  @Override
  protected VariableDocumentRecord toResponseDto(final DirectBuffer buffer) {
    final var responseDto = new VariableDocumentRecord();
    responseDto.wrap(buffer);
    return responseDto;
  }

  @Override
  public void setTenantId(final String tenantId) {
    requestDto.setTenantId(tenantId);
  }
}
