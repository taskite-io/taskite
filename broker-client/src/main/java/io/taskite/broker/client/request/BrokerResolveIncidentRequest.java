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

import io.taskite.protocol.impl.record.value.broker.IncidentRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.IncidentIntent;
import io.taskite.util.BufferWriter;
import org.agrona.DirectBuffer;

/** Routes an incident resolution command by incident key. */
public final class BrokerResolveIncidentRequest extends BrokerExecuteCommand<IncidentRecord> {

  private final IncidentRecord requestDto = new IncidentRecord();

  public BrokerResolveIncidentRequest(final long incidentKey) {
    super(ValueType.INCIDENT, IncidentIntent.RESOLVE);
    if (incidentKey <= 0) {
      throw new IllegalArgumentException("incidentKey must be positive");
    }
    request.setKey(incidentKey);
  }

  @Override
  public BufferWriter getRequestWriter() {
    return requestDto;
  }

  @Override
  protected IncidentRecord toResponseDto(final DirectBuffer buffer) {
    final var responseDto = new IncidentRecord();
    responseDto.wrap(buffer);
    return responseDto;
  }

  @Override
  public void setTenantId(final String tenantId) {
    requestDto.setTenantId(tenantId);
  }
}
