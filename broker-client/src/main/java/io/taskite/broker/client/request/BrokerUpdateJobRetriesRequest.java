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

import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.util.BufferWriter;
import org.agrona.DirectBuffer;

public class BrokerUpdateJobRetriesRequest extends BrokerExecuteCommand<JobRecord> {
  private final JobRecord requestDto = new JobRecord();

  public BrokerUpdateJobRetriesRequest(final long key, final int retries) {
    super(ValueType.JOB, JobIntent.UPDATE_RETRIES);
    if (key <= 0 || retries < 0) {
      throw new IllegalArgumentException("Job key must be positive and retries non-negative");
    }
    request.setKey(key);
    requestDto.setRetries(retries);
  }

  @Override
  protected JobRecord toResponseDto(final DirectBuffer buffer) {
    final JobRecord responseDto = new JobRecord();
    responseDto.wrap(buffer);
    return responseDto;
  }

  @Override
  public BufferWriter getRequestWriter() {
    return requestDto;
  }

  @Override
  public void setTenantId(final String tenantId) {
    requestDto.setTenantId(tenantId);
  }
}
