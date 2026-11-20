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

/** Routes a priority update command to the partition encoded in the job key. */
public final class BrokerUpdateJobPriorityRequest extends BrokerExecuteCommand<JobRecord> {
  private static final String INVALID_JOB_KEY = "jobKey must be positive";

  private final JobRecord requestDto = new JobRecord();

  public BrokerUpdateJobPriorityRequest(final long jobKey, final int priority) {
    super(ValueType.JOB, JobIntent.UPDATE_PRIORITY);
    if (jobKey <= 0) {
      throw new IllegalArgumentException(INVALID_JOB_KEY);
    }
    request.setKey(jobKey);
    requestDto.setPriority(priority);
  }

  @Override
  public BufferWriter getRequestWriter() {
    return requestDto;
  }

  @Override
  protected JobRecord toResponseDto(final DirectBuffer buffer) {
    final var responseDto = new JobRecord();
    responseDto.wrap(buffer);
    return responseDto;
  }

  @Override
  public void setTenantId(final String tenantId) {
    requestDto.setTenantId(tenantId);
  }
}
