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
import java.util.Map;
import org.agrona.DirectBuffer;

/** Routes a job failure command by job key. */
public final class BrokerFailJobRequest extends BrokerExecuteCommand<JobRecord> {

  private final JobRecord requestDto = new JobRecord();

  public BrokerFailJobRequest(
      final long jobKey,
      final int retries,
      final String errorMessage,
      final long retryBackoff,
      final Map<String, Object> variables) {
    super(ValueType.JOB, JobIntent.FAIL);
    if (jobKey <= 0) {
      throw new IllegalArgumentException("jobKey must be positive");
    }
    if (retries < 0) {
      throw new IllegalArgumentException("retries must not be negative");
    }
    if (retryBackoff < 0) {
      throw new IllegalArgumentException("retryBackOff must not be negative");
    }
    request.setKey(jobKey);
    requestDto
        .setRetries(retries)
        .setErrorMessage(errorMessage)
        .setRetryBackoff(retryBackoff)
        .setVariablesMap(variables);
  }

  @Override
  public BufferWriter getRequestWriter() {
    return requestDto;
  }

  public BrokerFailJobRequest setWorkerSession(String workerId, long workerLeaseToken) {
    requestDto.setWorkerId(workerId).setWorkerLeaseToken(workerLeaseToken);
    return this;
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
