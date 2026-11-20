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

import io.taskite.protocol.impl.record.value.broker.JobBatchRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobBatchIntent;
import java.util.List;
import org.agrona.DirectBuffer;

public final class BrokerActivateJobsRequest extends BrokerExecuteCommand<JobBatchRecord> {
  private final JobBatchRecord requestDto = new JobBatchRecord();

  public BrokerActivateJobsRequest(String type) {
    super(ValueType.JOB_BATCH, JobBatchIntent.ACTIVATE);
    requestDto.setType(type);
  }

  public BrokerActivateJobsRequest setWorker(String worker) {
    requestDto.setWorker(worker);
    return this;
  }

  public BrokerActivateJobsRequest setWorkerId(String value) {
    requestDto.setWorkerId(value);
    return this;
  }

  public BrokerActivateJobsRequest setWorkerLeaseToken(long value) {
    requestDto.setWorkerLeaseToken(value);
    return this;
  }

  public BrokerActivateJobsRequest setTimeout(long timeout) {
    requestDto.setTimeout(timeout);
    return this;
  }

  public BrokerActivateJobsRequest setMaxJobsToActivate(int count) {
    requestDto.setMaxJobsToActivate(count);
    return this;
  }

  public BrokerActivateJobsRequest setVariables(List<String> variables) {
    requestDto.setFetchVariables(variables);
    return this;
  }

  @Override
  public void setTenantId(String tenantId) {
    requestDto.setTenantId(tenantId);
  }

  @Override
  public JobBatchRecord getRequestWriter() {
    return requestDto;
  }

  @Override
  protected JobBatchRecord toResponseDto(DirectBuffer buffer) {
    final var response = new JobBatchRecord();
    response.wrap(buffer);
    return response;
  }
}
