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

/** Creates a job without automatically retrying an uncertain transport result. */
public final class BrokerCreateJobRequest extends BrokerExecuteCommand<JobRecord> {
  private final JobRecord value;

  public BrokerCreateJobRequest(
      final String clusterId, final int partitionId, final JobRecord value) {
    super(ValueType.JOB, JobIntent.CREATE);
    this.value = value;
    setClusterId(clusterId);
    if (partitionId > 0) {
      setPartitionId(partitionId);
    }
  }

  @Override
  public boolean allowsTransportRetry() {
    return false;
  }

  @Override
  public void setTenantId(final String tenantId) {
    value.setTenantId(tenantId);
  }

  @Override
  public BufferWriter getRequestWriter() {
    return value;
  }

  @Override
  protected JobRecord toResponseDto(final DirectBuffer buffer) {
    final var response = new JobRecord();
    response.wrap(buffer);
    return response;
  }
}
