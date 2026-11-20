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
package io.taskite.placementdriver.broker.request;

import io.taskite.broker.client.request.BrokerExecuteCommand;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.protocol.record.value.broker.JobYieldStatus;
import org.agrona.DirectBuffer;

public final class BrokerYieldJobsRequest extends BrokerExecuteCommand<JobYieldRecord> {
  private final JobYieldRecord value;

  public BrokerYieldJobsRequest(final JobYieldRecord request) {
    super(ValueType.JOB_YIELD, JobYieldIntent.FENCE_SESSION);
    value = sessionIdentity(request);
    setClusterId(value.getClusterId());
    setPartitionId(value.getPartitionId());
  }

  // PD delivery attempts and ACK counts must never overwrite Broker progress.
  static JobYieldRecord sessionIdentity(final JobYieldRecord request) {
    return new JobYieldRecord()
        .setYieldId(request.getYieldId())
        .setClusterId(request.getClusterId())
        .setPartitionId(request.getPartitionId())
        .setTenantId(request.getTenantId())
        .setWorkerId(request.getWorkerId())
        .setWorkerLeaseToken(request.getWorkerLeaseToken())
        .setReason(request.getReason());
  }

  @Override
  public JobYieldRecord getRequestWriter() {
    return value;
  }

  @Override
  protected JobYieldRecord toResponseDto(final DirectBuffer buffer) {
    final var result = new JobYieldRecord();
    result.wrap(buffer);
    if (response.getRecordType() != RecordType.EVENT
        || response.getIntent() != JobYieldIntent.SESSION_FENCED
        || !value.sameIdentity(result)
        || result.getReleasedJobCount() < 0
        || (result.getStatus() != JobYieldStatus.PENDING
            && result.getStatus() != JobYieldStatus.COMPLETED)) {
      throw new IllegalArgumentException("Invalid YieldJobs response");
    }
    return result;
  }

  @Override
  public boolean allowsTransportRetry() {
    return false;
  }
}
