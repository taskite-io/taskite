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
import org.agrona.DirectBuffer;

public final class BrokerSyncWorkerSessionsRequest extends BrokerExecuteCommand<JobYieldRecord> {
  private final JobYieldRecord value;

  public BrokerSyncWorkerSessionsRequest(final JobYieldRecord request) {
    super(ValueType.JOB_YIELD, JobYieldIntent.SYNC_SESSIONS);
    value =
        new JobYieldRecord()
            .setClusterId(request.getClusterId())
            .setPartitionId(request.getPartitionId())
            .setPreviousSyncCursor(request.getPreviousSyncCursor())
            .setSyncCursor(request.getSyncCursor())
            .setSyncComplete(request.isSyncComplete())
            .setSessions(
                request.getSessions().stream()
                    .map(BrokerYieldJobsRequest::sessionIdentity)
                    .toList());
    setClusterId(value.getClusterId());
    setPartitionId(value.getPartitionId());
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
        || response.getIntent() != JobYieldIntent.SESSIONS_SYNCED
        || !getClusterId().equals(result.getClusterId())
        || getPartitionId() != result.getPartitionId()
        || result.getSyncCursor() < 0) {
      throw new IllegalArgumentException("Invalid worker session synchronization response");
    }
    return result;
  }

  @Override
  public boolean allowsTransportRetry() {
    return false;
  }
}
