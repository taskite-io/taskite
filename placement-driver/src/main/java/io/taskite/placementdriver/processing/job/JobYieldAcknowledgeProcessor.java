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
package io.taskite.placementdriver.processing.job;

import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.protocol.record.value.broker.JobYieldStatus;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;

public final class JobYieldAcknowledgeProcessor implements TypedRecordProcessor<JobYieldRecord> {
  private final Writers writers;
  private static final String INVALID_ACK =
      "INVALID_YIELD_ACK: Expected to acknowledge job yield '%d' in cluster '%s' for tenant '%s', worker '%s', lease token '%d' and partition '%d', but status '%s' is not COMPLETED or the plan identity, reason, partition or progress counters are invalid (reason '%s', attempts '%d', released jobs '%d')";
  private final ProcessingState state;

  public JobYieldAcknowledgeProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
  }

  @Override
  public void processRecord(final TypedRecord<JobYieldRecord> record) {
    final var progress = record.getValue();
    if (progress.getStatus() != JobYieldStatus.COMPLETED
        || !JobYieldProgressValidation.valid(state, progress)) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_ARGUMENT,
              INVALID_ACK.formatted(
                  progress.getYieldId(),
                  progress.getClusterId(),
                  progress.getTenantId(),
                  progress.getWorkerId(),
                  progress.getWorkerLeaseToken(),
                  progress.getPartitionId(),
                  progress.getStatus(),
                  progress.getReason(),
                  progress.getAttempts(),
                  progress.getReleasedJobCount()));
      return;
    }
    final var target =
        state
            .getJobYieldState()
            .plan(progress.getClusterId(), progress.getYieldId())
            .orElseThrow()
            .partitions()
            .stream()
            .filter(partition -> partition.partitionId() == progress.getPartitionId())
            .findFirst()
            .orElseThrow();
    if (target.status() == JobYieldStatus.COMPLETED
        || progress.getAttempts() < target.attempts()
        || (progress.getDispatchAttemptId() > 0 && progress.getAttempts() != target.attempts())) {
      return;
    }
    writers
        .state()
        .appendFollowUpEvent(
            progress.getYieldId(),
            JobYieldIntent.ACKNOWLEDGED,
            progress.setTimestampEpochMillis(record.getTimestamp()));
  }
}
