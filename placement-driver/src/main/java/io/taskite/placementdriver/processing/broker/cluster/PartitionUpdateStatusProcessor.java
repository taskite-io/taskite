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
package io.taskite.placementdriver.processing.broker.cluster;

import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;

public final class PartitionUpdateStatusProcessor
    implements TypedRecordProcessor<PartitionChangeRecord> {
  private final Writers writers;
  private static final String PLAN_NOT_FOUND =
      "Expected to update status of topology change '%s' in cluster '%s', but no plan was found";
  private static final String UNKNOWN_STATUS =
      "Expected to update status of topology change '%s' in cluster '%s', but status '%s' is unknown";
  private static final String INVALID_STATUS =
      "Expected to update status of topology change '%s' in cluster '%s' to WAITING_READY or WAITING_LEADER_CONVERGE, but received '%s'";
  private static final String STALE_PROGRESS =
      "Expected to update status of topology change '%s' in cluster '%s' at operation index '%d' and generation '%d', but its plan is inactive, owned by another plan or has different progress (status '%s', current index '%d', current generation '%d')";
  private final ProcessingState state;

  public PartitionUpdateStatusProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
  }

  @Override
  public void processRecord(final TypedRecord<PartitionChangeRecord> record) {
    final PartitionChangeRecord value = record.getValue();
    final var existing =
        state.getBrokerTopologyChangeState().changePlan(value.getClusterId(), value.getChangeId());
    if (existing.isEmpty()) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.NOT_FOUND,
              PLAN_NOT_FOUND.formatted(value.getChangeId(), value.getClusterId()));
      return;
    }
    final BrokerPartitionChangeStatus requested;
    try {
      requested = BrokerPartitionChangeStatus.valueOf(value.getStatus().name());
    } catch (final IllegalArgumentException error) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_ARGUMENT,
              UNKNOWN_STATUS.formatted(
                  value.getChangeId(), value.getClusterId(), value.getStatus()));
      return;
    }
    if (requested != BrokerPartitionChangeStatus.WAITING_READY
        && requested != BrokerPartitionChangeStatus.WAITING_LEADER_CONVERGE) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_ARGUMENT,
              INVALID_STATUS.formatted(value.getChangeId(), value.getClusterId(), requested));
      return;
    }
    if (existing.get().nextOperationIndex() != value.getNextOperationIndex()
        || existing.get().isTerminal()
        || existing.get().status() == BrokerPartitionChangeStatus.QUEUED
        || existing.get().status() == BrokerPartitionChangeStatus.SUPERSEDING
        || existing.get().execution().generation() != value.getExecutionGeneration()
        || !state.getBrokerTopologyChangeState().ownsPendingPlan(existing.get())) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_STATE,
              STALE_PROGRESS.formatted(
                  value.getChangeId(),
                  value.getClusterId(),
                  value.getNextOperationIndex(),
                  value.getExecutionGeneration(),
                  existing.get().status(),
                  existing.get().nextOperationIndex(),
                  existing.get().execution().generation()));
      return;
    }
    writers
        .state()
        .appendFollowUpEvent(
            record.getKey(),
            BrokerPartitionChangeIntent.STATUS_UPDATED,
            BrokerPartitionChangeRecordMapper.fromPlan(existing.get().withStatus(requested)));
  }
}
