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

import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;

/** Records a retryable reason without ending the plan or changing its pending ownership. */
public final class PartitionBlockProcessor implements TypedRecordProcessor<PartitionChangeRecord> {
  private final Writers writers;
  private static final String UNKNOWN_REASON =
      "UNKNOWN_BLOCKED_REASON: Expected to update blocking state of topology change '%s' in cluster '%s', but blockedReason is unknown";
  private static final String CHANGE_NOT_CURRENT =
      "CHANGE_NOT_CURRENT: Expected to update blocking state of topology change '%s' in cluster '%s' at operation index '%d', generation '%d' and successor '%s', but no matching active owned plan was found";
  private final TypedRecordProcessorContext context;
  private final ProcessingState state;

  public PartitionBlockProcessor(final TypedRecordProcessorContext context) {
    this.context = context;
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
  }

  @Override
  public void processRecord(final TypedRecord<PartitionChangeRecord> record) {
    final var value = record.getValue();
    if (value.getBlockedReason() == null) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_ARGUMENT,
              UNKNOWN_REASON.formatted(value.getChangeId(), value.getClusterId()));
      return;
    }
    final var plan =
        state
            .getBrokerTopologyChangeState()
            .changePlan(value.getClusterId(), value.getChangeId())
            .orElse(null);
    if (plan == null
        || plan.isTerminal()
        || !state.getBrokerTopologyChangeState().ownsPendingPlan(plan)
        || plan.nextOperationIndex() != value.getNextOperationIndex()
        || plan.execution().generation() != value.getExecutionGeneration()
        || !plan.execution().successor().equals(value.getSuccessorChangeId())) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_STATE,
              CHANGE_NOT_CURRENT.formatted(
                  value.getChangeId(),
                  value.getClusterId(),
                  value.getNextOperationIndex(),
                  value.getExecutionGeneration(),
                  value.getSuccessorChangeId()));
      return;
    }
    if (plan.execution().blockedReason() == value.getBlockedReason()) {
      return;
    }
    final var execution = plan.execution();
    writers
        .state()
        .appendFollowUpEvent(
            record.getKey(),
            BrokerPartitionChangeIntent.BLOCKED,
            BrokerPartitionChangeRecordMapper.fromPlan(
                plan.withExecution(
                    new ChangePlan.Execution(
                        execution.generation(),
                        execution.authorityIndex(),
                        execution.predecessor(),
                        execution.successor(),
                        plan.rootChangeId(),
                        value.getBlockedReason(),
                        value.getBlockedReason() == PlanBlockedReason.NONE
                            ? 0
                            : context.getProcessingTime()))));
  }
}
