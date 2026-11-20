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
import io.taskite.placementdriver.broker.cluster.BrokerPlanDependencies;
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

/** Stops dispatch before any replacement RPC can acquire authority. */
public final class PartitionSupersedeProcessor
    implements TypedRecordProcessor<PartitionChangeRecord> {
  private final Writers writers;
  private static final String CHANGE_NOT_CURRENT =
      "CHANGE_NOT_CURRENT: Expected to supersede topology change '%s' in cluster '%s' at operation index '%d' and generation '%d' with successor '%s', but no matching active owned plan was found or the successor id is blank";
  private static final String SUCCESSOR_RESERVED =
      "SUCCESSOR_ALREADY_RESERVED: Expected to supersede topology change '%s' in cluster '%s' with successor '%s', but successor '%s' is already reserved";
  private static final String DEPENDENCIES_VALID =
      "PLAN_DEPENDENCIES_VALID: Expected to supersede topology change '%s' in cluster '%s', but its dependencies are still valid and no replacement is needed";
  private final TypedRecordProcessorContext context;
  private final ProcessingState state;

  public PartitionSupersedeProcessor(final TypedRecordProcessorContext context) {
    this.context = context;
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
  }

  @Override
  public void processRecord(final TypedRecord<PartitionChangeRecord> record) {
    final var value = record.getValue();
    final var plan =
        state
            .getBrokerTopologyChangeState()
            .changePlan(value.getClusterId(), value.getChangeId())
            .orElse(null);
    if (plan == null
        || plan.isTerminal()
        || plan.status() == BrokerPartitionChangeStatus.QUEUED
        || !state.getBrokerTopologyChangeState().ownsPendingPlan(plan)
        || plan.nextOperationIndex() != value.getNextOperationIndex()
        || plan.execution().generation() != value.getExecutionGeneration()
        || value.getSuccessorChangeId().isBlank()) {
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
    if (plan.status() == BrokerPartitionChangeStatus.SUPERSEDING) {
      if (!plan.execution().successor().equals(value.getSuccessorChangeId())) {
        writers
            .rejection()
            .appendRejection(
                RejectionType.INVALID_STATE,
                SUCCESSOR_RESERVED.formatted(
                    value.getChangeId(),
                    value.getClusterId(),
                    value.getSuccessorChangeId(),
                    plan.execution().successor()));
      }
      return;
    }
    final PlanBlockedReason reason =
        BrokerPlanDependencies.invalidation(state, plan, context.getProcessingTime());
    if (reason == PlanBlockedReason.NONE) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_STATE,
              DEPENDENCIES_VALID.formatted(value.getChangeId(), value.getClusterId()));
      return;
    }
    final var execution = plan.execution();
    final var stopping =
        plan.withStatus(BrokerPartitionChangeStatus.SUPERSEDING)
            .withExecution(
                new ChangePlan.Execution(
                    execution.generation(),
                    execution.authorityIndex(),
                    execution.predecessor(),
                    value.getSuccessorChangeId(),
                    plan.rootChangeId(),
                    reason,
                    context.getProcessingTime()));
    writers
        .state()
        .appendFollowUpEvent(
            record.getKey(),
            BrokerPartitionChangeIntent.SUPERSEDING,
            BrokerPartitionChangeRecordMapper.fromPlan(stopping));
  }
}
