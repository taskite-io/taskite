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
package io.taskite.placementdriver.processing.topology;

import io.taskite.placementdriver.broker.cluster.BrokerOperationEffectVerifier;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.Writers;

public final class TopologyPartitionChangeBehavior {
  private final Writers writers;
  private static final String PLAN_NOT_FOUND =
      "Expected to acknowledge topology change '%s' in cluster '%s', but no plan was found";
  private static final String CHANGE_NOT_CURRENT =
      "CHANGE_NOT_CURRENT: Expected to acknowledge topology change '%s' in cluster '%s' at generation '%d', but its plan is terminal, queued, superseding, owned by another plan or at a different generation (status '%s', current generation '%d')";
  private static final String OPERATION_MISMATCH =
      "Expected to acknowledge operation '%s' for topology change '%s' in cluster '%s' at operation index '%d', but the requested index '%d' or operation identity does not match";
  private static final String CONFIGURATION_NOT_APPLIED =
      "CONFIGURATION_NOT_APPLIED: Expected to acknowledge operation '%s' for topology change '%s' in cluster '%s' and partition '%d', but the partition is missing or has not applied the operation's configuration";
  private final ProcessingState state;
  private final KeyGenerator keyGenerator;

  public TopologyPartitionChangeBehavior(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
    keyGenerator = context.getKeyGenerator();
  }

  public void complete(
      final TypedRecord<PartitionChangeRecord> record,
      final TopologyOperationType operationType,
      final BrokerPartitionChangeIntent eventIntent) {
    final PartitionChangeRecord value = record.getValue();
    final var plan =
        state.getBrokerTopologyChangeState().changePlan(value.getClusterId(), value.getChangeId());
    if (plan.isEmpty()) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.NOT_FOUND,
              PLAN_NOT_FOUND.formatted(value.getChangeId(), value.getClusterId()));
      return;
    }
    if (plan.get().isTerminal()
        || plan.get().status() == BrokerPartitionChangeStatus.QUEUED
        || plan.get().status() == BrokerPartitionChangeStatus.SUPERSEDING
        || value.getExecutionGeneration() != plan.get().execution().generation()
        || !state.getBrokerTopologyChangeState().ownsPendingPlan(plan.get())) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_STATE,
              CHANGE_NOT_CURRENT.formatted(
                  value.getChangeId(),
                  value.getClusterId(),
                  value.getExecutionGeneration(),
                  plan.get().status(),
                  plan.get().execution().generation()));
      return;
    }
    if (!plan.get().hasMoreOperations()) {
      return;
    }
    if (value.getNextOperationIndex() != plan.get().nextOperationIndex()
        || plan.get().nextOperation().type() != operationType
        || value.getOperations().size() <= value.getNextOperationIndex()
        || !value
            .getOperations()
            .get(value.getNextOperationIndex())
            .opId()
            .equals(plan.get().nextOperation().opId())) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_STATE,
              OPERATION_MISMATCH.formatted(
                  plan.get().nextOperation().opId(),
                  plan.get().changeId(),
                  plan.get().clusterId(),
                  plan.get().nextOperationIndex(),
                  value.getNextOperationIndex()));
      return;
    }
    final var topology =
        state
            .getBrokerPartitionTopologyState()
            .partitionTopology(plan.get().clusterId(), plan.get().partitionId());
    if (topology.isEmpty()
        || !BrokerOperationEffectVerifier.applied(
            topology.get(),
            plan.get().nextOperation(),
            state.getBrokerRegistrationState().registrations(plan.get().clusterId()))) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_STATE,
              CONFIGURATION_NOT_APPLIED.formatted(
                  plan.get().nextOperation().opId(),
                  plan.get().changeId(),
                  plan.get().clusterId(),
                  plan.get().partitionId()));
      return;
    }
    writers
        .state()
        .appendFollowUpEvent(
            keyGenerator.nextKey(),
            eventIntent,
            BrokerPartitionChangeRecordMapper.fromPlan(plan.get()));
  }

  public static BrokerPartitionChangeIntent command(final TopologyOperationType type) {
    return switch (type) {
      case JOIN -> BrokerPartitionChangeIntent.JOIN;
      case LEAVE -> BrokerPartitionChangeIntent.LEAVE;
      case ADD -> BrokerPartitionChangeIntent.CREATE;
      case PROMOTE_LEARNER -> BrokerPartitionChangeIntent.PROMOTE;
      case RECONFIGURE_PRIORITY -> BrokerPartitionChangeIntent.CHANGE_PRIORITY;
      case TRANSFER_LEADER -> BrokerPartitionChangeIntent.TRANSFER_LEADER;
      case STEP_DOWN_IF_NOT_PRIMARY -> BrokerPartitionChangeIntent.STEP_DOWN_LEADER;
      case RESET_PEERS -> BrokerPartitionChangeIntent.RESET_PEERS;
    };
  }
}
