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
import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;
import java.util.HashSet;

/** Accepts only a complete successor based on a newly committed Raft authority certificate. */
public final class PartitionReplaceProcessor
    implements TypedRecordProcessor<PartitionChangeRecord> {
  private final Writers writers;
  private static final String INVALID_PLAN =
      "Expected to replace topology change with successor '%s' in cluster '%s', but decoding the successor plan failed: %s";
  private static final String INVALID_AUTHORITY =
      "INVALID_SUCCESSOR_AUTHORITY: Expected to install successor '%s' in cluster '%s' for partition '%d' after change '%s' with generation '%d' and authority index '%d', but its authority, lineage, operations or predecessor state is invalid";
  private static final String INCOMPLETE_CONFIGURATION =
      "INCOMPLETE_SOURCE_CONFIGURATION: Expected to install successor '%s' in cluster '%s' for partition '%d', but cluster configuration, frozen source configuration or positive membership epoch is missing";
  private static final String INVALID_CHAIN =
      "INVALID_SUCCESSOR_CONFIGURATION_CHAIN: Expected operation '%s' of successor '%s' in cluster '%s' to preserve a contiguous frozen configuration chain and recovery epoch '%d', but its type, identity or configuration is invalid";
  private static final String NO_QUORUM =
      "NO_QUORUM: Expected to install operation '%s' of successor '%s' in cluster '%s', but its source or target voters have no quorum among available brokers '%s'";
  private static final String INSUFFICIENT_CAPACITY =
      "INSUFFICIENT_CAPACITY: Expected to install successor '%s' in cluster '%s' with replicationFactor '%d', but its target members are unavailable or the voter count differs (available brokers '%s')";
  private final ProcessingState state;

  public PartitionReplaceProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
  }

  @Override
  public void processRecord(final TypedRecord<PartitionChangeRecord> record) {
    final ChangePlan successor;
    try {
      successor =
          BrokerPartitionChangeRecordMapper.toPlan(
              record.getValue(), BrokerPartitionChangeStatus.QUEUED);
    } catch (final IllegalArgumentException error) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_ARGUMENT,
              INVALID_PLAN.formatted(
                  record.getValue().getChangeId(),
                  record.getValue().getClusterId(),
                  error.getMessage()));
      return;
    }
    final var previous =
        state
            .getBrokerTopologyChangeState()
            .changePlan(successor.clusterId(), successor.execution().predecessor())
            .orElse(null);
    if (previous == null
        || previous.status() != BrokerPartitionChangeStatus.SUPERSEDING
        || !state.getBrokerTopologyChangeState().ownsPendingPlan(previous)
        || previous.partitionId() != successor.partitionId()
        || !previous.execution().successor().equals(successor.changeId())
        || !previous.rootChangeId().equals(successor.rootChangeId())
        || successor.execution().generation() <= previous.execution().generation()
        || successor.execution().authorityIndex() <= 0
        || successor.nextOperationIndex() != 0
        || successor.operations().isEmpty()
        || state
            .getBrokerTopologyChangeState()
            .changePlan(successor.clusterId(), successor.changeId())
            .isPresent()) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_STATE,
              INVALID_AUTHORITY.formatted(
                  successor.changeId(),
                  successor.clusterId(),
                  successor.partitionId(),
                  successor.execution().predecessor(),
                  successor.execution().generation(),
                  successor.execution().authorityIndex()));
      return;
    }
    final var available = BrokerPlanDependencies.available(state, successor.clusterId());
    final var configuration =
        state.getBrokerClusterConfigState().findClusterConfig(successor.clusterId()).orElse(null);
    final var first = successor.operations().getFirst().configuration();
    if (configuration == null || first == null || first.expectedMembershipEpoch() <= 0) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_STATE,
              INCOMPLETE_CONFIGURATION.formatted(
                  successor.changeId(), successor.clusterId(), successor.partitionId()));
      return;
    }
    final var ids = new HashSet<String>();
    String precedingDigest = first.sourceDigest(successor.clusterId());
    for (final var operation : successor.operations()) {
      final var step = operation.configuration();
      if (step == null
          || operation.type() == TopologyOperationType.RESET_PEERS
          || operation.type() == TopologyOperationType.ADD
          || !ids.add(operation.opId())
          || !step.sourceDigest(successor.clusterId()).equals(precedingDigest)
          || step.expectedRecoveryEpoch() != first.expectedRecoveryEpoch()
          || step.targetRecoveryEpoch() != first.expectedRecoveryEpoch()) {
        writers
            .rejection()
            .appendRejection(
                RejectionType.INVALID_ARGUMENT,
                INVALID_CHAIN.formatted(
                    operation.opId(),
                    successor.changeId(),
                    successor.clusterId(),
                    first.expectedRecoveryEpoch()));
        return;
      }
      final var sourceVoters =
          step.sourceMembers().stream().filter(member -> member.voter()).toList();
      final var targetVoters =
          step.targetMembers().stream().filter(member -> member.voter()).toList();
      if (sourceVoters.isEmpty()
          || targetVoters.isEmpty()
          || sourceVoters.stream().filter(member -> available.contains(member.brokerId())).count()
              < sourceVoters.size() / 2 + 1
          || targetVoters.stream().filter(member -> available.contains(member.brokerId())).count()
              < targetVoters.size() / 2 + 1) {
        writers
            .rejection()
            .appendRejection(
                RejectionType.INVALID_STATE,
                NO_QUORUM.formatted(
                    operation.opId(), successor.changeId(), successor.clusterId(), available));
        return;
      }
      precedingDigest = step.targetDigest(successor.clusterId());
    }
    final var target = successor.operations().getLast().configuration().targetMembers();
    if (target.stream().anyMatch(member -> !available.contains(member.brokerId()))
        || target.stream().filter(member -> member.voter()).count()
            != configuration.replicationFactor()) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_STATE,
              INSUFFICIENT_CAPACITY.formatted(
                  successor.changeId(),
                  successor.clusterId(),
                  configuration.replicationFactor(),
                  available));
      return;
    }
    writers
        .state()
        .appendFollowUpEvent(
            record.getKey(),
            BrokerPartitionChangeIntent.REPLACED,
            BrokerPartitionChangeRecordMapper.fromPlan(successor));
  }
}
