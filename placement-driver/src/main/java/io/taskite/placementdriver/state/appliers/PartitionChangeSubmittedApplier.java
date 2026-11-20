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
package io.taskite.placementdriver.state.appliers;

import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.PartitionChangeTransitions;
import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.streamplatform.api.state.TypedEventApplier;
import java.util.HashSet;
import java.util.Set;

public final class PartitionChangeSubmittedApplier
    implements TypedEventApplier<BrokerPartitionChangeIntent, PartitionChangeRecord> {
  private final MutableProcessingState state;

  public PartitionChangeSubmittedApplier(final MutableProcessingState state) {
    this.state = state;
  }

  @Override
  public void applyState(final long key, final PartitionChangeRecord value) {
    final int previousPartitionSize =
        state
            .getBrokerClusterConfigState()
            .findClusterConfig(value.getClusterId())
            .map(BrokerClusterConfig::partitionSize)
            .orElse(0);
    applyState(key, value, previousPartitionSize);
  }

  void applyState(
      final long key, final PartitionChangeRecord value, final int previousPartitionSize) {
    final ChangePlan plan =
        BrokerPartitionChangeRecordMapper.toPlan(value, BrokerPartitionChangeStatus.QUEUED);
    expandTopologyConfigForAdd(plan, previousPartitionSize);
    state.getBrokerTopologyChangeState().putChangePlan(plan);
    state.getBrokerTopologyChangeState().markPending(plan);
    // SUBMIT has reached durable plan execution, so its coordination reservation is released.
    state.getBrokerTopologyCoordinatorState().removeTask(plan.clusterId(), plan.partitionId());
    final int replicationFactor =
        state
            .getBrokerClusterConfigState()
            .findClusterConfig(plan.clusterId())
            .map(config -> config.replicationFactor())
            .orElseGet(() -> Math.max(1, desiredVoterCount(plan)));
    state
        .getBrokerPartitionTopologyState()
        .putPartitionTopology(
            PartitionChangeTransitions.startPlan(
                state
                    .getBrokerPartitionTopologyState()
                    .partitionTopology(plan.clusterId(), plan.partitionId()),
                plan,
                replicationFactor));
  }

  private void expandTopologyConfigForAdd(final ChangePlan plan, final int previousPartitionSize) {
    final boolean addPlan =
        plan.operations().stream()
            .anyMatch(operation -> operation.type() == TopologyOperationType.ADD);
    if (!addPlan) {
      return;
    }
    final var configuration =
        state.getBrokerClusterConfigState().findClusterConfig(plan.clusterId()).orElse(null);
    if (configuration != null && plan.partitionId() > configuration.partitionSize()) {
      state
          .getBrokerClusterConfigState()
          .putClusterConfig(
              plan.clusterId(),
              new BrokerClusterConfig(
                  configuration.clusterSize(),
                  plan.partitionId(),
                  configuration.replicationFactor(),
                  configuration.voterRoster(),
                  configuration.rosterEpoch()));
      state.getBrokerTopologyCoordinatorState().markCluster(plan.clusterId());
    }
    if (plan.partitionId() > previousPartitionSize) {
      redistributeToNewPartitions(
          plan.clusterId(), Math.max(0, previousPartitionSize), plan.partitionId());
    }
  }

  private void redistributeToNewPartitions(
      final String clusterId, final int oldPartitionSize, final int newPartitionSize) {
    final var distributionState = state.getCommandDistributionState();
    final var distributions = distributionState.distributions();
    final Set<Long> deletedDistributionKeys = new HashSet<>();
    for (final var distribution : distributions) {
      if (distribution.getValueType() == ValueType.JOB_SUBSCRIPTION_DELETION
          && distributionState.isTarget(distribution.getDistributionKey(), clusterId)) {
        deletedDistributionKeys.add(
            ((JobSubscriptionDeletionRecord) distribution.getValue()).getSubscriptionKey());
      }
    }
    for (final var distribution : distributions) {
      if (distribution.getValueType() != ValueType.JOB_SUBSCRIPTION
          || !distributionState.isTarget(distribution.getDistributionKey(), clusterId)
          || deletedDistributionKeys.contains(distribution.getDistributionKey())) {
        continue;
      }
      distributionState.putTarget(distribution.getDistributionKey(), clusterId, newPartitionSize);
      for (int partitionId = oldPartitionSize + 1; partitionId <= newPartitionSize; partitionId++) {
        distributionState.markPending(distribution.getDistributionKey(), clusterId, partitionId);
      }
    }
  }

  private static int desiredVoterCount(final ChangePlan plan) {
    return (int)
        plan.operations().stream()
            .filter(
                operation ->
                    operation.type() == TopologyOperationType.ADD
                        || operation.type() == TopologyOperationType.PROMOTE_LEARNER)
            .count();
  }
}
