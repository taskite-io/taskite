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
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.streamplatform.api.state.TypedEventApplier;
import io.taskite.topology.PartitionStatus;

public final class CommandDistributionAcknowledgedApplier
    implements TypedEventApplier<CommandDistributionIntent, CommandDistributionRecord<?>> {
  private final MutableProcessingState state;

  public CommandDistributionAcknowledgedApplier(final MutableProcessingState state) {
    this.state = state;
  }

  @Override
  public void applyState(final long key, final CommandDistributionRecord<?> value) {
    if (state
        .getCommandDistributionState()
        .distribution(value.getDistributionKey())
        .filter(distribution -> distribution.getGeneration() == value.getGeneration())
        .isEmpty()) {
      return;
    }
    state
        .getCommandDistributionState()
        .acknowledge(value.getDistributionKey(), value.getClusterId(), value.getPartitionId());
    final var activated =
        state
            .getBrokerPartitionTopologyState()
            .partitionTopology(value.getClusterId(), value.getPartitionId())
            .filter(
                topology ->
                    topology.status() == PartitionStatus.CREATING || topology.initializing())
            .filter(
                topology -> topologyChangeComplete(topology.clusterId(), topology.partitionId()))
            .filter(
                topology ->
                    PartitionActivation.redistributionComplete(
                        state, topology.clusterId(), topology.partitionId()))
            .map(
                topology ->
                    state
                            .getBrokerTopologyChangeState()
                            .pendingChangePlans(topology.clusterId())
                            .stream()
                            .anyMatch(
                                plan ->
                                    plan.partitionId() == topology.partitionId()
                                        && plan.purpose() == ChangePlan.Purpose.BOOTSTRAP
                                        && jobSubscriptionDistributionInProgress(plan))
                        ? PartitionActivation.finishBootstrap(state, topology)
                        : PartitionChangeTransitions.markActive(topology));
    activated.ifPresent(state.getBrokerPartitionTopologyState()::putPartitionTopology);
    activated.ifPresent(
        topology -> {
          completeJobSubscriptionDistribution(topology.clusterId(), topology.partitionId());
          state
              .getBrokerTopologyCoordinatorState()
              .markPartition(topology.clusterId(), topology.partitionId());
        });
  }

  private void completeJobSubscriptionDistribution(final String clusterId, final int partitionId) {
    state.getBrokerTopologyChangeState().pendingChangePlans(clusterId).stream()
        .filter(plan -> plan.partitionId() == partitionId)
        .filter(CommandDistributionAcknowledgedApplier::jobSubscriptionDistributionInProgress)
        .findFirst()
        .ifPresent(
            plan -> {
              state
                  .getBrokerTopologyChangeState()
                  .putChangePlan(plan.withStatus(BrokerPartitionChangeStatus.COMPLETED));
              state.getBrokerTopologyChangeState().removePending(clusterId, partitionId);
              state.getBrokerTopologyCoordinatorState().removeTask(clusterId, partitionId);
              ClusterChangeOperationProgress.refresh(state, clusterId, plan.rootChangeId());
            });
  }

  private boolean topologyChangeComplete(final String clusterId, final int partitionId) {
    return state.getBrokerTopologyChangeState().pendingChangePlans(clusterId).stream()
        .filter(plan -> plan.partitionId() == partitionId)
        .allMatch(CommandDistributionAcknowledgedApplier::jobSubscriptionDistributionInProgress);
  }

  private static boolean jobSubscriptionDistributionInProgress(final ChangePlan plan) {
    return plan.status() == BrokerPartitionChangeStatus.IN_PROGRESS && !plan.hasMoreOperations();
  }
}
