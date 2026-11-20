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
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.topology.PartitionStatus;

final class PartitionChangeOperationUpdater {
  private final MutableProcessingState state;

  PartitionChangeOperationUpdater(final MutableProcessingState state) {
    this.state = state;
  }

  void update(final PartitionChangeRecord value) {
    final ChangePlan plan =
        state
            .getBrokerTopologyChangeState()
            .changePlan(value.getClusterId(), value.getChangeId())
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Expected to apply topology operation, but change plan does not exist: "
                            + value.getChangeId()));
    if (plan.isTerminal()
        || plan.status() == BrokerPartitionChangeStatus.QUEUED
        || plan.status() == BrokerPartitionChangeStatus.SUPERSEDING
        || plan.execution().generation() != value.getExecutionGeneration()
        || !state.getBrokerTopologyChangeState().ownsPendingPlan(plan)
        || plan.nextOperationIndex() != value.getNextOperationIndex()
        || !plan.hasMoreOperations()
        || value.getOperations().size() <= value.getNextOperationIndex()
        || !plan.nextOperation()
            .opId()
            .equals(value.getOperations().get(value.getNextOperationIndex()).opId())) {
      return;
    }
    final var topology =
        state
            .getBrokerPartitionTopologyState()
            .partitionTopology(plan.clusterId(), plan.partitionId())
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Expected to apply topology operation, but partition topology does not exist: "
                            + plan.partitionId()));
    final ChangePlan advanced = plan.advanced();
    final boolean redistributionComplete =
        !advanced.isTerminal()
            || (topology.status() != PartitionStatus.CREATING && !topology.initializing())
            || PartitionActivation.redistributionComplete(
                state, plan.clusterId(), plan.partitionId());
    final ChangePlan updated =
        advanced.isTerminal() && !redistributionComplete
            ? advanced.withStatus(BrokerPartitionChangeStatus.IN_PROGRESS)
            : advanced;
    final var applied =
        PartitionChangeTransitions.afterOperation(topology, plan, redistributionComplete);
    state
        .getBrokerPartitionTopologyState()
        .putPartitionTopology(
            updated.isTerminal() && plan.purpose() == ChangePlan.Purpose.BOOTSTRAP
                ? PartitionActivation.finishBootstrap(state, applied)
                : applied);
    state
        .getBrokerTopologyChangeState()
        .markOperationApplied(plan.clusterId(), plan.nextOperation().opId());
    state.getBrokerTopologyChangeState().putChangePlan(updated);
    if (updated.isTerminal()) {
      state.getBrokerTopologyChangeState().removePending(plan.clusterId(), plan.partitionId());
      state.getBrokerTopologyCoordinatorState().removeTask(plan.clusterId(), plan.partitionId());
      state.getBrokerTopologyCoordinatorState().markPartition(plan.clusterId(), plan.partitionId());
      BrokerRemovalFinalizer.finalizeRemovals(state, plan.clusterId());
    }
    ClusterChangeOperationProgress.refresh(state, plan.clusterId(), plan.rootChangeId());
  }
}
