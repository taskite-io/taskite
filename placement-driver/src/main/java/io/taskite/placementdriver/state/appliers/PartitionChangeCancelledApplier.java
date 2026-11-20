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
import io.taskite.placementdriver.broker.cluster.PartitionChangeTransitions;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.streamplatform.api.state.TypedEventApplier;

public final class PartitionChangeCancelledApplier
    implements TypedEventApplier<BrokerPartitionChangeIntent, PartitionChangeRecord> {
  private final MutableProcessingState state;

  public PartitionChangeCancelledApplier(final MutableProcessingState state) {
    this.state = state;
  }

  @Override
  public void applyState(final long key, final PartitionChangeRecord value) {
    state
        .getBrokerTopologyChangeState()
        .changePlan(value.getClusterId(), value.getChangeId())
        .filter(plan -> !plan.isTerminal())
        .filter(plan -> plan.execution().predecessor().isBlank())
        .filter(plan -> plan.status() != BrokerPartitionChangeStatus.SUPERSEDING)
        .filter(plan -> plan.execution().generation() == value.getExecutionGeneration())
        .filter(state.getBrokerTopologyChangeState()::ownsPendingPlan)
        .filter(plan -> plan.nextOperationIndex() == value.getNextOperationIndex())
        .ifPresent(
            plan -> {
              state
                  .getBrokerTopologyChangeState()
                  .putChangePlan(plan.withStatus(BrokerPartitionChangeStatus.FAILED));
              state
                  .getBrokerTopologyChangeState()
                  .removePending(plan.clusterId(), plan.partitionId());
              state
                  .getBrokerTopologyCoordinatorState()
                  .removeTask(plan.clusterId(), plan.partitionId());
              state
                  .getBrokerTopologyCoordinatorState()
                  .markPartition(plan.clusterId(), plan.partitionId());
              state
                  .getBrokerPartitionTopologyState()
                  .partitionTopology(plan.clusterId(), plan.partitionId())
                  .map(PartitionChangeTransitions::cancel)
                  .ifPresent(state.getBrokerPartitionTopologyState()::putPartitionTopology);
              ClusterChangeOperationProgress.refresh(state, plan.clusterId(), plan.changeId());
            });
  }
}
