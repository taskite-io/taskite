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

import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.streamplatform.api.state.TypedEventApplier;

public final class PartitionChangeBlockedApplier
    implements TypedEventApplier<BrokerPartitionChangeIntent, PartitionChangeRecord> {
  private final MutableProcessingState state;

  public PartitionChangeBlockedApplier(final MutableProcessingState state) {
    this.state = state;
  }

  @Override
  public void applyState(final long key, final PartitionChangeRecord value) {
    state
        .getBrokerTopologyChangeState()
        .changePlan(value.getClusterId(), value.getChangeId())
        .filter(
            plan ->
                !plan.isTerminal() && state.getBrokerTopologyChangeState().ownsPendingPlan(plan))
        .filter(plan -> plan.nextOperationIndex() == value.getNextOperationIndex())
        .filter(plan -> plan.execution().generation() == value.getExecutionGeneration())
        .filter(plan -> plan.execution().successor().equals(value.getSuccessorChangeId()))
        .ifPresent(
            plan -> {
              final var execution = plan.execution();
              state
                  .getBrokerTopologyChangeState()
                  .putChangePlan(
                      plan.withExecution(
                          new ChangePlan.Execution(
                              execution.generation(),
                              execution.authorityIndex(),
                              execution.predecessor(),
                              execution.successor(),
                              plan.rootChangeId(),
                              value.getBlockedReason(),
                              value.getBlockedSinceEpochMillis())));
              ClusterChangeOperationProgress.refresh(state, plan.clusterId(), plan.rootChangeId());
            });
  }
}
