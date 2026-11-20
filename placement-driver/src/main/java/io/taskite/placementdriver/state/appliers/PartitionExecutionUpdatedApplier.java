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

import io.taskite.placementdriver.state.immutable.TopologyMembershipChangeState;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionExecutionRecord;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.streamplatform.api.state.TypedEventApplier;

public final class PartitionExecutionUpdatedApplier
    implements TypedEventApplier<BrokerPartitionChangeIntent, PartitionExecutionRecord> {
  private final MutableProcessingState state;

  public PartitionExecutionUpdatedApplier(final MutableProcessingState state) {
    this.state = state;
  }

  @Override
  public void applyState(final long key, final PartitionExecutionRecord value) {
    final var schedule = value.getSchedule();
    final var identity = schedule.plan();
    state
        .getBrokerTopologyChangeState()
        .changePlan(identity.clusterId(), identity.changeId())
        .filter(state.getBrokerTopologyChangeState()::ownsPendingPlan)
        .filter(
            plan ->
                !plan.isTerminal() && TopologyMembershipChangeState.identity(plan).equals(identity))
        .ifPresent(ignored -> state.getTopologyMembershipChangeState().putPartition(schedule));
  }
}
