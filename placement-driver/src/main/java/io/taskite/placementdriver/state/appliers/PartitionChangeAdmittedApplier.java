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
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.streamplatform.api.state.TypedEventApplier;
import java.util.Optional;

/** Installs a reservation and its execution clock in the same event transaction. */
public final class PartitionChangeAdmittedApplier
    implements TypedEventApplier<BrokerPartitionChangeIntent, PartitionChangeRecord> {
  private final MutableProcessingState state;

  public PartitionChangeAdmittedApplier(final MutableProcessingState state) {
    this.state = state;
  }

  @Override
  public void applyState(final long key, final PartitionChangeRecord value) {
    final var previous =
        state
            .getBrokerTopologyChangeState()
            .changePlan(value.getClusterId(), value.getChangeId())
            .orElse(null);
    if (previous == null
        || previous.status() != BrokerPartitionChangeStatus.QUEUED
        || !state.getBrokerTopologyChangeState().ownsPendingPlan(previous)
        || previous.execution().generation() != value.getExecutionGeneration()
        || previous.nextOperationIndex() != value.getNextOperationIndex()) {
      return;
    }
    final var admitted =
        BrokerPartitionChangeRecordMapper.toPlan(value, BrokerPartitionChangeStatus.PENDING);
    state.getBrokerTopologyChangeState().putChangePlan(admitted);
    state
        .getBrokerPartitionTopologyState()
        .partitionTopology(admitted.clusterId(), admitted.partitionId())
        .ifPresent(
            topology ->
                state
                    .getBrokerPartitionTopologyState()
                    .putPartitionTopology(
                        PartitionChangeTransitions.startPlan(
                            Optional.of(topology), admitted, topology.replicationFactor())));
  }
}
