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
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.streamplatform.api.state.TypedEventApplier;
import io.taskite.topology.PartitionStatus;
import java.util.LinkedHashMap;
import java.util.Optional;

/** One event transaction replaces the pending owner, actual membership and durable lineage. */
public final class PartitionChangeReplacedApplier
    implements TypedEventApplier<BrokerPartitionChangeIntent, PartitionChangeRecord> {
  private final MutableProcessingState state;

  public PartitionChangeReplacedApplier(final MutableProcessingState state) {
    this.state = state;
  }

  @Override
  public void applyState(final long key, final PartitionChangeRecord value) {
    final var successor =
        BrokerPartitionChangeRecordMapper.toPlan(value, BrokerPartitionChangeStatus.QUEUED);
    final var previous =
        state
            .getBrokerTopologyChangeState()
            .changePlan(successor.clusterId(), successor.execution().predecessor())
            .orElse(null);
    if (previous == null
        || previous.status() != BrokerPartitionChangeStatus.SUPERSEDING
        || !state.getBrokerTopologyChangeState().ownsPendingPlan(previous)
        || !previous.execution().successor().equals(successor.changeId())
        || successor.partitionId() != previous.partitionId()
        || successor.execution().generation() <= previous.execution().generation()) {
      return;
    }
    final var evidence = successor.operations().getFirst().configuration();
    final var existing =
        state
            .getBrokerPartitionTopologyState()
            .partitionTopology(successor.clusterId(), successor.partitionId())
            .orElseThrow();
    final var priorities = new LinkedHashMap<String, Integer>();
    evidence
        .sourceMembers()
        .forEach(member -> priorities.put(member.brokerId(), member.priority()));
    final var actual =
        new PartitionTopology(
            existing.identity(),
            existing.placement(),
            new PartitionTopology.Membership(
                evidence.sourceMembers().stream()
                    .filter(member -> member.voter())
                    .map(member -> member.brokerId())
                    .toList(),
                evidence.sourceMembers().stream()
                    .filter(member -> !member.voter())
                    .map(member -> member.brokerId())
                    .toList(),
                priorities,
                evidence.expectedRecoveryEpoch()),
            existing.status() == PartitionStatus.CREATING
                ? PartitionStatus.CREATING
                : PartitionStatus.REBALANCING,
            existing.replicaStates(),
            existing.initializing());
    state
        .getBrokerTopologyChangeState()
        .putChangePlan(previous.withStatus(BrokerPartitionChangeStatus.SUPERSEDED));
    state.getBrokerTopologyChangeState().putChangePlan(successor);
    state.getBrokerTopologyChangeState().markPending(successor);
    state
        .getBrokerTopologyCoordinatorState()
        .removeTask(successor.clusterId(), successor.partitionId());
    state
        .getBrokerPartitionTopologyState()
        .putPartitionTopology(
            PartitionChangeTransitions.startPlan(
                Optional.of(actual), successor, existing.replicationFactor()));
    ClusterChangeOperationProgress.refresh(state, successor.clusterId(), successor.rootChangeId());
  }
}
