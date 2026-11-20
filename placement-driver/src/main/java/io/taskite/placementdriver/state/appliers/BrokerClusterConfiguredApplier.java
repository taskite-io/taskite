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

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeStatus;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.ClusterChangeOperation;
import io.taskite.placementdriver.broker.cluster.PartitionChangeTransitions;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.common.codec.ChangePlanRecordCodec;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.streamplatform.api.state.TypedEventApplier;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

public final class BrokerClusterConfiguredApplier
    implements TypedEventApplier<TopologyIntent, ClusterConfigRecord> {
  private final MutableProcessingState state;

  public BrokerClusterConfiguredApplier(final MutableProcessingState state) {
    this.state = state;
  }

  @Override
  public void applyState(final long key, final ClusterConfigRecord value) {
    final long timestamp = value.getRequestedAtEpochMillis();
    state.getBrokerClusterState().clusterKey(value.getClusterId());
    // Preserve the existing voter roster (T9) and its epoch across a sizing reconfigure; rebuilding
    // with the sizing-only ctor would silently drop it.
    final var existing =
        state.getBrokerClusterConfigState().findClusterConfig(value.getClusterId());
    final int previousPartitionSize =
        value.getSourcePartitionSize() > 0
            ? value.getSourcePartitionSize()
            : existing.map(BrokerClusterConfig::partitionSize).orElse(0);
    final var requestedRoster = value.getVoterRoster();
    requestedRoster.stream()
        .filter(
            brokerId ->
                !existing
                    .map(BrokerClusterConfig::voterRoster)
                    .orElse(List.of())
                    .contains(brokerId))
        .forEach(
            brokerId ->
                state
                    .getBrokerRegistrationState()
                    .includeInPlacement(value.getClusterId(), brokerId));
    state
        .getBrokerClusterConfigState()
        .putClusterConfig(
            value.getClusterId(),
            new BrokerClusterConfig(
                value.getClusterSize(),
                value.getPartitionSize(),
                value.getReplicationFactor(),
                requestedRoster.isEmpty()
                    ? existing.map(BrokerClusterConfig::voterRoster).orElse(List.of())
                    : requestedRoster,
                value.isRosterManaged()
                    ? BrokerClusterConfig.MANAGED_ROSTER_EPOCH
                    : requestedRoster.isEmpty()
                        ? existing.map(BrokerClusterConfig::rosterEpoch).orElse(0L)
                        : existing.map(BrokerClusterConfig::rosterEpoch).orElse(0L) + 1));
    value
        .getRemovedBrokers()
        .forEach(
            brokerId -> {
              state
                  .getBrokerRegistrationState()
                  .excludeFromPlacement(value.getClusterId(), brokerId);
              state
                  .getBrokerRegistrationState()
                  .markBrokerDraining(value.getClusterId(), brokerId, Instant.EPOCH);
            });
    final List<ChangePlan> plans =
        value.getEncodedChangePlans().stream()
            .map(encoded -> Base64.getDecoder().decode(encoded))
            .map(ChangePlanRecordCodec::fromBytes)
            .toList();
    final PartitionChangeSubmittedApplier planApplier = new PartitionChangeSubmittedApplier(state);
    for (final ChangePlan requested : plans) {
      final ChangePlan plan = replaceRecoveryOwner(requested);
      planApplier.applyState(
          key, BrokerPartitionChangeRecordMapper.fromPlan(plan), previousPartitionSize);
    }
    if (!value.getChangeId().isBlank()) {
      state
          .getBrokerTopologyChangeState()
          .putClusterChangeOperation(clusterChangeOperation(state, value, plans));
    }
    BrokerRemovalFinalizer.finalizeRemovals(state, value.getClusterId());
    state.getBrokerTopologyCoordinatorState().markCluster(value.getClusterId(), timestamp);
  }

  private ChangePlan replaceRecoveryOwner(final ChangePlan recovery) {
    if (!recovery.isQuorumRecovery()) {
      return recovery;
    }
    final var previous =
        state.getBrokerTopologyChangeState().pendingChangePlans(recovery.clusterId()).stream()
            .filter(plan -> plan.partitionId() == recovery.partitionId())
            .findFirst()
            .orElse(null);
    if (previous == null) {
      return recovery;
    }
    if (previous.isQuorumRecovery()) {
      throw new IllegalStateException(
          "Quorum recovery already owns partition: " + previous.changeId());
    }
    // CONFIGURED is one event transaction: retire the old authority and install reciprocal lineage
    // before replacing the pending owner. An old RPC completion can no longer advance membership.
    final var authority = previous.execution();
    state
        .getBrokerTopologyChangeState()
        .putChangePlan(
            previous
                .withStatus(BrokerPartitionChangeStatus.SUPERSEDED)
                .withExecution(
                    new ChangePlan.Execution(
                        authority.generation(),
                        authority.authorityIndex(),
                        authority.predecessor(),
                        recovery.changeId(),
                        previous.rootChangeId(),
                        authority.blockedReason(),
                        authority.blockedSinceEpochMillis())));
    return recovery.withExecution(
        new ChangePlan.Execution(
            Math.max(recovery.execution().generation(), authority.generation() + 1),
            recovery.execution().authorityIndex(),
            previous.changeId(),
            "",
            previous.rootChangeId(),
            PlanBlockedReason.NONE,
            0));
  }

  private static ClusterChangeOperation clusterChangeOperation(
      final MutableProcessingState state,
      final ClusterConfigRecord value,
      final List<ChangePlan> plans) {
    final Map<Integer, ChangePlan> plansByPartition = new LinkedHashMap<>();
    plans.forEach(plan -> plansByPartition.put(plan.partitionId(), plan));
    final List<ClusterChangeOperation.ExpectedPartition> expectedTopology = new ArrayList<>();
    for (final PartitionTopology topology :
        state.getBrokerPartitionTopologyState().partitionTopologies(value.getClusterId())) {
      PartitionTopology expected = topology;
      final ChangePlan plan = plansByPartition.get(topology.partitionId());
      if (plan != null) {
        for (int index = plan.nextOperationIndex(); index < plan.operations().size(); index++) {
          expected =
              PartitionChangeTransitions.targetMembership(expected, plan.operations().get(index));
        }
        expected = PartitionChangeTransitions.markActive(expected);
      }
      expectedTopology.add(
          new ClusterChangeOperation.ExpectedPartition(
              expected.partitionId(),
              expected.desiredVoters(),
              expected.currentVoters(),
              expected.learners(),
              expected.desiredPrimary(),
              expected.priorities()));
    }
    final LinkedHashSet<String> selectedBrokerIds = new LinkedHashSet<>(value.getVoterRoster());
    selectedBrokerIds.addAll(value.getRemovedBrokers());
    return new ClusterChangeOperation(
        value.getClusterId(),
        value.getChangeId(),
        value.getSourceTopologyVersion(),
        value.getClusterSize(),
        value.getPartitionSize(),
        value.getReplicationFactor(),
        value.getVoterRoster(),
        value.getRemovedBrokers(),
        List.copyOf(selectedBrokerIds),
        expectedTopology,
        plans.stream().map(ChangePlan::changeId).toList(),
        plans.isEmpty() ? BrokerClusterChangeStatus.COMPLETED : BrokerClusterChangeStatus.PENDING,
        plans.stream()
            .mapToLong(ChangePlan::startedAtEpochMillis)
            .min()
            .orElse(value.getRequestedAtEpochMillis()),
        plans.stream().mapToLong(ChangePlan::deadlineEpochMillis).max().orElse(0L));
  }
}
