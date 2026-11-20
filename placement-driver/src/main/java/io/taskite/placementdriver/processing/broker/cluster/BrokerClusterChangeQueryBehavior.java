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
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.ClusterChangeOperation;
import io.taskite.placementdriver.broker.cluster.PartitionChangeTransitions;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.processing.query.TopologyQueryBehavior;
import io.taskite.placementdriver.processing.query.TopologyQueryRejection;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerClusterChangeStatusRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.record.TopologyOperationType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeCancellationReason;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeStatus;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.QueryType;
import io.taskite.topology.PartitionStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class BrokerClusterChangeQueryBehavior implements TopologyQueryBehavior {
  private final ProcessingState state;
  private static final String CHANGE_NOT_FOUND =
      "CHANGE_NOT_FOUND: Expected to query topology change '%s' in cluster '%s', but no matching change was found";
  private static final String ACTIVE_CHANGE_NOT_FOUND =
      "ACTIVE_CHANGE_NOT_FOUND: Expected to query the active topology change in cluster '%s', but no active change was found";

  public BrokerClusterChangeQueryBehavior(final ProcessingState state) {
    this.state = state;
  }

  @Override
  public QueryType queryType() {
    return QueryType.BROKER_CLUSTER_CHANGE;
  }

  @Override
  public UnifiedRecordValue createResponseValue(final TopologyQueryRecord query) {
    final String clusterId = query.getClusterId();
    final String changeId = resolveChangeId(clusterId, query.getChangeId());
    final var target =
        BrokerClusterChangeTarget.resolve(state, clusterId, changeId)
            .orElseThrow(
                () ->
                    TopologyQueryRejection.notFound(
                        CHANGE_NOT_FOUND.formatted(changeId, clusterId)));
    final ClusterChangeOperation clusterChange = target.clusterChange();
    final MemberChangeRecord memberChange = target.memberChange();
    final List<ChangePlan> plans = target.plans();
    final List<ChangePlan> currentPlans =
        plans.stream()
            .map(
                plan ->
                    state
                        .getBrokerTopologyChangeState()
                        .currentDescendant(clusterId, plan.changeId())
                        .orElse(plan))
            .toList();
    final boolean terminal =
        clusterChange != null
            ? clusterChange.status().isTerminal()
            : memberChange != null
                ? memberChange.isTerminal()
                : currentPlans.stream().allMatch(ChangePlan::isTerminal);
    final ChangeStatus status =
        clusterChange != null
            ? ChangeStatus.from(clusterChange.status())
            : memberChange != null
                ? ChangeStatus.from(memberChange.getStatus())
                : terminal
                    ? currentPlans.stream()
                            .allMatch(
                                plan -> plan.status() == BrokerPartitionChangeStatus.COMPLETED)
                        ? ChangeStatus.COMPLETED
                        : ChangeStatus.from(currentPlans.getFirst().status())
                    : currentPlans.stream()
                            .anyMatch(plan -> plan.status() == BrokerPartitionChangeStatus.FAILED)
                        ? ChangeStatus.CANCELLING
                        : ChangeStatus.from(currentPlans.getFirst().status());
    final List<BrokerClusterChangeStatusRecord.PartitionChange> partitionChanges =
        currentPlans.stream()
            .filter(plan -> !plan.isTerminal())
            .map(BrokerClusterChangeQueryBehavior::partitionChange)
            .toList();
    final Map<Integer, ChangePlan> plansByPartition = new LinkedHashMap<>();
    plans.forEach(plan -> plansByPartition.put(plan.partitionId(), plan));
    final List<PartitionTopology> currentTopologies =
        state.getBrokerPartitionTopologyState().partitionTopologies(clusterId);
    final var currentTopology =
        currentTopologies.stream()
            .map(BrokerClusterChangeQueryBehavior::partitionPlacement)
            .toList();
    final var expectedTopology =
        clusterChange != null
            ? clusterChange.expectedTopology().stream()
                .map(BrokerClusterChangeQueryBehavior::partitionPlacement)
                .toList()
            : currentTopologies.stream()
                .map(
                    topology ->
                        partitionPlacement(
                            calculateExpectedTopology(
                                topology, plansByPartition.get(topology.partitionId()))))
                .toList();
    final var plannedChanges =
        plans.stream()
            .flatMap(plan -> plan.operations().stream())
            .map(BrokerClusterChangeQueryBehavior::operation)
            .toList();
    return new BrokerClusterChangeStatusRecord()
        .setClusterId(clusterId)
        .setChangeId(changeId)
        .setStatus(status)
        .setTerminal(terminal)
        .setChangeType(target.changeType())
        .setCancellable(target.cancellationReason(state) == ChangeCancellationReason.NONE)
        .setCancellationReason(target.cancellationReason(state))
        .setCurrentTopology(currentTopology)
        .setPlannedChanges(plannedChanges)
        .setPartitionChanges(partitionChanges)
        .setExecutionHistory(
            plans.stream()
                .flatMap(plan -> history(clusterId, plan).stream())
                .distinct()
                .map(BrokerClusterChangeQueryBehavior::partitionChange)
                .toList())
        .setExpectedTopology(expectedTopology);
  }

  private List<ChangePlan> history(final String clusterId, final ChangePlan root) {
    final var result = new ArrayList<ChangePlan>();
    final var visited = new HashSet<String>();
    ChangePlan current = root;
    while (current != null && visited.add(current.changeId())) {
      result.add(current);
      current =
          current.execution().successor().isBlank()
              ? null
              : state
                  .getBrokerTopologyChangeState()
                  .changePlan(clusterId, current.execution().successor())
                  .orElse(null);
    }
    return result;
  }

  private String resolveChangeId(final String clusterId, final String requestedChangeId) {
    if (requestedChangeId != null && !requestedChangeId.isBlank()) {
      return requestedChangeId;
    }
    return state
        .getBrokerTopologyChangeState()
        .activeClusterChange(clusterId)
        .map(ClusterChangeOperation::changeId)
        .or(
            () ->
                state
                    .getMemberChangeState()
                    .active(MemberChangeScope.BROKER, clusterId)
                    .map(MemberChangeRecord::getChangeId))
        .or(
            () ->
                state.getBrokerTopologyChangeState().pendingChangePlans(clusterId).stream()
                    .findFirst()
                    .map(ChangePlan::changeId))
        .orElseThrow(
            () -> TopologyQueryRejection.notFound(ACTIVE_CHANGE_NOT_FOUND.formatted(clusterId)));
  }

  private static BrokerClusterChangeStatusRecord.PartitionChange partitionChange(
      final ChangePlan plan) {
    final List<BrokerClusterChangeStatusRecord.Operation> remainingOperations =
        plan.operations().subList(plan.nextOperationIndex(), plan.operations().size()).stream()
            .map(BrokerClusterChangeQueryBehavior::operation)
            .toList();
    return new BrokerClusterChangeStatusRecord.PartitionChange(
        plan.changeId(),
        plan.partitionId(),
        ChangeStatus.from(plan.status()),
        plan.nextOperationIndex(),
        plan.operations().size(),
        plan.isTerminal(),
        remainingOperations.isEmpty() ? null : remainingOperations.getFirst(),
        remainingOperations,
        instantOrNull(plan.startedAtEpochMillis()),
        instantOrNull(plan.deadlineEpochMillis()),
        plan.execution().generation(),
        plan.execution().authorityIndex(),
        plan.rootChangeId(),
        plan.execution().predecessor(),
        plan.execution().successor(),
        plan.execution().blockedReason(),
        instantOrNull(plan.execution().blockedSinceEpochMillis()));
  }

  private static PartitionTopology calculateExpectedTopology(
      final PartitionTopology current, final ChangePlan plan) {
    if (plan == null) {
      return current;
    }
    final var frozen =
        plan.operations().isEmpty() ? null : plan.operations().getLast().configuration();
    if (frozen != null) {
      final var priorities =
          frozen.targetMembers().stream()
              .collect(Collectors.toMap(member -> member.brokerId(), member -> member.priority()));
      final var voters =
          frozen.targetMembers().stream()
              .filter(member -> member.voter())
              .map(member -> member.brokerId())
              .toList();
      final var learners =
          frozen.targetMembers().stream()
              .filter(member -> !member.voter())
              .map(member -> member.brokerId())
              .toList();
      final var primary =
          frozen.targetMembers().stream()
              .filter(member -> member.voter())
              .max(Comparator.comparingInt(member -> member.priority()))
              .map(member -> member.brokerId())
              .orElse("");
      return new PartitionTopology(
          current.identity(),
          new PartitionTopology.Placement(current.replicationFactor(), voters, primary),
          new PartitionTopology.Membership(voters, learners, priorities, current.recoveryEpoch()),
          PartitionStatus.ACTIVE,
          current.replicaStates(),
          current.initializing());
    }
    if (plan.isTerminal()) {
      return current;
    }
    PartitionTopology expected = current;
    for (int index = plan.nextOperationIndex(); index < plan.operations().size(); index++) {
      expected =
          PartitionChangeTransitions.targetMembership(expected, plan.operations().get(index));
    }
    return PartitionChangeTransitions.markActive(expected);
  }

  private static BrokerClusterChangeStatusRecord.PartitionPlacement partitionPlacement(
      final PartitionTopology topology) {
    return new BrokerClusterChangeStatusRecord.PartitionPlacement(
        topology.partitionId(),
        topology.status(),
        topology.desiredVoters(),
        topology.currentVoters(),
        topology.learners(),
        topology.desiredPrimary(),
        topology.priorities());
  }

  private static BrokerClusterChangeStatusRecord.PartitionPlacement partitionPlacement(
      final ClusterChangeOperation.ExpectedPartition topology) {
    return new BrokerClusterChangeStatusRecord.PartitionPlacement(
        topology.partitionId(),
        PartitionStatus.ACTIVE,
        topology.desiredVoters(),
        topology.currentVoters(),
        topology.learners(),
        topology.desiredPrimary(),
        topology.priorities());
  }

  private static BrokerClusterChangeStatusRecord.Operation operation(
      final TopologyOperation operation) {
    return new BrokerClusterChangeStatusRecord.Operation(
        operation.opId(),
        TopologyOperationType.valueOf(operation.type().name()),
        operation.targetBroker(),
        operation.priority());
  }

  private static String instantOrNull(final long epochMillis) {
    return epochMillis <= 0 ? null : Instant.ofEpochMilli(epochMillis).toString();
  }
}
