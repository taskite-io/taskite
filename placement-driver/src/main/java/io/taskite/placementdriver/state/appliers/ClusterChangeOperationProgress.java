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

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeIds;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeStatus;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.ClusterChangeOperation;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationStatus;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import io.taskite.topology.PartitionStatus;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Recomputes aggregate status from the durable phases of its partition plans. */
public final class ClusterChangeOperationProgress {
  private ClusterChangeOperationProgress() {}

  static void refresh(
      final MutableProcessingState state, final String clusterId, final String planChangeId) {
    refreshMemberChange(state, clusterId, planChangeId);
    final String operationId = BrokerClusterChangeIds.extractOperationId(planChangeId);
    final ClusterChangeOperation operation =
        state
            .getBrokerTopologyChangeState()
            .clusterChangeOperation(clusterId, operationId)
            .orElse(null);
    refreshOperation(state, operation);
    // A fenced recovery may also be the successor of an older administrative target. Its own
    // manifest completes when RESET_PEERS completes; the original goal remains independently
    // tracked until its requested membership and job subscription distribution converge.
    state
        .getBrokerTopologyChangeState()
        .currentDescendant(clusterId, planChangeId)
        .filter(plan -> !plan.changeId().equals(planChangeId))
        .flatMap(
            plan ->
                state
                    .getBrokerTopologyChangeState()
                    .clusterChangeOperation(
                        clusterId, BrokerClusterChangeIds.extractOperationId(plan.changeId())))
        .ifPresent(recovery -> refreshOperation(state, recovery));
    state
        .getBrokerTopologyChangeState()
        .activeClusterChange(clusterId)
        .filter(active -> operation == null || !active.changeId().equals(operation.changeId()))
        .ifPresent(active -> refreshOperation(state, active));
  }

  private static void refreshOperation(
      final MutableProcessingState state, final ClusterChangeOperation operation) {
    if (operation == null || operation.status().isTerminal()) {
      return;
    }
    final String clusterId = operation.clusterId();
    final List<ChangePlan> plans =
        operation.planChangeIds().stream()
            .map(changeId -> progressPlan(state, clusterId, changeId))
            .filter(Objects::nonNull)
            .toList();
    if (plans.size() != operation.planChangeIds().size()) {
      return;
    }
    state
        .getBrokerTopologyChangeState()
        .putClusterChangeOperation(operation.withStatus(aggregateStatus(plans)));
  }

  private static void refreshMemberChange(
      final MutableProcessingState state, final String clusterId, final String planChangeId) {
    final var active =
        state.getMemberChangeState().active(MemberChangeScope.BROKER, clusterId).orElse(null);
    if (active == null || active.isTerminal()) {
      return;
    }
    final List<ChangePlan> plans =
        active.getChildPartitionChangeIds().stream()
            .map(changeId -> progressPlan(state, clusterId, changeId))
            .filter(Objects::nonNull)
            .toList();
    if (plans.size() != active.getChildPartitionChangeIds().size()) {
      return;
    }
    final MemberChangeStatus status;
    if (plans.stream().allMatch(plan -> plan.status() == BrokerPartitionChangeStatus.COMPLETED)) {
      status = MemberChangeStatus.COMPLETED;
    } else if (plans.stream()
        .anyMatch(plan -> plan.status() == BrokerPartitionChangeStatus.FAILED)) {
      status =
          plans.stream().allMatch(ChangePlan::isTerminal)
              ? MemberChangeStatus.FAILED
              : MemberChangeStatus.CANCELLING;
    } else if (plans.stream()
        .anyMatch(plan -> plan.status() == BrokerPartitionChangeStatus.WAITING_READY)) {
      status = MemberChangeStatus.WAITING_CATCH_UP;
    } else if (plans.stream()
        .anyMatch(
            plan ->
                plan.status() == BrokerPartitionChangeStatus.IN_PROGRESS
                    || plan.status() == BrokerPartitionChangeStatus.WAITING_LEADER_CONVERGE)) {
      status = MemberChangeStatus.IN_PROGRESS;
    } else {
      status = MemberChangeStatus.PENDING;
    }
    final var updated = active.copy().setStatus(status);
    if (status == MemberChangeStatus.COMPLETED) {
      // Individual Member commands acknowledge convergence; their Appliers finish the parent.
      return;
    }
    if (status.isTerminal()) {
      updated
          .getOperations()
          .forEach(
              operation ->
                  operation.setStatus(
                      status == MemberChangeStatus.COMPLETED
                          ? MemberChangeOperationStatus.COMPLETED
                          : MemberChangeOperationStatus.FAILED));
      updated.setNextOperationIndex(updated.getOperations().size());
      state.getMemberChangeState().clearActive(MemberChangeScope.BROKER, clusterId);
      BrokerRemovalFinalizer.finalizeRemovals(state, clusterId);
    }
    state.getMemberChangeState().put(updated);
  }

  private static BrokerClusterChangeStatus aggregateStatus(final List<ChangePlan> plans) {
    if (plans.stream().allMatch(plan -> plan.status() == BrokerPartitionChangeStatus.COMPLETED)) {
      return BrokerClusterChangeStatus.COMPLETED;
    }
    final boolean failed =
        plans.stream().anyMatch(plan -> plan.status() == BrokerPartitionChangeStatus.FAILED);
    if (failed) {
      return plans.stream().allMatch(ChangePlan::isTerminal)
          ? BrokerClusterChangeStatus.CANCELLED
          : BrokerClusterChangeStatus.CANCELLING;
    }
    if (plans.stream()
        .anyMatch(plan -> plan.status() == BrokerPartitionChangeStatus.WAITING_LEADER_CONVERGE)) {
      return BrokerClusterChangeStatus.WAITING_LEADER_CONVERGE;
    }
    if (plans.stream()
        .anyMatch(plan -> plan.status() == BrokerPartitionChangeStatus.WAITING_READY)) {
      return BrokerClusterChangeStatus.WAITING_READY;
    }
    if (plans.stream().anyMatch(plan -> plan.status() == BrokerPartitionChangeStatus.IN_PROGRESS)) {
      return BrokerClusterChangeStatus.IN_PROGRESS;
    }
    return BrokerClusterChangeStatus.PENDING;
  }

  public static ChangePlan progressPlan(
      final ProcessingState state, final String clusterId, final String rootId) {
    final var root =
        state.getBrokerTopologyChangeState().changePlan(clusterId, rootId).orElse(null);
    final var current =
        state.getBrokerTopologyChangeState().currentDescendant(clusterId, rootId).orElse(null);
    if (root == null || current == null) {
      return null;
    }
    if (current.status() == BrokerPartitionChangeStatus.COMPLETED
        && !current.changeId().equals(root.changeId())
        && !root.operations().isEmpty()
        && !current.operations().isEmpty()) {
      final var expected = root.operations().getLast().configuration();
      final var topology =
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology(clusterId, root.partitionId())
              .orElse(null);
      final boolean targetReached =
          expected != null
              && topology != null
              && topology.status() == PartitionStatus.ACTIVE
              && !state
                  .getBrokerTopologyChangeState()
                  .hasPendingPlanForPartition(clusterId, root.partitionId())
              && new HashSet<>(topology.currentVoters())
                  .equals(
                      expected.targetMembers().stream()
                          .filter(member -> member.voter())
                          .map(member -> member.brokerId())
                          .collect(Collectors.toSet()))
              && new HashSet<>(topology.learners())
                  .equals(
                      expected.targetMembers().stream()
                          .filter(member -> !member.voter())
                          .map(member -> member.brokerId())
                          .collect(Collectors.toSet()))
              && topology
                  .priorities()
                  .equals(
                      expected.targetMembers().stream()
                          .collect(
                              Collectors.toMap(
                                  member -> member.brokerId(), member -> member.priority())))
              && PartitionActivation.redistributionComplete(state, clusterId, root.partitionId());
      if (expected != null && !targetReached) {
        // Availability recovery is complete, but the administrative target remains outstanding.
        return current.withStatus(BrokerPartitionChangeStatus.WAITING_READY);
      }
    }
    return current;
  }
}
