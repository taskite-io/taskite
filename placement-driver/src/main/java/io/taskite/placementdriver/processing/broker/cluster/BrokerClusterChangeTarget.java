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
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeCancellationReason;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeType;
import java.util.List;
import java.util.Optional;

/** Resolves the same durable change entity for detail queries and cancellation commands. */
record BrokerClusterChangeTarget(
    ClusterChangeOperation clusterChange, MemberChangeRecord memberChange, List<ChangePlan> plans) {

  static Optional<BrokerClusterChangeTarget> resolve(
      final ProcessingState state, final String clusterId, final String changeId) {
    final var changes = state.getBrokerTopologyChangeState();
    final var clusterChange = changes.clusterChangeOperation(clusterId, changeId).orElse(null);
    final var memberChange =
        state
            .getMemberChangeState()
            .get(MemberChangeScope.BROKER, clusterId, changeId)
            .orElse(null);
    final List<ChangePlan> plans =
        clusterChange != null
            ? clusterChange.planChangeIds().stream()
                .map(id -> changes.changePlan(clusterId, id))
                .flatMap(Optional::stream)
                .toList()
            : memberChange != null
                ? memberChange.getChildPartitionChangeIds().stream()
                    .map(id -> changes.changePlan(clusterId, id))
                    .flatMap(Optional::stream)
                    .toList()
                : changes.changePlans(clusterId, changeId);
    return clusterChange == null && memberChange == null && plans.isEmpty()
        ? Optional.empty()
        : Optional.of(new BrokerClusterChangeTarget(clusterChange, memberChange, plans));
  }

  ChangeType changeType() {
    return clusterChange != null
        ? ChangeType.CLUSTER
        : memberChange != null ? ChangeType.MEMBER : ChangeType.PARTITION;
  }

  ChangeCancellationReason cancellationReason(final ProcessingState state) {
    if (memberChange != null
        && !memberChange.isTerminal()
        && !memberChange.getSourceVoters().isEmpty()
        && state
            .getBrokerClusterConfigState()
            .findClusterConfig(memberChange.getResourceId())
            .filter(config -> config.replicationFactor() > memberChange.getSourceVoters().size())
            .isPresent()) {
      return ChangeCancellationReason.UNSAFE_CONFIGURATION_CANCELLATION;
    }
    if (clusterChange != null) {
      return clusterChange.status().isTerminal()
          ? ChangeCancellationReason.CHANGE_TERMINAL
          : ChangeCancellationReason.NONE;
    }
    if (memberChange != null) {
      return memberChange.isTerminal()
          ? ChangeCancellationReason.CHANGE_TERMINAL
          : ChangeCancellationReason.NONE;
    }
    // An old ID may display a descendant in GET. It never authorizes cancelling that descendant.
    if (plans.stream().anyMatch(plan -> plan.status() == BrokerPartitionChangeStatus.SUPERSEDED)) {
      return ChangeCancellationReason.CHANGE_SUPERSEDED;
    }
    if (plans.stream().anyMatch(plan -> plan.status() == BrokerPartitionChangeStatus.SUPERSEDING)) {
      return ChangeCancellationReason.CHANGE_RECONCILING;
    }
    if (plans.stream()
        .anyMatch(plan -> !plan.isTerminal() && !plan.execution().predecessor().isBlank())) {
      return ChangeCancellationReason.CHANGE_RECOVERY_REQUIRED;
    }
    if (plans.stream().allMatch(ChangePlan::isTerminal)) {
      return ChangeCancellationReason.CHANGE_TERMINAL;
    }
    if (plans.stream()
        .anyMatch(
            plan ->
                !plan.isTerminal()
                    && !state.getBrokerTopologyChangeState().ownsPendingPlan(plan))) {
      return ChangeCancellationReason.CHANGE_NOT_CURRENT;
    }
    return ChangeCancellationReason.NONE;
  }
}
