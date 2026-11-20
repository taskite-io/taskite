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
package io.taskite.placementdriver.state.immutable;

import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.ClusterChangeOperation;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface BrokerTopologyChangeState {
  Optional<ChangePlan> changePlan(String clusterId, String changeId);

  Optional<ClusterChangeOperation> clusterChangeOperation(String clusterId, String changeId);

  Optional<ClusterChangeOperation> activeClusterChange(String clusterId);

  List<ClusterChangeOperation> recentClusterChanges(String clusterId, int limit);

  List<ChangePlan> pendingChangePlans(String clusterId);

  List<ChangePlan> pendingChangePlansAfter(String clusterId, int partitionId, int limit);

  List<ChangePlan> changePlans(String clusterId, String operationId);

  /** Follows only an atomically installed successor, retaining the original goal at the root. */
  default Optional<ChangePlan> currentDescendant(final String clusterId, final String rootId) {
    final Set<String> visited = new HashSet<>();
    ChangePlan plan = changePlan(clusterId, rootId).orElse(null);
    while (plan != null
        && plan.status() == BrokerPartitionChangeStatus.SUPERSEDED
        && !plan.execution().successor().isBlank()) {
      if (!visited.add(plan.changeId())) {
        return Optional.empty();
      }
      final ChangePlan successor = changePlan(clusterId, plan.execution().successor()).orElse(null);
      if (successor == null
          || !successor.execution().predecessor().equals(plan.changeId())
          || !successor.rootChangeId().equals(plan.rootChangeId())
          || successor.partitionId() != plan.partitionId()) {
        return Optional.empty();
      }
      plan = successor;
    }
    return Optional.ofNullable(plan);
  }

  /**
   * Whether a non-terminal plan already exists for the partition (single-partition serialization).
   */
  boolean hasPendingPlanForPartition(String clusterId, int partitionId);

  /** Whether this exact plan still owns the partition's pending execution. */
  default boolean ownsPendingPlan(final ChangePlan plan) {
    return pendingChangePlans(plan.clusterId()).stream()
        .anyMatch(
            pending ->
                pending.partitionId() == plan.partitionId()
                    && pending.changeId().equals(plan.changeId()));
  }

  /** Whether explicit change-plan ownership has ever been established for the partition. */
  boolean hasChangePlanForPartition(String clusterId, int partitionId);

  /** Whether the given operation id has already been applied (idempotency). */
  boolean isOperationApplied(String clusterId, String opId);
}
