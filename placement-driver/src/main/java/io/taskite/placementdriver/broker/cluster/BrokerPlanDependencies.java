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
package io.taskite.placementdriver.broker.cluster;

import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import java.util.Set;
import java.util.stream.Collectors;

/** Evaluates the plan's remaining and final dependencies, independently of dispatch progress. */
public final class BrokerPlanDependencies {
  private BrokerPlanDependencies() {}

  public static Set<String> available(final ProcessingState state, final String clusterId) {
    return state.getBrokerRegistrationState().registrations(clusterId).stream()
        .filter(registration -> registration.status() != BrokerStatus.OFFLINE)
        .map(registration -> registration.brokerId())
        .collect(Collectors.toSet());
  }

  public static PlanBlockedReason invalidation(
      final ProcessingState state, final ChangePlan plan, final long now) {
    if (plan.execution().blockedReason() == PlanBlockedReason.CONFIGURATION_CONFLICT) {
      return PlanBlockedReason.CONFIGURATION_CONFLICT;
    }
    final var available = available(state, plan.clusterId());
    final var eligible =
        state.getBrokerRegistrationState().registrations(plan.clusterId()).stream()
            .filter(registration -> registration.status() == BrokerStatus.ONLINE)
            .filter(
                registration ->
                    !state
                        .getBrokerRegistrationState()
                        .isLearnerExcluded(
                            plan.clusterId(),
                            registration.brokerId(),
                            registration.incarnationId()))
            .map(registration -> registration.brokerId())
            .collect(Collectors.toSet());
    if (!plan.operations().isEmpty()) {
      final var target = plan.operations().getLast().configuration();
      if (target != null
          && target.targetMembers().stream()
              .anyMatch(
                  member ->
                      !eligible.contains(member.brokerId())
                          || (member.voter()
                              && state
                                  .getBrokerRegistrationState()
                                  .isVoterExcluded(plan.clusterId(), member.brokerId())))) {
        return PlanBlockedReason.TARGET_UNAVAILABLE;
      }
    }
    if (plan.operations().stream()
        .skip(plan.nextOperationIndex())
        .filter(operation -> operation.type() != TopologyOperationType.LEAVE)
        .anyMatch(
            operation ->
                !available.contains(operation.targetBroker())
                    || ((operation.type() == TopologyOperationType.JOIN
                            || operation.type() == TopologyOperationType.PROMOTE_LEARNER
                            || operation.type() == TopologyOperationType.TRANSFER_LEADER)
                        && !eligible.contains(operation.targetBroker()))
                    || ((operation.type() == TopologyOperationType.PROMOTE_LEARNER
                            || operation.type() == TopologyOperationType.TRANSFER_LEADER)
                        && state
                            .getBrokerRegistrationState()
                            .isVoterExcluded(plan.clusterId(), operation.targetBroker())))) {
      return PlanBlockedReason.TARGET_UNAVAILABLE;
    }
    return plan.status() != BrokerPartitionChangeStatus.QUEUED
            && plan.deadlineEpochMillis() > 0
            && now >= plan.deadlineEpochMillis()
        ? PlanBlockedReason.PLAN_DEADLINE_EXCEEDED
        : PlanBlockedReason.NONE;
  }

  public static boolean hasQuorum(final ProcessingState state, final ChangePlan plan) {
    final var available = available(state, plan.clusterId());
    return state
        .getBrokerPartitionTopologyState()
        .partitionTopology(plan.clusterId(), plan.partitionId())
        .map(
            topology ->
                topology.currentVoters().stream().filter(available::contains).count()
                    >= topology.currentVoters().size() / 2 + 1)
        .orElse(false);
  }
}
