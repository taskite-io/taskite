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

import io.taskite.placementdriver.broker.topology.BrokerTopologyCalculator;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.topology.Partition;
import io.taskite.topology.PartitionStatus;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Derives reservations entirely from committed pending owners, including stopped executions. */
public final class BrokerMigrationBudget {
  public static final Policy DEFAULT = new Policy(1, 1, 1);

  private BrokerMigrationBudget() {}

  public record Policy(
      int maxConcurrentPartitions, int maxIncomingLearnersPerBroker, int maxAffectedPerBroker) {
    public Policy {
      if (maxConcurrentPartitions <= 0
          || maxIncomingLearnersPerBroker <= 0
          || maxAffectedPerBroker <= 0) {
        throw new IllegalArgumentException("migration budgets must be positive");
      }
    }
  }

  public static boolean admitted(final ChangePlan plan) {
    return !plan.isTerminal() && plan.status() != BrokerPartitionChangeStatus.QUEUED;
  }

  public static boolean canAdmit(
      final ProcessingState state, final ChangePlan candidate, final Policy policy) {
    final List<ChangePlan> running =
        state.getBrokerTopologyChangeState().pendingChangePlans(candidate.clusterId()).stream()
            .filter(BrokerMigrationBudget::admitted)
            .filter(plan -> !plan.changeId().equals(candidate.changeId()))
            .toList();
    if (running.stream().anyMatch(plan -> plan.partitionId() == candidate.partitionId())) {
      return false;
    }
    if (candidate.isQuorumRecovery()) {
      return true;
    }
    if (candidate.purpose() == ChangePlan.Purpose.BOOTSTRAP) {
      // The durable lifecycle is authoritative; an online migration cannot claim bootstrap
      // admission merely because a replica is unavailable or the group is under-replicated.
      return state
          .getBrokerPartitionTopologyState()
          .partitionTopology(candidate.clusterId(), candidate.partitionId())
          .map(PartitionTopology::initializing)
          .orElseGet(
              () ->
                  candidate.operations().stream()
                      .anyMatch(operation -> operation.type() == TopologyOperationType.ADD));
    }
    final var migrations =
        running.stream().filter(plan -> plan.purpose() == ChangePlan.Purpose.MIGRATION).toList();
    if (migrations.size() >= policy.maxConcurrentPartitions()) {
      return false;
    }
    for (final String target : incomingLearners(candidate)) {
      if (migrations.stream().filter(plan -> incomingLearners(plan).contains(target)).count()
          >= policy.maxIncomingLearnersPerBroker()) {
        return false;
      }
    }
    for (final String broker : affectedBrokers(candidate)) {
      if (migrations.stream().filter(plan -> affectedBrokers(plan).contains(broker)).count()
          >= policy.maxAffectedPerBroker()) {
        return false;
      }
    }
    // Healthy existing partitions reserve all but the configured migration width. Recovery of
    // an already unavailable partition must remain possible and does not consume another route.
    try {
      final var topology =
          new BrokerTopologyCalculator(state)
              .calculateBrokerTopology(candidate.clusterId(), Instant.EPOCH);
      final var partition = topology.partition(candidate.partitionId()).orElse(null);
      if (partition == null || !partition.serviceReady()) {
        return true;
      }
      final long existing =
          topology.partitions().stream()
              .filter(p -> p.status() != PartitionStatus.CREATING)
              .count();
      final Set<Integer> reserved = new LinkedHashSet<>();
      migrations.forEach(plan -> reserved.add(plan.partitionId()));
      reserved.add(candidate.partitionId());
      final long stable =
          topology.partitions().stream()
              .filter(Partition::serviceReady)
              .filter(p -> !reserved.contains(p.partitionId()))
              .count();
      return stable >= Math.max(0, existing - policy.maxConcurrentPartitions());
    } catch (final RuntimeException unavailable) {
      return false;
    }
  }

  public static Set<String> incomingLearners(final ChangePlan plan) {
    final Set<String> targets = new LinkedHashSet<>();
    plan.operations().stream()
        .filter(operation -> operation.type() == TopologyOperationType.JOIN)
        .forEach(operation -> targets.add(operation.targetBroker()));
    return Set.copyOf(targets);
  }

  private static Set<String> affectedBrokers(final ChangePlan plan) {
    final Set<String> brokers = new LinkedHashSet<>();
    for (final var operation : plan.operations()) {
      brokers.add(operation.targetBroker());
      if (operation.configuration() != null) {
        operation.configuration().sourceMembers().forEach(member -> brokers.add(member.brokerId()));
        operation.configuration().targetMembers().forEach(member -> brokers.add(member.brokerId()));
      }
    }
    return brokers;
  }

  /** Preserve the requested execution duration while moving its clock to admission. */
  public static ChangePlan admit(final ChangePlan plan, final long now) {
    final long duration = Math.max(1, plan.deadlineEpochMillis() - plan.startedAtEpochMillis());
    final long deadline = duration > Long.MAX_VALUE - now ? Long.MAX_VALUE : now + duration;
    return plan.withStatus(BrokerPartitionChangeStatus.PENDING).withDeadline(now, deadline);
  }
}
