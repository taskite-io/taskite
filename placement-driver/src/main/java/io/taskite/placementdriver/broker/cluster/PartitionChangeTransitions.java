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

import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.topology.PartitionStatus;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Pure state transitions shared by partition-change appliers and the orchestration actor. */
public final class PartitionChangeTransitions {
  private PartitionChangeTransitions() {}

  public static PartitionTopology startPlan(
      final Optional<PartitionTopology> existing,
      final ChangePlan plan,
      final int replicationFactor) {
    final boolean addPlan =
        plan.operations().stream()
            .anyMatch(operation -> operation.type() == TopologyOperationType.ADD);
    final Set<String> currentVoters = new LinkedHashSet<>();
    final Set<String> learners = new LinkedHashSet<>();
    final Set<String> desiredVoters = new LinkedHashSet<>();
    existing.ifPresent(
        topology -> {
          currentVoters.addAll(topology.currentVoters());
          learners.addAll(topology.learners());
          desiredVoters.addAll(topology.currentVoters());
        });
    for (final TopologyOperation operation : plan.operations()) {
      switch (operation.type()) {
        case ADD, PROMOTE_LEARNER -> desiredVoters.add(operation.targetBroker());
        case LEAVE -> desiredVoters.remove(operation.targetBroker());
        case RESET_PEERS -> {
          // Fenced reset collapses the voter/learner sets to the single survivor.
          desiredVoters.clear();
          desiredVoters.add(operation.targetBroker());
        }
        default -> {}
      }
    }
    final String desiredPrimary =
        plan.operations().stream()
            .filter(
                operation ->
                    operation.type() == TopologyOperationType.ADD
                        || operation.type() == TopologyOperationType.TRANSFER_LEADER
                        || operation.type() == TopologyOperationType.STEP_DOWN_IF_NOT_PRIMARY
                        || operation.type() == TopologyOperationType.RESET_PEERS)
            .reduce((left, right) -> right)
            .map(TopologyOperation::targetBroker)
            .orElseGet(
                () ->
                    existing
                        .map(PartitionTopology::desiredPrimary)
                        .filter(primary -> !primary.isBlank() && desiredVoters.contains(primary))
                        .orElseGet(() -> desiredVoters.stream().findFirst().orElse("")));
    final Map<String, Integer> priorities =
        new LinkedHashMap<>(existing.map(PartitionTopology::priorities).orElse(Map.of()));
    for (final String voter : currentVoters) {
      priorities.putIfAbsent(voter, PartitionTopology.VOTER_PRIORITY);
    }
    for (final String learner : learners) {
      priorities.putIfAbsent(learner, PartitionTopology.LEARNER_PRIORITY);
    }
    final PartitionTopology topology =
        new PartitionTopology(
            new PartitionTopology.Identity(plan.clusterId(), plan.partitionId()),
            new PartitionTopology.Placement(
                Math.max(1, replicationFactor), desiredVoters.stream().toList(), desiredPrimary),
            new PartitionTopology.Membership(
                currentVoters.stream().toList(),
                learners.stream().toList(),
                priorities,
                existing.map(PartitionTopology::recoveryEpoch).orElse(0L)),
            addPlan
                    || existing
                        .filter(previous -> previous.status() == PartitionStatus.CREATING)
                        .isPresent()
                ? PartitionStatus.CREATING
                : plan.status() == BrokerPartitionChangeStatus.QUEUED
                    ? existing.map(PartitionTopology::status).orElse(PartitionStatus.REBALANCING)
                    : PartitionStatus.REBALANCING,
            existing.map(PartitionTopology::replicaStates).orElse(Map.of()),
            existing.map(PartitionTopology::initializing).orElse(addPlan));
    return topology;
  }

  public static PartitionTopology afterOperation(
      final PartitionTopology topology, final ChangePlan planBeforeAdvance) {
    return afterOperation(topology, planBeforeAdvance, true);
  }

  public static PartitionTopology afterOperation(
      final PartitionTopology topology,
      final ChangePlan planBeforeAdvance,
      final boolean redistributionComplete) {
    final PartitionTopology applied = apply(topology, planBeforeAdvance.nextOperation());
    final ChangePlan advanced = planBeforeAdvance.advanced();
    if (advanced.isTerminal()) {
      if ((applied.status() == PartitionStatus.CREATING || applied.initializing())
          && !redistributionComplete) {
        return applied;
      }
      // Bootstrap completion also depends on the current persisted roster, checked by the applier.
      return markActive(applied);
    }
    return applied;
  }

  public static PartitionTopology markActive(final PartitionTopology topology) {
    return withStatus(topology, PartitionStatus.ACTIVE);
  }

  public static PartitionTopology finishBootstrap(final PartitionTopology topology) {
    final boolean initialized =
        topology.currentVoters().size() >= topology.replicationFactor()
            && new LinkedHashSet<>(topology.currentVoters())
                .equals(new LinkedHashSet<>(topology.desiredVoters()))
            // Learners may be part of the final placement; the applier checks their exact set.
            && topology.learners().stream()
                .allMatch(
                    learner ->
                        !topology.currentVoters().contains(learner)
                            && topology.priorities().getOrDefault(learner, -1)
                                == PartitionTopology.LEARNER_PRIORITY)
            && topology.currentVoters().contains(topology.desiredPrimary())
            && topology.currentVoters().stream()
                .allMatch(
                    voter ->
                        topology.priorities().getOrDefault(voter, -1)
                            == (voter.equals(topology.desiredPrimary())
                                ? PartitionTopology.PRIMARY_PRIORITY
                                : PartitionTopology.VOTER_PRIORITY));
    return markActive(topology).withInitializing(topology.initializing() && !initialized);
  }

  /** Returns the membership that should be sent to jraft for this operation. */
  public static PartitionTopology targetMembership(
      final PartitionTopology topology, final TopologyOperation operation) {
    return apply(topology, operation);
  }

  /** Stops driving the target and accepts the currently applied membership as stable. */
  public static PartitionTopology cancel(final PartitionTopology topology) {
    final String primary =
        topology.currentVoters().contains(topology.desiredPrimary())
            ? topology.desiredPrimary()
            : topology.currentVoters().stream().findFirst().orElse("");
    return new PartitionTopology(
        new PartitionTopology.Identity(topology.clusterId(), topology.partitionId()),
        new PartitionTopology.Placement(
            topology.replicationFactor(), topology.currentVoters(), primary),
        new PartitionTopology.Membership(
            topology.currentVoters(),
            topology.learners(),
            topology.priorities(),
            topology.recoveryEpoch()),
        PartitionStatus.ACTIVE,
        topology.replicaStates(),
        topology.initializing());
  }

  private static PartitionTopology apply(
      final PartitionTopology topology, final TopologyOperation operation) {
    final Set<String> voters = new LinkedHashSet<>(topology.currentVoters());
    final Set<String> learners = new LinkedHashSet<>(topology.learners());
    final Map<String, Integer> priorities = new LinkedHashMap<>(topology.priorities());
    long recoveryEpoch = topology.recoveryEpoch();
    switch (operation.type()) {
      case ADD -> {
        learners.remove(operation.targetBroker());
        voters.add(operation.targetBroker());
        priorities.put(
            operation.targetBroker(),
            operation.targetBroker().equals(topology.desiredPrimary())
                ? PartitionTopology.PRIMARY_PRIORITY
                : PartitionTopology.VOTER_PRIORITY);
      }
      case JOIN -> {
        if (!voters.contains(operation.targetBroker())) {
          learners.add(operation.targetBroker());
          priorities.put(operation.targetBroker(), PartitionTopology.LEARNER_PRIORITY);
        }
      }
      case PROMOTE_LEARNER -> {
        learners.remove(operation.targetBroker());
        voters.add(operation.targetBroker());
        priorities.put(
            operation.targetBroker(),
            operation.targetBroker().equals(topology.desiredPrimary())
                ? PartitionTopology.PRIMARY_PRIORITY
                : PartitionTopology.VOTER_PRIORITY);
      }
      case LEAVE -> {
        voters.remove(operation.targetBroker());
        learners.remove(operation.targetBroker());
        priorities.remove(operation.targetBroker());
      }
      case RECONFIGURE_PRIORITY -> priorities.put(operation.targetBroker(), operation.priority());
      case TRANSFER_LEADER, STEP_DOWN_IF_NOT_PRIMARY -> {}
      case RESET_PEERS -> {
        // Fenced disaster recovery: the survivor becomes the sole voter and primary; every other
        // voter/learner is dropped from the committed set. The operation priority carries the
        // target
        // recovery epoch (Q6), which becomes the partition's new monotonic epoch.
        voters.clear();
        learners.clear();
        priorities.clear();
        voters.add(operation.targetBroker());
        priorities.put(operation.targetBroker(), PartitionTopology.PRIMARY_PRIORITY);
        recoveryEpoch = Math.max(recoveryEpoch, operation.priority());
      }
    }
    return new PartitionTopology(
        new PartitionTopology.Identity(topology.clusterId(), topology.partitionId()),
        new PartitionTopology.Placement(
            topology.replicationFactor(), topology.desiredVoters(), topology.desiredPrimary()),
        new PartitionTopology.Membership(
            voters.stream().toList(), learners.stream().toList(), priorities, recoveryEpoch),
        topology.status(),
        topology.replicaStates(),
        topology.initializing());
  }

  private static PartitionTopology withStatus(
      final PartitionTopology topology, final PartitionStatus status) {
    return new PartitionTopology(
        new PartitionTopology.Identity(topology.clusterId(), topology.partitionId()),
        new PartitionTopology.Placement(
            topology.replicationFactor(), topology.desiredVoters(), topology.desiredPrimary()),
        new PartitionTopology.Membership(
            topology.currentVoters(),
            topology.learners(),
            topology.priorities(),
            topology.recoveryEpoch()),
        status,
        topology.replicaStates(),
        topology.initializing());
  }
}
