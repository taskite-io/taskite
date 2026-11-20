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

import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.BrokerTopologyPlanner;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.broker.topology.QuorumRecoveryCoordinator;
import io.taskite.placementdriver.broker.topology.ReplicaLifecycleState;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Stateless entry point for every Broker cluster membership and placement calculation. */
public final class BrokerClusterChangePlanningService {

  public BrokerClusterCreationPlan calculateClusterCreation(final BrokerClusterCreation creation) {
    final PartitionTopology topology =
        BrokerTopologyPlanner.planPartition(
            creation.clusterId(),
            creation.configuration(),
            creation.partitionId(),
            creation.eligibleBrokers(),
            creation.voterBrokers());
    return new BrokerClusterCreationPlan(
        topology, BrokerTopologyPlanner.addPartitionOperations(topology));
  }

  public BrokerClusterChangePlan calculateConfigurationChange(
      final BrokerClusterConfigurationChange change) {
    return calculateConfigurationChange(change, true);
  }

  public BrokerClusterChangePlan calculateExplicitRebalance(
      final BrokerClusterConfigurationChange change) {
    return calculateConfigurationChange(change, false);
  }

  private BrokerClusterChangePlan calculateConfigurationChange(
      final BrokerClusterConfigurationChange change, final boolean retainLeader) {
    final var desired =
        retainLeader
            ? preserveLeader(
                change.currentTopology(), change.desiredTopology(), change.observedLeader())
            : change.desiredTopology();
    final List<TopologyOperation> operations =
        new ArrayList<>(
            calculateRebalanceOperations(
                change.currentTopology(), desired, change.observedLeader()));
    final Set<String> trackedMembers =
        new LinkedHashSet<>(change.currentTopology().currentVoters());
    trackedMembers.addAll(change.currentTopology().learners());
    final Set<String> desiredMembers = new LinkedHashSet<>(desired.desiredVoters());
    desiredMembers.addAll(desired.learners());
    for (final BrokerRegistration registration : change.registrations()) {
      final String brokerId = registration.brokerId();
      if (trackedMembers.contains(brokerId) || desiredMembers.contains(brokerId)) {
        continue;
      }
      final boolean hasTemporaryLearner =
          registration.partitions().stream()
              .anyMatch(
                  partition ->
                      partition.partitionId() == change.currentTopology().partitionId()
                          && partition.role() == MemberRole.LEARNER);
      if (hasTemporaryLearner) {
        operations.add(TopologyOperation.join(change.currentTopology().partitionId(), brokerId));
        operations.add(TopologyOperation.leave(change.currentTopology().partitionId(), brokerId));
      }
    }
    return new BrokerClusterChangePlan(desired, List.copyOf(operations));
  }

  public BrokerClusterChangePlan calculateTopologyRebalance(
      final BrokerTopologyRebalance rebalance) {
    final var desired =
        preserveLeader(
            rebalance.currentTopology(), rebalance.desiredTopology(), rebalance.observedLeader());
    return new BrokerClusterChangePlan(
        desired,
        calculateRebalanceOperations(
            rebalance.currentTopology(), desired, rebalance.observedLeader()));
  }

  /** Recovery must converge to the persistent goal rather than choose a new placement goal. */
  public BrokerClusterChangePlan calculateTopologyRebalanceToTarget(
      final BrokerTopologyRebalance rebalance) {
    return new BrokerClusterChangePlan(
        rebalance.desiredTopology(),
        calculateRebalanceOperations(
            rebalance.currentTopology(), rebalance.desiredTopology(), rebalance.observedLeader()));
  }

  private static PartitionTopology preserveLeader(
      final PartitionTopology current,
      final PartitionTopology desired,
      final String observedLeader) {
    final String primary =
        observedLeader != null && desired.desiredVoters().contains(observedLeader)
            ? observedLeader
            : desired.desiredVoters().contains(current.desiredPrimary())
                ? current.desiredPrimary()
                : desired.desiredPrimary();
    return new PartitionTopology(
        desired.identity(),
        new PartitionTopology.Placement(
            desired.replicationFactor(), desired.desiredVoters(), primary),
        PartitionTopology.Membership.initial(desired.desiredVoters(), desired.learners(), primary),
        desired.status(),
        desired.replicaStates(),
        desired.initializing());
  }

  public BrokerTopologyQuorumRecoveryPlan calculateQuorumRecovery(
      final BrokerTopologyQuorumRecovery recovery) {
    if (!QuorumRecoveryCoordinator.hasLostQuorum(
        recovery.currentTopology(), recovery.registrationsById())) {
      return new BrokerTopologyQuorumRecoveryPlan(List.of());
    }
    return new BrokerTopologyQuorumRecoveryPlan(
        QuorumRecoveryCoordinator.planReset(
            recovery.currentTopology().partitionId(),
            recovery.survivorBrokerId(),
            recovery.recoveryEpoch()));
  }

  public List<TopologyOperation> calculateBrokerRecovery(
      final PartitionTopology topology,
      final String brokerId,
      final List<BrokerRegistration> registrations) {
    final List<TopologyOperation> operations = new ArrayList<>();
    if (!topology.currentVoters().contains(brokerId)) {
      if (topology.learners().contains(brokerId)) {
        operations.add(TopologyOperation.leave(topology.partitionId(), brokerId));
      }
      return List.copyOf(operations);
    }
    final List<BrokerRegistration> online = calculateOnlineBrokers(registrations);
    final String replacement = selectRecoveryCandidate(topology, brokerId, online);
    if (replacement.isBlank()) {
      return List.of();
    }
    operations.add(TopologyOperation.join(topology.partitionId(), replacement));
    operations.add(TopologyOperation.promoteLearner(topology.partitionId(), replacement));
    operations.add(TopologyOperation.leave(topology.partitionId(), brokerId));
    return List.copyOf(operations);
  }

  public List<TopologyOperation> calculateBrokerScaleUp(
      final PartitionTopology topology, final String brokerId) {
    if (topology.status() != PartitionStatus.ACTIVE
        || brokerId == null
        || brokerId.isBlank()
        || topology.currentVoters().contains(brokerId)) {
      return List.of();
    }
    final boolean underReplicated = topology.currentVoters().size() < topology.replicationFactor();
    if (topology.learners().contains(brokerId)) {
      return underReplicated
          ? List.of(
              TopologyOperation.join(topology.partitionId(), brokerId),
              TopologyOperation.promoteLearner(topology.partitionId(), brokerId))
          : List.of();
    }
    final List<TopologyOperation> operations = new ArrayList<>();
    operations.add(TopologyOperation.join(topology.partitionId(), brokerId));
    if (underReplicated) {
      operations.add(TopologyOperation.promoteLearner(topology.partitionId(), brokerId));
    }
    return List.copyOf(operations);
  }

  public List<BrokerRegistration> calculateVoterBrokers(
      final BrokerClusterConfig configuration, final List<BrokerRegistration> onlineBrokers) {
    return BrokerTopologyPlanner.voterRegistrations(configuration, onlineBrokers);
  }

  public List<BrokerRegistration> calculateOnlineBrokers(
      final List<BrokerRegistration> registrations) {
    return BrokerTopologyPlanner.online(registrations);
  }

  public List<String> calculateVoterRoster(
      final List<String> currentRoster,
      final int clusterSize,
      final List<BrokerRegistration> registrations,
      final List<PartitionTopology> partitionTopologies) {
    final int size = Math.max(1, clusterSize);
    final Map<String, BrokerStatus> statusById = new LinkedHashMap<>();
    registrations.forEach(
        registration -> statusById.put(registration.brokerId(), registration.status()));
    if (currentRoster == null || currentRoster.isEmpty()) {
      return calculateInitialVoterRoster(size, registrations, partitionTopologies);
    }
    final List<String> result = new ArrayList<>(new LinkedHashSet<>(currentRoster));
    final Set<String> inRoster = new LinkedHashSet<>(result);
    final List<String> spares = calculateOnlineSpares(registrations, inRoster);
    int nextSpare = 0;
    for (int slot = 0; slot < result.size() && nextSpare < spares.size(); slot++) {
      if (statusById.get(result.get(slot)) != BrokerStatus.ONLINE) {
        final String spare = spares.get(nextSpare++);
        inRoster.remove(result.get(slot));
        result.set(slot, spare);
        inRoster.add(spare);
      }
    }
    for (; result.size() < size && nextSpare < spares.size(); nextSpare++) {
      final String spare = spares.get(nextSpare);
      if (inRoster.add(spare)) {
        result.add(spare);
      }
    }
    return result.size() > size ? List.copyOf(result.subList(0, size)) : List.copyOf(result);
  }

  private static String selectRecoveryCandidate(
      final PartitionTopology topology,
      final String unavailableBrokerId,
      final List<BrokerRegistration> online) {
    final Set<String> onlineIds = new HashSet<>();
    online.forEach(registration -> onlineIds.add(registration.brokerId()));
    final Comparator<String> score =
        Comparator.<String, Boolean>comparing(
                brokerId ->
                    !Optional.ofNullable(topology.replicaStates().get(brokerId))
                        .map(state -> state.readyForPromotion())
                        .orElse(false))
            .thenComparingLong(
                brokerId ->
                    Optional.ofNullable(topology.replicaStates().get(brokerId))
                        .map(state -> state.lag())
                        .orElse(Long.MAX_VALUE))
            .thenComparing(String::compareTo);
    return topology.learners().stream()
        .filter(onlineIds::contains)
        .filter(brokerId -> !brokerId.equals(unavailableBrokerId))
        .min(score)
        .or(
            () ->
                onlineIds.stream()
                    .filter(brokerId -> !topology.currentVoters().contains(brokerId))
                    .filter(brokerId -> !brokerId.equals(unavailableBrokerId))
                    .min(score))
        .orElse("");
  }

  private static List<String> calculateInitialVoterRoster(
      final int clusterSize,
      final List<BrokerRegistration> registrations,
      final List<PartitionTopology> partitionTopologies) {
    final Map<String, Long> voterCounts = new LinkedHashMap<>();
    registrations.forEach(registration -> voterCounts.putIfAbsent(registration.brokerId(), 0L));
    partitionTopologies.forEach(
        topology ->
            topology.currentVoters().forEach(voter -> voterCounts.merge(voter, 1L, Long::sum)));
    final Set<String> onlineIds = new LinkedHashSet<>();
    registrations.stream()
        .filter(registration -> registration.status() == BrokerStatus.ONLINE)
        .forEach(registration -> onlineIds.add(registration.brokerId()));
    return voterCounts.keySet().stream()
        .filter(onlineIds::contains)
        .sorted(
            Comparator.<String, Long>comparing(voterCounts::get)
                .reversed()
                .thenComparing(Comparator.naturalOrder()))
        .limit(Math.max(1, clusterSize))
        .toList();
  }

  private static List<String> calculateOnlineSpares(
      final List<BrokerRegistration> registrations, final Set<String> inRoster) {
    return registrations.stream()
        .filter(registration -> registration.status() == BrokerStatus.ONLINE)
        .map(BrokerRegistration::brokerId)
        .filter(brokerId -> !inRoster.contains(brokerId))
        .sorted()
        .toList();
  }

  private static List<TopologyOperation> calculateRebalanceOperations(
      final PartitionTopology current,
      final PartitionTopology desired,
      final String observedLeader) {
    if (current.status() != PartitionStatus.ACTIVE
        || current.partitionId() != desired.partitionId()) {
      return List.of();
    }
    final List<TopologyOperation> operations = new ArrayList<>();
    final String currentLeader = observedLeader == null ? "" : observedLeader;
    final Set<String> currentVoters = new LinkedHashSet<>(current.currentVoters());
    final Set<String> currentLearners = new LinkedHashSet<>(current.learners());
    final Set<String> desiredVoters = new LinkedHashSet<>(desired.desiredVoters());
    final Set<String> desiredLearners = new LinkedHashSet<>(desired.learners());
    for (final String voter : desiredVoters) {
      if (!currentVoters.contains(voter)) {
        operations.add(TopologyOperation.join(current.partitionId(), voter));
        operations.add(TopologyOperation.promoteLearner(current.partitionId(), voter));
      }
    }
    for (final String learner : desiredLearners) {
      if (!currentVoters.contains(learner)
          && (!currentLearners.contains(learner) || learnerReplicationStalled(current, learner))) {
        operations.add(TopologyOperation.join(current.partitionId(), learner));
      }
    }
    for (final String voter : desiredVoters) {
      final int targetPriority =
          desired
              .priorities()
              .getOrDefault(
                  voter,
                  voter.equals(desired.desiredPrimary())
                      ? PartitionTopology.PRIMARY_PRIORITY
                      : PartitionTopology.VOTER_PRIORITY);
      final var observed = current.replicaStates().get(voter);
      if (desiredVoters.size() > 1
          && (current.priorities().getOrDefault(voter, -1) != targetPriority
              || observed != null && observed.priority() != targetPriority)) {
        operations.add(
            TopologyOperation.reconfigurePriority(current.partitionId(), voter, targetPriority));
      }
    }
    final boolean desiredPrimaryChanged =
        !desired.desiredPrimary().equals(current.desiredPrimary());
    if (!desired.desiredPrimary().isBlank()
        && !desired.desiredPrimary().equals(currentLeader)
        && (!currentLeader.isBlank() || desiredPrimaryChanged)) {
      operations.add(
          TopologyOperation.transferLeader(current.partitionId(), desired.desiredPrimary()));
    }
    if (desiredVoters.size() >= desired.replicationFactor()) {
      for (final String voter : currentVoters) {
        if (!desiredVoters.contains(voter)) {
          operations.add(TopologyOperation.leave(current.partitionId(), voter));
          if (desiredLearners.contains(voter)) {
            operations.add(TopologyOperation.join(current.partitionId(), voter));
          }
        }
      }
    }
    for (final String learner : currentLearners) {
      if (!desiredVoters.contains(learner) && !desiredLearners.contains(learner)) {
        operations.add(TopologyOperation.leave(current.partitionId(), learner));
      }
    }
    return List.copyOf(operations);
  }

  private static boolean learnerReplicationStalled(
      final PartitionTopology topology, final String brokerId) {
    final var state = topology.replicaStates().get(brokerId);
    return state != null
        && state.lifecycle() == ReplicaLifecycleState.CATCHING_UP
        && !state.readyForPromotion()
        && state.matchIndex() == 0
        && state.lag() > 0;
  }

  public record BrokerClusterCreation(
      String clusterId,
      BrokerClusterConfig configuration,
      int partitionId,
      List<BrokerRegistration> eligibleBrokers,
      List<BrokerRegistration> voterBrokers) {}

  public record BrokerClusterConfigurationChange(
      PartitionTopology currentTopology,
      PartitionTopology desiredTopology,
      List<BrokerRegistration> registrations,
      String observedLeader) {}

  public record BrokerTopologyRebalance(
      PartitionTopology currentTopology,
      PartitionTopology desiredTopology,
      String observedLeader) {}

  public record BrokerTopologyQuorumRecovery(
      PartitionTopology currentTopology,
      Map<String, BrokerRegistration> registrationsById,
      String survivorBrokerId,
      int recoveryEpoch) {}

  public record BrokerClusterCreationPlan(
      PartitionTopology topology, List<TopologyOperation> operations) {}

  public record BrokerClusterChangePlan(
      PartitionTopology topology, List<TopologyOperation> operations) {}

  public record BrokerTopologyQuorumRecoveryPlan(List<TopologyOperation> operations) {}
}
