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
package io.taskite.placementdriver.broker.topology;

import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.immutable.BrokerRegistrationState;
import io.taskite.topology.PartitionStatus;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Deterministic partition-placement planner. Phase 1 preserves the original round-robin assignment,
 * so the persisted {@link PartitionTopology} is bit-for-bit equivalent to the old derived view.
 * Because it depends only on the (replayed) topology config plus the registered brokers, the same
 * inputs always yield the same plan — safe to run from an event applier under the ZEP004 replay
 * invariant.
 *
 * <p>The planner intentionally does not decide leadership: leadership is observed from brokers'
 * self-elected raft roles and overlaid at read time. It also does not assign priorities in Phase 1;
 * {@code desiredPrimary} is set to the first replica purely as a placeholder for later phases.
 */
public final class BrokerTopologyPlanner {
  private BrokerTopologyPlanner() {}

  /** Plans the placement for every partition in the cluster, in ascending partition-id order. */
  public static List<PartitionTopology> plan(
      final String clusterId,
      final BrokerClusterConfig config,
      final List<BrokerRegistration> allRegistrations) {
    final List<PartitionTopology> partitions = new ArrayList<>();
    final List<BrokerRegistration> onlineRegistrations = online(allRegistrations);
    if (onlineRegistrations.isEmpty()) {
      return partitions;
    }
    final List<BrokerRegistration> voterRegistrations =
        voterRegistrations(config, onlineRegistrations);
    for (int partitionId = 1; partitionId <= config.partitionSize(); partitionId++) {
      partitions.add(
          planPartition(clusterId, config, partitionId, onlineRegistrations, voterRegistrations));
    }
    return partitions;
  }

  /** Plans the placement for a single partition using the same math as the full plan. */
  public static PartitionTopology planPartition(
      final String clusterId,
      final BrokerClusterConfig config,
      final int partitionId,
      final List<BrokerRegistration> onlineRegistrations,
      final List<BrokerRegistration> voterRegistrations) {
    final int partitionIndex = partitionId - 1;
    final int brokerCount = voterRegistrations.size();
    final int replicaCount = Math.min(config.replicationFactor(), brokerCount);
    final List<String> replicas = new ArrayList<>();
    for (int replica = 0; replica < replicaCount; replica++) {
      replicas.add(voterRegistrations.get((partitionIndex + replica) % brokerCount).brokerId());
    }
    final Set<String> voterPool =
        voterRegistrations.stream().map(BrokerRegistration::brokerId).collect(Collectors.toSet());
    final List<String> learners =
        onlineRegistrations.stream()
            .filter(registration -> registration.status() == BrokerStatus.ONLINE)
            .map(BrokerRegistration::brokerId)
            .filter(brokerId -> !voterPool.contains(brokerId))
            .toList();
    final String desiredPrimary = replicas.isEmpty() ? "" : replicas.get(0);
    return new PartitionTopology(
        new PartitionTopology.Identity(clusterId, partitionId),
        new PartitionTopology.Placement(config.replicationFactor(), replicas, desiredPrimary),
        PartitionTopology.Membership.initial(replicas, learners, desiredPrimary),
        PartitionStatus.ACTIVE,
        Map.of());
  }

  /**
   * Expands a new partition placement into the ordered operations required by the topology actor.
   * The primary is bootstrapped first, target voters join as learners before promotion, and
   * remaining online brokers stay as learners.
   */
  public static List<TopologyOperation> addPartitionOperations(final PartitionTopology placement) {
    if (placement.desiredPrimary().isBlank() || placement.desiredVoters().isEmpty()) {
      return List.of();
    }
    final List<TopologyOperation> operations = new ArrayList<>();
    operations.add(TopologyOperation.add(placement.partitionId(), placement.desiredPrimary()));
    for (final String voter : placement.desiredVoters()) {
      if (voter.equals(placement.desiredPrimary())) {
        continue;
      }
      operations.add(TopologyOperation.join(placement.partitionId(), voter));
      operations.add(TopologyOperation.promoteLearner(placement.partitionId(), voter));
    }
    for (final String learner : placement.learners()) {
      operations.add(TopologyOperation.join(placement.partitionId(), learner));
    }
    if (placement.desiredVoters().size() > 1) {
      for (final String voter : placement.desiredVoters()) {
        operations.add(
            TopologyOperation.reconfigurePriority(
                placement.partitionId(),
                voter,
                placement.priorities().getOrDefault(voter, PartitionTopology.VOTER_PRIORITY)));
      }
    }
    operations.add(
        TopologyOperation.stepDownIfNotPrimary(
            placement.partitionId(), placement.desiredPrimary()));
    return List.copyOf(operations);
  }

  /** Online registrations, preserving the caller's order. */
  public static List<BrokerRegistration> online(final List<BrokerRegistration> registrations) {
    return registrations.stream()
        .filter(registration -> registration.status() == BrokerStatus.ONLINE)
        .toList();
  }

  /** Learner placement excludes the removed startup, while a new startup can return as a spare. */
  public static List<BrokerRegistration> eligibleRegistrations(
      final BrokerRegistrationState state, final String clusterId) {
    return state.registrations(clusterId).stream()
        .filter(
            registration ->
                !state.isLearnerExcluded(
                    clusterId, registration.brokerId(), registration.incarnationId()))
        .toList();
  }

  /** Ordinary voter candidates; lifecycle admission can use a new startup to fill a vacancy. */
  public static List<BrokerRegistration> eligibleVoterRegistrations(
      final BrokerRegistrationState state, final String clusterId) {
    return eligibleRegistrations(state, clusterId).stream()
        .filter(registration -> !state.isVoterExcluded(clusterId, registration.brokerId()))
        .toList();
  }

  /**
   * Registrations that remain addressable by the runtime topology.
   *
   * <p>A draining Broker is ineligible for new placement, but it must remain routable until every
   * current Raft membership and leadership reference has been migrated away.
   */
  public static List<BrokerRegistration> routable(final List<BrokerRegistration> registrations) {
    return registrations.stream()
        .filter(
            registration ->
                registration.status() == BrokerStatus.ONLINE
                    || registration.status() == BrokerStatus.DRAINING)
        .toList();
  }

  /**
   * The eligible voter registrations for placement, in deterministic order.
   *
   * <p>When the cluster has a non-empty persisted voter roster (T9), voters are drawn from the
   * roster in roster order, restricted to those currently online — the roster is the canonical,
   * sticky source of "who is a voter", so a broker that is not in the roster stays a learner
   * (spare) and a returning broker whose slot was reassigned does not reclaim it. An empty roster
   * means no Broker has been admitted by the configuration API yet.
   */
  public static List<BrokerRegistration> voterRegistrations(
      final BrokerClusterConfig config, final List<BrokerRegistration> onlineRegistrations) {
    final List<String> roster = config.voterRoster();
    if (roster == null || roster.isEmpty()) {
      return config.managedRoster()
          ? List.of()
          : onlineRegistrations.stream().limit(Math.max(1, config.clusterSize())).toList();
    }
    final Map<String, BrokerRegistration> onlineById = new LinkedHashMap<>();
    for (final BrokerRegistration registration : onlineRegistrations) {
      onlineById.put(registration.brokerId(), registration);
    }
    final List<BrokerRegistration> voters = new ArrayList<>();
    for (final String brokerId : roster) {
      final BrokerRegistration registration = onlineById.get(brokerId);
      if (registration != null) {
        voters.add(registration);
      }
    }
    return List.copyOf(voters);
  }
}
