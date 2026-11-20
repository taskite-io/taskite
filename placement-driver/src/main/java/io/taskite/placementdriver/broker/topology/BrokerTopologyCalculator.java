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

import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.ClusterTopology;
import io.taskite.topology.Partition;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Calculates one Broker topology snapshot from committed state. */
public final class BrokerTopologyCalculator {
  private final ProcessingState state;

  public BrokerTopologyCalculator(final ProcessingState state) {
    this.state = state;
  }

  public ClusterTopology calculateBrokerTopology(final String clusterId, final Instant updatedAt) {
    return calculateBrokerTopologySnapshot(clusterId, updatedAt).topology();
  }

  public BrokerTopologySnapshot calculateBrokerTopologySnapshot(
      final String clusterId, final Instant updatedAt) {
    final BrokerClusterConfig config =
        state
            .getBrokerClusterConfigState()
            .findClusterConfig(clusterId)
            .orElseThrow(
                () ->
                    new BrokerTopologyCalculationException(
                        BrokerTopologyCalculationException.Status.NOT_FOUND,
                        "CLUSTER_NOT_REGISTERED: " + clusterId));
    final List<BrokerRegistration> registrations =
        state.getBrokerRegistrationState().registrations(clusterId);
    final List<BrokerRegistration> online =
        BrokerTopologyPlanner.eligibleRegistrations(state.getBrokerRegistrationState(), clusterId)
            .stream()
            .filter(registration -> registration.status() == BrokerStatus.ONLINE)
            .toList();
    final List<BrokerRegistration> routable = BrokerTopologyPlanner.routable(registrations);
    if (registrations.isEmpty()) {
      throw new BrokerTopologyCalculationException(
          BrokerTopologyCalculationException.Status.UNAVAILABLE,
          "NO_BROKER_REGISTERED: " + clusterId);
    }
    if (online.isEmpty()
        && state.getBrokerPartitionTopologyState().partitionTopologies(clusterId).isEmpty()) {
      throw new BrokerTopologyCalculationException(
          BrokerTopologyCalculationException.Status.UNAVAILABLE, "NO_ONLINE_BROKER: " + clusterId);
    }
    final Map<String, BrokerEndpoint> brokers = new LinkedHashMap<>();
    final Map<String, BrokerRegistration> registrationsById = new LinkedHashMap<>();
    registrations.forEach(
        registration -> registrationsById.put(registration.brokerId(), registration));
    for (final BrokerRegistration registration : routable) {
      brokers.put(
          registration.brokerId(),
          new BrokerEndpoint(
              registration.brokerId(),
              registration.host(),
              registration.port(),
              registration.raftPort()));
    }
    final List<BrokerRegistration> voters =
        BrokerTopologyPlanner.voterRegistrations(config, online);
    final Map<Integer, ChangePlan> pendingPlans = new LinkedHashMap<>();
    state
        .getBrokerTopologyChangeState()
        .pendingChangePlans(clusterId)
        .forEach(plan -> pendingPlans.put(plan.partitionId(), plan));
    final List<Partition> partitions = new ArrayList<>();
    for (int partitionId = 1; partitionId <= config.partitionSize(); partitionId++) {
      final int currentPartitionId = partitionId;
      final PartitionTopology placement =
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology(clusterId, currentPartitionId)
              .orElseGet(
                  () ->
                      BrokerTopologyPlanner.planPartition(
                          clusterId, config, currentPartitionId, online, voters));
      final PartitionLeadershipResolver.Observation leadership =
          PartitionLeadershipResolver.resolve(
              routable, currentPartitionId, placement.currentVoters(), placement.recoveryEpoch());
      // Liveness does not change Raft membership. Keep every committed voter and learner endpoint
      // until removal is applied, so membership changes can resolve the remaining offline peers.
      final List<String> members = new ArrayList<>(placement.currentVoters());
      members.addAll(placement.learners());
      for (final String member : members) {
        final BrokerRegistration registration = registrationsById.get(member);
        if (registration != null) {
          brokers.putIfAbsent(
              member,
              new BrokerEndpoint(
                  registration.brokerId(),
                  registration.host(),
                  registration.port(),
                  registration.raftPort()));
        }
      }
      final String leaderId = leadership.leaderId();
      final List<String> learners = new ArrayList<>(placement.learners());
      final Map<String, Integer> priorities = new LinkedHashMap<>(placement.priorities());
      final ChangePlan pending = pendingPlans.get(currentPartitionId);
      final boolean destructiveChange =
          pending != null
              && pending.operations().stream()
                  .anyMatch(operation -> operation.type() == TopologyOperationType.RESET_PEERS);
      if (pending != null
          && !pending.isTerminal()
          && pending.status() != BrokerPartitionChangeStatus.QUEUED
          && pending.status() != BrokerPartitionChangeStatus.SUPERSEDING
          && state.getBrokerTopologyChangeState().ownsPendingPlan(pending)
          && pending.execution().generation() > 0
          && pending.hasMoreOperations()) {
        final var operation = pending.nextOperation();
        final var target = registrationsById.get(operation.targetBroker());
        if (operation.type() == TopologyOperationType.JOIN
            && operation.configuration() != null
            && operation.configuration().expectedRecoveryEpoch() == placement.recoveryEpoch()
            && target != null
            && target.status() == BrokerStatus.ONLINE
            && !state
                .getBrokerRegistrationState()
                .isLearnerExcluded(clusterId, target.brokerId(), target.incarnationId())
            && !placement.currentVoters().contains(operation.targetBroker())
            && !learners.contains(operation.targetBroker())) {
          // This is a durable bootstrap assignment, not an acknowledgement of Raft membership.
          // The target must receive replication even when the leader's JOIN receipt is delayed.
          // Publishing only after ACK would leave it without a local Raft group on that path.
          learners.add(operation.targetBroker());
          priorities.put(operation.targetBroker(), PartitionTopology.LEARNER_PRIORITY);
        }
      }
      partitions.add(
          new Partition(
              currentPartitionId,
              new Partition.Leadership(leaderId, leadership.term()),
              new Partition.Membership(
                  placement.currentVoters().stream()
                      .filter(voter -> !voter.equals(leaderId))
                      .toList(),
                  learners,
                  priorities,
                  placement.recoveryEpoch()),
              placement.status(),
              !leaderId.isBlank() && !destructiveChange));
    }
    return new BrokerTopologySnapshot(
        config.clusterSize(),
        new ClusterTopology(
            clusterId,
            state.getVersion(),
            config.partitionSize(),
            config.replicationFactor(),
            partitions,
            brokers,
            updatedAt),
        brokers.keySet().stream().map(registrationsById::get).toList());
  }
}
