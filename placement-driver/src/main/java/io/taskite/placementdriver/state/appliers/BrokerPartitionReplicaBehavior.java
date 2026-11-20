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

import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.broker.topology.ReplicaState;
import io.taskite.placementdriver.common.mapper.BrokerPartitionStateMapper;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** Replaces replica observations using one complete Broker registration report. */
final class BrokerPartitionReplicaBehavior {
  private BrokerPartitionReplicaBehavior() {}

  static void observe(
      final MutableProcessingState state,
      final String clusterId,
      final String brokerId,
      final BrokerRegistrationRecord record,
      final Instant reportTime) {
    if (record.isPreservePartitionObservations()) {
      return;
    }
    // REGISTER and normal RENEW carry a complete report. Missing partitions invalidate prior
    // observations; in particular a new incarnation's empty REGISTER cannot revive old receipts.
    final Map<Integer, ReplicaState> observations = new LinkedHashMap<>();
    for (final var reported : BrokerPartitionStateMapper.fromRecord(record, reportTime)) {
      observations.put(
          reported.partitionId(), ReplicaState.observed(reported, reportTime.toEpochMilli()));
    }
    for (final var topology :
        state.getBrokerPartitionTopologyState().partitionTopologies(clusterId)) {
      final var previous = topology.replicaStates().get(brokerId);
      final var observed = observations.get(topology.partitionId());
      if (previous == null && observed == null) {
        continue;
      }
      final Map<String, ReplicaState> replicas = new LinkedHashMap<>(topology.replicaStates());
      if (observed == null) {
        replicas.remove(brokerId);
      } else {
        replicas.put(brokerId, observed);
      }
      state.getBrokerPartitionTopologyState().putPartitionTopology(withStates(topology, replicas));
      // Progress and receipts remain available to the plan executor without restarting placement.
      if (previous == null
          || observed == null
          || previous.role() != observed.role()
          || previous.lifecycle() != observed.lifecycle()
          || previous.priority() != observed.priority()
          || previous.recoveryEpoch() != observed.recoveryEpoch()) {
        state
            .getBrokerTopologyCoordinatorState()
            .markPartition(clusterId, topology.partitionId(), reportTime.toEpochMilli());
      }
    }
  }

  private static PartitionTopology withStates(
      final PartitionTopology topology, final Map<String, ReplicaState> states) {
    return new PartitionTopology(
        new PartitionTopology.Identity(topology.clusterId(), topology.partitionId()),
        new PartitionTopology.Placement(
            topology.replicationFactor(), topology.desiredVoters(), topology.desiredPrimary()),
        new PartitionTopology.Membership(
            topology.currentVoters(),
            topology.learners(),
            topology.priorities(),
            topology.recoveryEpoch()),
        topology.status(),
        states,
        topology.initializing());
  }
}
