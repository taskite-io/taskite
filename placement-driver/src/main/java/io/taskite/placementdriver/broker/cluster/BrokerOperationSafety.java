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
import io.taskite.placementdriver.broker.topology.ReplicaLifecycleState;
import io.taskite.placementdriver.broker.topology.ReplicaState;
import io.taskite.topology.MemberRole;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Pure readiness and quorum rules used before executing a topology operation. */
public final class BrokerOperationSafety {
  private BrokerOperationSafety() {}

  public static boolean leavePreservesQuorum(
      final PartitionTopology topology,
      final String leavingBroker,
      final Set<String> availableBrokers) {
    final Set<String> remainingVoters =
        topology.currentVoters().stream()
            .filter(brokerId -> !brokerId.equals(leavingBroker))
            .collect(Collectors.toCollection(LinkedHashSet::new));
    final int currentQuorum = topology.currentVoters().size() / 2 + 1;
    final int targetQuorum = topology.replicationFactor() / 2 + 1;
    if (remainingVoters.size() < currentQuorum) {
      return false;
    }
    final long activeRemaining =
        remainingVoters.stream()
            .filter(availableBrokers::contains)
            .map(topology.replicaStates()::get)
            .filter(Objects::nonNull)
            .filter(replica -> replica.lifecycle() == ReplicaLifecycleState.ACTIVE)
            .count();
    final long readyTarget =
        topology.desiredVoters().stream()
            .filter(remainingVoters::contains)
            .filter(availableBrokers::contains)
            .map(topology.replicaStates()::get)
            .filter(Objects::nonNull)
            .filter(replica -> replica.lifecycle() == ReplicaLifecycleState.ACTIVE)
            .count();
    return activeRemaining >= currentQuorum && readyTarget >= targetQuorum;
  }

  public static boolean replicaReadyForPromotion(
      final PartitionTopology topology, final String brokerId) {
    final var candidate = topology.replicaStates().get(brokerId);
    if (candidate == null) {
      return false;
    }
    if (candidate.role() != MemberRole.LEARNER) {
      return candidate.lifecycle() == ReplicaLifecycleState.ACTIVE;
    }
    if (!candidate.readyForPromotion()) {
      return false;
    }
    if (candidate.lifecycle() != ReplicaLifecycleState.ACTIVE
        && candidate.lifecycle() != ReplicaLifecycleState.CATCHING_UP) {
      return false;
    }
    final long voterHighWatermark =
        topology.currentVoters().stream()
            .filter(voter -> !voter.equals(brokerId))
            .map(topology.replicaStates()::get)
            .filter(Objects::nonNull)
            .mapToLong(ReplicaState::matchIndex)
            .max()
            .orElse(0);
    return candidate.lag() == 0 && candidate.matchIndex() >= voterHighWatermark;
  }
}
