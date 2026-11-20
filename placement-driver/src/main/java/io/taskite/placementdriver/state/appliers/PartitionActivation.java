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

import io.taskite.placementdriver.broker.cluster.PartitionChangeTransitions;
import io.taskite.placementdriver.broker.topology.BrokerTopologyPlanner;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.record.ValueType;
import java.util.HashSet;

/** Determines whether a creating partition has received every deployed resource. */
final class PartitionActivation {
  private PartitionActivation() {}

  static PartitionTopology finishBootstrap(
      final ProcessingState state, final PartitionTopology topology) {
    final var config =
        state.getBrokerClusterConfigState().findClusterConfig(topology.clusterId()).orElseThrow();
    final var online =
        BrokerTopologyPlanner.online(
            BrokerTopologyPlanner.eligibleRegistrations(
                state.getBrokerRegistrationState(), topology.clusterId()));
    final var voters = BrokerTopologyPlanner.voterRegistrations(config, online);
    if (voters.size() < config.clusterSize()) {
      return PartitionChangeTransitions.markActive(topology);
    }
    final var desired =
        BrokerTopologyPlanner.planPartition(
            topology.clusterId(), config, topology.partitionId(), online, voters);
    if (!new HashSet<>(topology.currentVoters()).equals(new HashSet<>(desired.desiredVoters()))
        || !new HashSet<>(topology.learners()).equals(new HashSet<>(desired.learners()))
        || !topology.desiredPrimary().equals(desired.desiredPrimary())
        || !topology.priorities().equals(desired.priorities())) {
      // A completed intermediate plan is valid progress, but not the final initial placement.
      return PartitionChangeTransitions.markActive(topology);
    }
    return PartitionChangeTransitions.finishBootstrap(topology);
  }

  static boolean redistributionComplete(
      final ProcessingState state, final String clusterId, final int partitionId) {
    final var distributionState = state.getCommandDistributionState();
    for (final var distribution : distributionState.distributions()) {
      if (distribution.getValueType() == ValueType.JOB_SUBSCRIPTION
          && distributionState.isTarget(distribution.getDistributionKey(), clusterId)
          && distributionState.isPending(
              distribution.getDistributionKey(), clusterId, partitionId)) {
        return false;
      }
    }
    return true;
  }
}
