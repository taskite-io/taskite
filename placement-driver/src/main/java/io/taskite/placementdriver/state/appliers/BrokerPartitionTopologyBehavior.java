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

import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.topology.BrokerTopologyPlanner;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.topology.PartitionStatus;
import java.util.List;

/** Materializes initial partition placement after the first Broker joins the voter roster. */
final class BrokerPartitionTopologyBehavior {
  private BrokerPartitionTopologyBehavior() {}

  static void rematerialize(final MutableProcessingState state, final String clusterId) {
    final BrokerClusterConfig config =
        state.getBrokerClusterConfigState().findClusterConfig(clusterId).orElse(null);
    if (config == null || config.voterRoster().isEmpty()) {
      return;
    }
    final List<BrokerRegistration> registrations =
        BrokerTopologyPlanner.eligibleRegistrations(state.getBrokerRegistrationState(), clusterId);
    for (final PartitionTopology partition :
        BrokerTopologyPlanner.plan(clusterId, config, registrations)) {
      final boolean explicitlyManaged =
          state
              .getBrokerTopologyChangeState()
              .hasChangePlanForPartition(clusterId, partition.partitionId());
      final boolean changeInProgress =
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology(clusterId, partition.partitionId())
              .map(existing -> existing.status() != PartitionStatus.ACTIVE)
              .orElse(false);
      if (!explicitlyManaged && !changeInProgress) {
        final var existing =
            state
                .getBrokerPartitionTopologyState()
                .partitionTopology(clusterId, partition.partitionId());
        state
            .getBrokerPartitionTopologyState()
            .putPartitionTopology(
                partition.withInitializing(
                    existing.map(PartitionTopology::initializing).orElse(true)));
      }
    }
  }
}
