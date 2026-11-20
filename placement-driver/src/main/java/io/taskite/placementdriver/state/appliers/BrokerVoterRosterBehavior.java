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

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.BrokerTopologyPlanner;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;

/** Shared Voter-roster behavior invoked within registration event transactions. */
final class BrokerVoterRosterBehavior {
  private final MutableProcessingState state;
  private final BrokerClusterChangePlanningService planningService =
      new BrokerClusterChangePlanningService();

  BrokerVoterRosterBehavior(final MutableProcessingState state) {
    this.state = state;
  }

  void reconcileVoterRoster(final String clusterId, final long timestamp) {
    final var config =
        state.getBrokerClusterConfigState().findClusterConfig(clusterId).orElse(null);
    if (config == null) {
      return;
    }
    final var registrationState = state.getBrokerRegistrationState();
    final var registrations =
        BrokerTopologyPlanner.eligibleRegistrations(registrationState, clusterId);
    if (registrations.isEmpty()) {
      return;
    }
    final var ordinary =
        registrations.stream()
            .filter(
                registration ->
                    !registrationState.isVoterExcluded(clusterId, registration.brokerId()))
            .toList();
    final var partitions = state.getBrokerPartitionTopologyState().partitionTopologies(clusterId);
    var nextRoster =
        planningService.calculateVoterRoster(
            config.voterRoster(), config.clusterSize(), ordinary, partitions);
    final var onlineIds =
        ordinary.stream()
            .filter(registration -> registration.status() == BrokerStatus.ONLINE)
            .map(registration -> registration.brokerId())
            .toList();
    if (nextRoster.stream().filter(onlineIds::contains).count() < config.clusterSize()) {
      // Preserve ordinary spare priority and sticky slots. Only a target vacancy admits a
      // previously removed broker's new startup; eligibleRegistrations retains the startup fence.
      nextRoster =
          planningService.calculateVoterRoster(
              nextRoster, config.clusterSize(), registrations, partitions);
      for (final var registration : registrations) {
        if (registration.status() == BrokerStatus.ONLINE
            && nextRoster.contains(registration.brokerId())
            && registrationState.isVoterExcluded(clusterId, registration.brokerId())) {
          // Admission and roster replacement are persisted by the same lifecycle event transaction.
          registrationState.includeInPlacement(clusterId, registration.brokerId());
          state.getBrokerTopologyCoordinatorState().markCluster(clusterId, timestamp);
        }
      }
    }
    if (!nextRoster.equals(config.voterRoster())) {
      state.getBrokerClusterConfigState().updateVoterRoster(clusterId, nextRoster);
      state.getBrokerTopologyCoordinatorState().markCluster(clusterId, timestamp);
    }
  }
}
