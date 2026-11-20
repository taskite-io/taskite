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

import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;

/** Deterministically removes drained registrations once no partition references the Broker. */
final class BrokerRemovalFinalizer {
  private BrokerRemovalFinalizer() {}

  static void finalizeRemovals(final MutableProcessingState state, final String clusterId) {
    final var topologies = state.getBrokerPartitionTopologyState().partitionTopologies(clusterId);
    state.getBrokerRegistrationState().registrations(clusterId).stream()
        .filter(registration -> registration.status() == BrokerStatus.DRAINING)
        .map(registration -> registration.brokerId())
        .filter(
            brokerId ->
                topologies.stream()
                    .noneMatch(
                        topology ->
                            topology.currentVoters().contains(brokerId)
                                || topology.desiredVoters().contains(brokerId)
                                || topology.learners().contains(brokerId)))
        .toList()
        .forEach(
            brokerId -> {
              state.getBrokerRegistrationState().removeBroker(clusterId, brokerId);
              state.getBrokerTopologyCoordinatorState().markCluster(clusterId);
            });
  }
}
