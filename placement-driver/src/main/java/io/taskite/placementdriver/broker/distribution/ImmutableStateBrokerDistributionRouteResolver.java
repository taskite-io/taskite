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
package io.taskite.placementdriver.broker.distribution;

import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.PartitionLeadershipResolver;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.topology.BrokerEndpoint;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ImmutableStateBrokerDistributionRouteResolver
    implements BrokerDistributionRouteResolver {
  private final ProcessingState state;

  public ImmutableStateBrokerDistributionRouteResolver(final ProcessingState state) {
    this.state = state;
  }

  @Override
  public BrokerDistributionRoute resolveBrokerDistributionRoute(final String clusterId) {
    final var configuration =
        state
            .getBrokerClusterConfigState()
            .findClusterConfig(clusterId)
            .orElseThrow(() -> new IllegalStateException("CLUSTER_NOT_REGISTERED: " + clusterId));
    final List<BrokerRegistration> routable =
        state.getBrokerRegistrationState().registrations(clusterId).stream()
            .filter(registration -> registration.status() != BrokerStatus.OFFLINE)
            .toList();
    final Map<String, BrokerEndpoint> endpoints = new LinkedHashMap<>();
    routable.forEach(
        registration ->
            endpoints.put(
                registration.brokerId(),
                new BrokerEndpoint(
                    registration.brokerId(),
                    registration.host(),
                    registration.port(),
                    registration.raftPort())));
    final Map<Integer, BrokerEndpoint> leaders = new LinkedHashMap<>();
    for (int partitionId = 1; partitionId <= configuration.partitionSize(); partitionId++) {
      final int currentPartitionId = partitionId;
      state
          .getBrokerPartitionTopologyState()
          .partitionTopology(clusterId, currentPartitionId)
          .map(
              topology ->
                  PartitionLeadershipResolver.resolve(
                          routable,
                          currentPartitionId,
                          topology.currentVoters(),
                          topology.recoveryEpoch())
                      .leaderId())
          .map(endpoints::get)
          .ifPresent(endpoint -> leaders.put(currentPartitionId, endpoint));
    }
    return new BrokerDistributionRoute(configuration.partitionSize(), Map.copyOf(leaders));
  }
}
