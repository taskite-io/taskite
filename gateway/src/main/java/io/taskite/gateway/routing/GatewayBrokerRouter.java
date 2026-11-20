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
package io.taskite.gateway.routing;

import io.taskite.broker.client.RequestDispatchStrategy;
import io.taskite.broker.client.request.BrokerRequest;
import io.taskite.broker.client.response.PartitionNotFoundException;
import io.taskite.broker.client.response.PartitionNotReadyException;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.Partition;
import java.util.Objects;

/** Resolves a Broker request to a partition leader and fenced recovery generation. */
public final class GatewayBrokerRouter {
  private final GatewayTopologyCache topologyCache;
  private final RequestDispatchStrategy defaultDispatchStrategy =
      RequestDispatchStrategy.roundRobin();

  public GatewayBrokerRouter(final GatewayTopologyCache topologyCache) {
    this.topologyCache = Objects.requireNonNull(topologyCache, "topologyCache");
  }

  public Route resolve(final String clusterId, final BrokerRequest<?> request) {
    final int partitionId;
    if (request.addressesSpecificPartition()) {
      partitionId = request.getPartitionId();
    } else {
      final RequestDispatchStrategy strategy =
          request.requestDispatchStrategy().orElse(defaultDispatchStrategy);
      partitionId = strategy.determinePartition(topologyCache.current(clusterId));
    }
    if (partitionId < 0) {
      throw new PartitionNotReadyException();
    }
    return routeFor(clusterId, partitionId);
  }

  public BrokerEndpoint endpointFor(final String clusterId, final int partitionId) {
    return routeFor(clusterId, partitionId).endpoint();
  }

  public Route routeFor(final String clusterId, final int partitionId) {
    final var topology = topologyCache.current(clusterId);
    final Partition partition =
        topology
            .partition(partitionId)
            .orElseThrow(() -> new PartitionNotFoundException(partitionId));
    if (!partition.serviceReady()) {
      throw new PartitionNotReadyException(partitionId, partition.status());
    }
    final BrokerEndpoint endpoint =
        topology
            .leaderEndpoint(partitionId)
            .orElseThrow(() -> new PartitionNotFoundException(partitionId));
    return new Route(partitionId, endpoint, partition.recoveryEpoch());
  }

  public record Route(int partitionId, BrokerEndpoint endpoint, long routingEpoch) {}
}
