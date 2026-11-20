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
package io.taskite.placementdriver.client.mapper;

import io.taskite.placementdriver.client.response.ClusterConfig;
import io.taskite.placementdriver.protocol.BrokerTopology;
import io.taskite.placementdriver.protocol.GetClusterConfigResponse;
import io.taskite.protocol.Protocol;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.ClusterTopology;
import io.taskite.topology.Partition;
import io.taskite.topology.PartitionStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class PlacementDriverResponseMapper {
  private static final String MISSING_CLUSTER_KEY =
      "PD returned configuration for cluster '%s' without clusterKey";
  private static final String RESERVED_CLUSTER_KEY =
      "PD returned configuration for Broker cluster '%s' with reserved clusterKey 0";

  private PlacementDriverResponseMapper() {}

  public static ClusterConfig toClusterConfig(final GetClusterConfigResponse response) {
    if (!response.hasClusterKey()) {
      throw new IllegalArgumentException(MISSING_CLUSTER_KEY.formatted(response.getClusterId()));
    }
    Protocol.validateClusterKey(response.getClusterKey());
    if (response.getClusterKey() == Protocol.PLACEMENT_DRIVER_CLUSTER_KEY) {
      throw new IllegalArgumentException(RESERVED_CLUSTER_KEY.formatted(response.getClusterId()));
    }
    return new ClusterConfig(
        response.getClusterId(),
        response.getClusterKey(),
        response.getClusterSize(),
        response.getPartitionSize(),
        response.getReplicationFactor(),
        response.getRemovedBrokersList());
  }

  public static ClusterTopology toClusterTopology(final BrokerTopology response) {
    final Map<String, BrokerEndpoint> brokers = new LinkedHashMap<>();
    response
        .getBrokersList()
        .forEach(
            broker ->
                brokers.put(
                    broker.getBrokerId(),
                    new BrokerEndpoint(
                        broker.getBrokerId(),
                        broker.getHost(),
                        broker.getPort(),
                        broker.getRaftPort())));
    final List<Partition> partitions = new ArrayList<>();
    response
        .getPartitionsList()
        .forEach(
            partition ->
                partitions.add(
                    new Partition(
                        partition.getPartitionId(),
                        new Partition.Leadership(partition.getLeaderId(), partition.getTerm()),
                        new Partition.Membership(
                            partition.getReplicasList(),
                            partition.getLearnersList(),
                            partition.getPrioritiesMap(),
                            partition.getRecoveryEpoch()),
                        status(partition.getStatus()),
                        partition.getServiceReady())));
    return new ClusterTopology(
        response.getClusterId(),
        response.getVersion(),
        response.getPartitionSize(),
        response.getReplicationFactor(),
        partitions,
        brokers,
        Instant.ofEpochMilli(response.getUpdatedAtEpochMillis()));
  }

  private static PartitionStatus status(final String value) {
    return value == null || value.isBlank()
        ? PartitionStatus.ACTIVE
        : PartitionStatus.valueOf(value);
  }
}
