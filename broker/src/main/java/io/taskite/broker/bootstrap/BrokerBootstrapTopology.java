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
package io.taskite.broker.bootstrap;

import io.taskite.broker.BrokerClusterConfig;
import io.taskite.broker.BrokerConfig;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.ClusterTopology;
import io.taskite.topology.Partition;
import io.taskite.topology.PartitionStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class BrokerBootstrapTopology {
  private BrokerBootstrapTopology() {}

  public static ClusterTopology bootstrapTopology(
      final BrokerConfig config, final BrokerClusterConfig clusterConfig) {
    return firstBrokerTopology(config, clusterConfig);
  }

  private static ClusterTopology firstBrokerTopology(
      final BrokerConfig config, final BrokerClusterConfig clusterConfig) {
    final Map<String, BrokerEndpoint> brokers =
        Map.of(
            config.brokerId(),
            new BrokerEndpoint(
                config.brokerId(),
                config.network().host(),
                config.network().commandPort(),
                config.network().raftPort()));
    final List<Partition> partitions = new ArrayList<>();
    for (int partitionId = 1; partitionId <= clusterConfig.partitionSize(); partitionId++) {
      partitions.add(
          new Partition(
              partitionId,
              new Partition.Leadership(config.brokerId(), 0),
              new Partition.Membership(List.of(), List.of(), Map.of(), 0),
              PartitionStatus.ACTIVE));
    }
    return new ClusterTopology(
        config.clusterId(),
        0,
        clusterConfig.partitionSize(),
        clusterConfig.replicationFactor(),
        partitions,
        brokers,
        Instant.now());
  }

  /**
   * Adapts the PD membership for local startup. Learner preparation is authorized by the PD's
   * admitted JOIN plan; registration alone does not assign this Broker to any partition.
   */
  public static ClusterTopology bootstrapTopology(
      final BrokerConfig config,
      final BrokerClusterConfig clusterConfig,
      final ClusterTopology persistedTopology) {
    Objects.requireNonNull(persistedTopology, "persistedTopology");
    if (!config.clusterId().equals(persistedTopology.clusterId())) {
      throw new IllegalArgumentException(
          "Bootstrap topology belongs to another cluster: " + persistedTopology.clusterId());
    }
    final Map<String, BrokerEndpoint> brokers = new LinkedHashMap<>(persistedTopology.brokers());
    final List<Partition> partitions = new ArrayList<>();
    for (int partitionId = 1; partitionId <= clusterConfig.partitionSize(); partitionId++) {
      final Partition persisted = persistedTopology.partition(partitionId).orElse(null);
      if (persisted == null) {
        continue;
      }
      // Newly configured partitions are created by ADD on the selected broker. Empty placement
      // must not cause every concurrently starting broker to bootstrap an isolated voter.
      if (persisted.voters().isEmpty()) {
        if (persisted.status() != PartitionStatus.CREATING) {
          partitions.add(persisted);
        }
        continue;
      }
      partitions.add(persisted);
    }
    return new ClusterTopology(
        config.clusterId(),
        persistedTopology.version(),
        clusterConfig.partitionSize(),
        clusterConfig.replicationFactor(),
        partitions,
        brokers,
        persistedTopology.updatedAt());
  }
}
