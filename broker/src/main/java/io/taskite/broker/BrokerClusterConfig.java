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
package io.taskite.broker;

import io.taskite.placementdriver.client.response.ClusterConfig;
import io.taskite.protocol.Protocol;
import java.util.Objects;

/** Cluster configuration resolved from PD and consumed by Broker runtime initialization. */
public record BrokerClusterConfig(
    String clusterId, long clusterKey, int clusterSize, int partitionSize, int replicationFactor) {

  public BrokerClusterConfig {
    Objects.requireNonNull(clusterId, "clusterId");
    Protocol.validateClusterKey(clusterKey);
    if (clusterKey == Protocol.PLACEMENT_DRIVER_CLUSTER_KEY) {
      throw new IllegalArgumentException("clusterKey 0 is reserved for placement-driver");
    }
    if (clusterSize <= 0
        || partitionSize <= 0
        || partitionSize > Protocol.MAXIMUM_PARTITIONS
        || replicationFactor <= 0
        || replicationFactor > clusterSize) {
      throw new IllegalArgumentException(
          "placement-driver returned an invalid cluster configuration");
    }
  }

  public static BrokerClusterConfig from(final BrokerConfig local, final ClusterConfig resolved) {
    if (resolved == null || !local.clusterId().equals(resolved.clusterId())) {
      throw new IllegalArgumentException("placement-driver returned a mismatched clusterId");
    }
    return new BrokerClusterConfig(
        resolved.clusterId(),
        resolved.clusterKey(),
        resolved.clusterSize(),
        resolved.partitionSize(),
        resolved.replicationFactor());
  }
}
