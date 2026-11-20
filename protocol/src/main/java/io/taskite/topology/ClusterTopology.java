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
package io.taskite.topology;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public record ClusterTopology(
    String clusterId,
    long version,
    int partitionSize,
    int replicationFactor,
    List<Partition> partitions,
    Map<String, BrokerEndpoint> brokers,
    Instant updatedAt) {
  public ClusterTopology {
    partitions = List.copyOf(partitions);
    brokers = Map.copyOf(brokers);
  }

  public Optional<Partition> partition(int partitionId) {
    return partitions.stream()
        .filter(partition -> partition.partitionId() == partitionId)
        .findFirst();
  }

  public Optional<BrokerEndpoint> leaderEndpoint(int partitionId) {
    return partition(partitionId).map(Partition::leaderId).map(brokers::get);
  }
}
