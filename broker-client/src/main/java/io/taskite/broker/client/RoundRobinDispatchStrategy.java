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
package io.taskite.broker.client;

import io.taskite.topology.ClusterTopology;
import io.taskite.topology.Partition;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Return the next partition using a round-robin strategy, but skips the partitions where there is
 * no leader at the moment.
 */
public final class RoundRobinDispatchStrategy implements RequestDispatchStrategy {

  private final AtomicInteger partitions = new AtomicInteger(0);

  @Override
  public int determinePartition(final ClusterTopology topology) {
    if (topology != null) {
      final var ready =
          topology.partitions().stream()
              .filter(Partition::serviceReady)
              .filter(partition -> topology.leaderEndpoint(partition.partitionId()).isPresent())
              .toList();
      if (!ready.isEmpty()) {
        return ready.get(Math.floorMod(partitions.getAndIncrement(), ready.size())).partitionId();
      }
    }

    return -1;
  }
}
