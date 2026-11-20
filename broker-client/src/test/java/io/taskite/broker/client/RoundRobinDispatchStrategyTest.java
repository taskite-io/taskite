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

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.ClusterTopology;
import io.taskite.topology.Partition;
import io.taskite.topology.PartitionStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class RoundRobinDispatchStrategyTest {

  @Test
  void rotatesEvenlyBetweenReadyPartitionsWithGapsAndUnavailableLeaders() {
    final var strategy = new RoundRobinDispatchStrategy();
    final var topology =
        topology(
            List.of(
                partition(1, "broker-1", PartitionStatus.ACTIVE),
                partition(7, "broker-2", PartitionStatus.CREATING),
                partition(9, "broker-3", PartitionStatus.ACTIVE),
                partition(11, "missing", PartitionStatus.ACTIVE)));
    for (int index = 0; index < 12; index++) {
      assertEquals(index % 2 == 0 ? 1 : 9, strategy.determinePartition(topology));
    }
  }

  @Test
  void skipsPartitionsThatAreNotActiveOrHaveNoLeader() {
    final RoundRobinDispatchStrategy strategy = new RoundRobinDispatchStrategy();
    final ClusterTopology topology =
        topology(
            List.of(
                partition(1, "broker-1", PartitionStatus.REBALANCING),
                partition(2, "", PartitionStatus.ACTIVE),
                partition(3, "broker-3", PartitionStatus.ACTIVE)));

    assertEquals(3, strategy.determinePartition(topology));
    assertEquals(3, strategy.determinePartition(topology));
  }

  @Test
  void returnsNoPartitionWhenEveryPartitionIsNotReady() {
    final RoundRobinDispatchStrategy strategy = new RoundRobinDispatchStrategy();
    final ClusterTopology topology =
        topology(
            List.of(
                partition(1, "broker-1", PartitionStatus.CREATING),
                partition(2, "broker-2", PartitionStatus.REBALANCING)));

    assertEquals(-1, strategy.determinePartition(topology));
  }

  @Test
  void routesRebalancingPartitionWithExplicitServiceReadiness() {
    final var original = partition(1, "broker-1", PartitionStatus.REBALANCING);
    final var serving =
        new Partition(1, original.leadership(), original.membership(), original.status(), true);
    assertEquals(
        1, new RoundRobinDispatchStrategy().determinePartition(topology(List.of(serving))));
  }

  @Test
  void handlesCounterOverflowWithoutSkippingTheOnlyReadyPartition() throws Exception {
    final var strategy = new RoundRobinDispatchStrategy();
    final var field = RoundRobinDispatchStrategy.class.getDeclaredField("partitions");
    field.setAccessible(true);
    ((AtomicInteger) field.get(strategy)).set(Integer.MAX_VALUE - 1);
    final var topology =
        topology(
            List.of(
                partition(1, "broker-1", PartitionStatus.CREATING),
                partition(2, "broker-2", PartitionStatus.REBALANCING),
                partition(3, "broker-3", PartitionStatus.ACTIVE)));
    for (int i = 0; i < 20; i++) {
      assertEquals(3, strategy.determinePartition(topology));
    }
  }

  @Test
  void concurrentRequestsEachScanEveryCandidate() throws Exception {
    final var strategy = new RoundRobinDispatchStrategy();
    final var topology =
        topology(
            List.of(
                partition(1, "broker-1", PartitionStatus.CREATING),
                partition(2, "broker-2", PartitionStatus.REBALANCING),
                partition(3, "broker-3", PartitionStatus.ACTIVE)));
    try (var executor = Executors.newFixedThreadPool(8)) {
      final var results = new ArrayList<Future<Integer>>();
      for (int i = 0; i < 1000; i++) {
        results.add(executor.submit(() -> strategy.determinePartition(topology)));
      }
      for (final var result : results) assertEquals(3, result.get());
    }
  }

  private static ClusterTopology topology(final List<Partition> partitions) {
    return new ClusterTopology(
        "demo",
        1,
        partitions.size(),
        1,
        partitions,
        Map.of(
            "broker-1", new BrokerEndpoint("broker-1", "127.0.0.1", 26501, 26601),
            "broker-2", new BrokerEndpoint("broker-2", "127.0.0.1", 26502, 26602),
            "broker-3", new BrokerEndpoint("broker-3", "127.0.0.1", 26503, 26603)),
        Instant.EPOCH);
  }

  private static Partition partition(
      final int partitionId, final String leaderId, final PartitionStatus status) {
    return new Partition(
        partitionId,
        new Partition.Leadership(leaderId, 1),
        new Partition.Membership(List.of("broker-" + partitionId), List.of(), Map.of(), 0),
        status);
  }
}
