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
package io.taskite.placementdriver.broker.topology;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.ClusterTopology;
import io.taskite.topology.Partition;
import io.taskite.topology.PartitionStatus;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class BrokerTopologyWatchPublisherTest {

  @Test
  void publishesChangedTopologyAndClosesClusterSubscribers() throws Exception {
    final BrokerTopologyWatchPublisher publisher = new BrokerTopologyWatchPublisher();
    final AtomicInteger updates = new AtomicInteger();
    final AtomicInteger unavailable = new AtomicInteger();
    final AutoCloseable subscription =
        publisher.subscribeToBrokerTopology(
            "demo",
            new BrokerTopologyWatchListener() {
              @Override
              public void onBrokerTopology(final ClusterTopology topology) {
                updates.incrementAndGet();
              }

              @Override
              public void onBrokerTopologyUnavailable(final String clusterId) {
                unavailable.incrementAndGet();
              }
            });

    publisher.publishBrokerTopology(topology(1));
    publisher.publishBrokerTopology(topology(1));
    publisher.publishBrokerTopology(topology(2));
    publisher.publishBrokerTopology(topology(2, 1));
    publisher.closeBrokerTopology("demo");
    subscription.close();

    assertEquals(3, updates.get());
    assertEquals(1, unavailable.get());
  }

  private static ClusterTopology topology(final long term) {
    return topology(term, 0);
  }

  private static ClusterTopology topology(final long term, final long recoveryEpoch) {
    return new ClusterTopology(
        "demo",
        term,
        1,
        1,
        List.of(
            new Partition(
                1,
                new Partition.Leadership("broker-0", term),
                new Partition.Membership(
                    List.of(), List.of(), Map.of("broker-0", 100), recoveryEpoch),
                PartitionStatus.ACTIVE)),
        Map.of("broker-0", new BrokerEndpoint("broker-0", "127.0.0.1", 26501, 26601)),
        Instant.EPOCH);
  }
}
