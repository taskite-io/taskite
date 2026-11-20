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
package io.taskite.placementdriver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.BrokerTopologyPlanner;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

final class BrokerTopologyPlannerTest {

  @Test
  void addPartitionUsesLearnerFirstOperationSequence() {
    final BrokerClusterConfig config = new BrokerClusterConfig(2, 1, 2);
    final List<BrokerRegistration> online =
        List.of(broker("broker-0"), broker("broker-1"), broker("broker-2"));
    final var placement =
        BrokerTopologyPlanner.planPartition(
            "demo", config, 2, online, BrokerTopologyPlanner.voterRegistrations(config, online));

    final var operations = BrokerTopologyPlanner.addPartitionOperations(placement);

    assertEquals(List.of("broker-1", "broker-0"), placement.desiredVoters());
    assertEquals(List.of("broker-2"), placement.learners());
    assertEquals(
        List.of(
            TopologyOperationType.ADD,
            TopologyOperationType.JOIN,
            TopologyOperationType.PROMOTE_LEARNER,
            TopologyOperationType.JOIN,
            TopologyOperationType.RECONFIGURE_PRIORITY,
            TopologyOperationType.RECONFIGURE_PRIORITY,
            TopologyOperationType.STEP_DOWN_IF_NOT_PRIMARY),
        operations.stream().map(operation -> operation.type()).toList());
    assertEquals(
        List.of("broker-1", "broker-0", "broker-0", "broker-2", "broker-1", "broker-0", "broker-1"),
        operations.stream().map(operation -> operation.targetBroker()).toList());
  }

  @Test
  void assignsOnlineSpareBrokersAsLearnersOnEveryPartition() {
    final BrokerClusterConfig config = new BrokerClusterConfig(3, 5, 3);
    final List<BrokerRegistration> online =
        List.of(broker("broker-0"), broker("broker-1"), broker("broker-2"), broker("broker-3"));

    final List<PartitionTopology> placements = BrokerTopologyPlanner.plan("demo", config, online);

    assertEquals(5, placements.size());
    for (final var placement : placements) {
      assertEquals(3, placement.desiredVoters().size());
      assertTrue(placement.desiredVoters().stream().noneMatch("broker-3"::equals));
      assertEquals(List.of("broker-3"), placement.learners());
      assertEquals(0, placement.priorities().get("broker-3"));
    }
  }

  @Test
  void keepsDrainingBrokerRoutableButIneligibleForPlacement() {
    final BrokerRegistration online = broker("broker-0", BrokerStatus.ONLINE);
    final BrokerRegistration draining = broker("broker-1", BrokerStatus.DRAINING);
    final BrokerRegistration offline = broker("broker-2", BrokerStatus.OFFLINE);

    assertEquals(List.of(online), BrokerTopologyPlanner.online(List.of(online, draining, offline)));
    assertEquals(
        List.of(online, draining),
        BrokerTopologyPlanner.routable(List.of(online, draining, offline)));
  }

  private static BrokerRegistration broker(final String brokerId) {
    return broker(brokerId, BrokerStatus.ONLINE);
  }

  @Test
  void singleVoterPlanSkipsUnsupportedPriorityOnlyChanges() {
    final BrokerClusterConfig config = new BrokerClusterConfig(1, 1, 1);
    final var online = List.of(broker("broker-0"), broker("broker-1"));
    final var placement =
        BrokerTopologyPlanner.planPartition(
            "demo", config, 1, online, BrokerTopologyPlanner.voterRegistrations(config, online));
    final var operations = BrokerTopologyPlanner.addPartitionOperations(placement);
    assertEquals(
        List.of(
            TopologyOperationType.ADD,
            TopologyOperationType.JOIN,
            TopologyOperationType.STEP_DOWN_IF_NOT_PRIMARY),
        operations.stream().map(operation -> operation.type()).toList());
  }

  private static BrokerRegistration broker(final String brokerId, final BrokerStatus status) {
    return new BrokerRegistration(
        new BrokerRegistration.Identity("demo", brokerId, "test", ""),
        new BrokerRegistration.Endpoint("127.0.0.1", 26501, 26601),
        new BrokerRegistration.Lease(
            Instant.EPOCH, Instant.EPOCH, BrokerRegistration.DEFAULT_RENEWAL_TIMEOUT_MILLIS),
        status,
        List.of());
  }
}
