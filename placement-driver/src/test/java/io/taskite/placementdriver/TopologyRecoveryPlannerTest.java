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

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService;
import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.broker.topology.ReplicaState;
import io.taskite.topology.BrokerPartitionElectionState;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class TopologyRecoveryPlannerTest {
  private static final BrokerClusterChangePlanningService PLANNING_SERVICE =
      new BrokerClusterChangePlanningService();

  @Test
  void promotesReadyExistingLearnerBeforeCreatingAnotherReplica() {
    final PartitionTopology topology =
        new PartitionTopology(
            new PartitionTopology.Identity("demo", 1),
            new PartitionTopology.Placement(2, List.of("broker-0", "broker-1"), "broker-0"),
            new PartitionTopology.Membership(
                List.of("broker-0", "broker-1"), List.of("broker-2", "broker-3"), Map.of(), 0),
            PartitionStatus.ACTIVE,
            Map.of(
                "broker-2", replica(false, 0),
                "broker-3", replica(true, 5)));

    final var operations =
        PLANNING_SERVICE.calculateBrokerRecovery(
            topology,
            "broker-1",
            List.of(
                registration("broker-0", BrokerStatus.ONLINE),
                registration("broker-1", BrokerStatus.OFFLINE),
                registration("broker-2", BrokerStatus.ONLINE),
                registration("broker-3", BrokerStatus.ONLINE),
                registration("broker-4", BrokerStatus.ONLINE)));

    assertEquals(
        List.of(
            TopologyOperationType.JOIN,
            TopologyOperationType.PROMOTE_LEARNER,
            TopologyOperationType.LEAVE),
        operations.stream().map(operation -> operation.type()).toList());
    assertEquals("broker-3", operations.get(0).targetBroker());
    assertEquals("broker-3", operations.get(1).targetBroker());
    assertEquals("broker-1", operations.get(2).targetBroker());
  }

  @Test
  void joinsLowestLagOnlineBrokerWhenNoLearnerExists() {
    final PartitionTopology topology =
        new PartitionTopology(
            new PartitionTopology.Identity("demo", 1),
            new PartitionTopology.Placement(2, List.of("broker-0", "broker-1"), "broker-0"),
            new PartitionTopology.Membership(
                List.of("broker-0", "broker-1"), List.of(), Map.of(), 0),
            PartitionStatus.ACTIVE,
            Map.of("broker-2", replica(true, 20), "broker-3", replica(true, 3)));

    final var operations =
        PLANNING_SERVICE.calculateBrokerRecovery(
            topology,
            "broker-1",
            List.of(
                registration("broker-0", BrokerStatus.ONLINE),
                registration("broker-1", BrokerStatus.OFFLINE),
                registration("broker-2", BrokerStatus.ONLINE),
                registration("broker-3", BrokerStatus.ONLINE)));

    assertEquals(TopologyOperationType.JOIN, operations.get(0).type());
    assertEquals("broker-3", operations.get(0).targetBroker());
    assertEquals(TopologyOperationType.PROMOTE_LEARNER, operations.get(1).type());
    assertEquals("broker-3", operations.get(1).targetBroker());
  }

  private static BrokerRegistration registration(final String brokerId, final BrokerStatus status) {
    return new BrokerRegistration(
        new BrokerRegistration.Identity("demo", brokerId, "test", ""),
        new BrokerRegistration.Endpoint("127.0.0.1", 26501, 26601),
        new BrokerRegistration.Lease(
            Instant.EPOCH, Instant.EPOCH, BrokerRegistration.DEFAULT_RENEWAL_TIMEOUT_MILLIS),
        status,
        List.of());
  }

  private static ReplicaState replica(final boolean ready, final long lag) {
    return new ReplicaState(
        new BrokerPartitionState.Status(
            MemberRole.LEARNER, 0, BrokerPartitionLifecycle.CATCHING_UP, 0, 0),
        new BrokerPartitionState.Replication(10, lag, ready),
        new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
        1,
        BrokerPartitionElectionState.UNKNOWN);
  }
}
