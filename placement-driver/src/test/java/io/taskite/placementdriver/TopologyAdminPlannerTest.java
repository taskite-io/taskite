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

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeIds;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService.BrokerClusterConfigurationChange;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class TopologyAdminPlannerTest {
  private static final BrokerClusterChangePlanningService PLANNING_SERVICE =
      new BrokerClusterChangePlanningService();

  @Test
  void replicationFactorDecreaseRemovesExcessVoter() {
    final PartitionTopology current =
        new PartitionTopology(
            new PartitionTopology.Identity("demo", 1),
            new PartitionTopology.Placement(
                3, List.of("broker-0", "broker-1", "broker-2"), "broker-0"),
            PartitionTopology.Membership.initial(
                List.of("broker-0", "broker-1", "broker-2"), List.of(), "broker-0"),
            PartitionStatus.ACTIVE,
            Map.of());
    final PartitionTopology desired =
        new PartitionTopology(
            new PartitionTopology.Identity("demo", 1),
            new PartitionTopology.Placement(2, List.of("broker-0", "broker-1"), "broker-0"),
            PartitionTopology.Membership.initial(
                List.of("broker-0", "broker-1"), List.of(), "broker-0"),
            PartitionStatus.ACTIVE,
            Map.of());

    final var operations =
        configuration(
            current,
            desired,
            List.of(registration("broker-0"), registration("broker-1"), registration("broker-2")),
            "broker-0");

    assertEquals(List.of(TopologyOperationType.LEAVE), operationTypes(operations));
    assertEquals("broker-2", operations.getFirst().targetBroker());
  }

  @Test
  void unselectedBootstrapLearnerIsJoinedThenRemoved() {
    final PartitionTopology current =
        new PartitionTopology(
            new PartitionTopology.Identity("demo", 1),
            new PartitionTopology.Placement(1, List.of("broker-0"), "broker-0"),
            PartitionTopology.Membership.initial(List.of("broker-0"), List.of(), "broker-0"),
            PartitionStatus.ACTIVE,
            Map.of());
    final BrokerRegistration extra =
        registration("broker-1", List.of(new BrokerPartitionState(1, MemberRole.LEARNER, 0)));

    final var operations =
        configuration(current, current, List.of(registration("broker-0"), extra), "broker-0");

    assertEquals(
        List.of(TopologyOperationType.JOIN, TopologyOperationType.LEAVE),
        operationTypes(operations));
    assertEquals(
        List.of("broker-1", "broker-1"),
        operations.stream().map(operation -> operation.targetBroker()).toList());
  }

  @Test
  void adminBatchKeyGroupsPartitionPlans() {
    final String first = BrokerClusterChangeIds.createAdminChangeId("rebalance", "batch-1", 1);
    final String second = BrokerClusterChangeIds.createAdminChangeId("rebalance", "batch-1", 2);

    assertEquals(
        BrokerClusterChangeIds.extractBatchKey(first),
        BrokerClusterChangeIds.extractBatchKey(second));
    assertEquals("", BrokerClusterChangeIds.extractBatchKey("quorum-recovery:broker-0:1:id"));
    assertEquals("batch-1", BrokerClusterChangeIds.extractOperationId(first));
    assertEquals(
        "quorum-recovery:broker-0:1:id",
        BrokerClusterChangeIds.extractOperationId("quorum-recovery:broker-0:1:id"));
  }

  private static BrokerRegistration registration(final String brokerId) {
    return registration(brokerId, List.of());
  }

  private static BrokerRegistration registration(
      final String brokerId, final List<BrokerPartitionState> partitions) {
    return new BrokerRegistration(
        new BrokerRegistration.Identity("demo", brokerId, "test", ""),
        new BrokerRegistration.Endpoint("127.0.0.1", 26501, 26601),
        new BrokerRegistration.Lease(
            Instant.EPOCH, Instant.EPOCH, BrokerRegistration.DEFAULT_RENEWAL_TIMEOUT_MILLIS),
        BrokerStatus.ONLINE,
        partitions);
  }

  private static List<TopologyOperationType> operationTypes(
      final List<TopologyOperation> operations) {
    return operations.stream().map(operation -> operation.type()).toList();
  }

  private static List<TopologyOperation> configuration(
      final PartitionTopology current,
      final PartitionTopology desired,
      final List<BrokerRegistration> registrations,
      final String observedLeader) {
    return PLANNING_SERVICE
        .calculateConfigurationChange(
            new BrokerClusterConfigurationChange(current, desired, registrations, observedLeader))
        .operations();
  }
}
