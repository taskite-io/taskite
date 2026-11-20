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
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService.BrokerClusterConfigurationChange;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService.BrokerTopologyRebalance;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
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

final class TopologyScalePlannerTest {
  private static final BrokerClusterChangePlanningService PLANNING_SERVICE =
      new BrokerClusterChangePlanningService();

  @Test
  void retainsDesiredLearnerKnownOnlyThroughRegistrationReport() {
    final var current =
        new PartitionTopology(
            new PartitionTopology.Identity("demo", 1),
            new PartitionTopology.Placement(1, List.of("broker-0"), "broker-0"),
            PartitionTopology.Membership.initial(List.of("broker-0"), List.of(), "broker-0"),
            PartitionStatus.ACTIVE,
            Map.of());
    final var desired =
        new PartitionTopology(
            current.identity(),
            current.placement(),
            PartitionTopology.Membership.initial(
                List.of("broker-0"), List.of("broker-4"), "broker-0"),
            PartitionStatus.ACTIVE,
            Map.of());
    final var learnerReport =
        new BrokerRegistration(
            new BrokerRegistration.Identity("demo", "broker-4", "test", ""),
            new BrokerRegistration.Endpoint("127.0.0.1", 26501, 26502),
            new BrokerRegistration.Lease(Instant.EPOCH, Instant.EPOCH, 30_000),
            BrokerStatus.ONLINE,
            List.of(
                new BrokerPartitionState(
                    1,
                    new BrokerPartitionState.Status(
                        MemberRole.LEARNER, 1, BrokerPartitionLifecycle.CATCHING_UP, 0, 0),
                    new BrokerPartitionState.Replication(1, 0, true),
                    new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
                    BrokerPartitionElectionState.UNKNOWN)));

    final var retained =
        PLANNING_SERVICE.calculateConfigurationChange(
            new BrokerClusterConfigurationChange(
                current, desired, List.of(learnerReport), "broker-0"));
    assertEquals(List.of(TopologyOperationType.JOIN), operationTypes(retained.operations()));
    assertEquals("broker-4", retained.operations().getFirst().targetBroker());

    final var removed =
        PLANNING_SERVICE.calculateConfigurationChange(
            new BrokerClusterConfigurationChange(
                current, current, List.of(learnerReport), "broker-0"));
    assertEquals(
        List.of(TopologyOperationType.JOIN, TopologyOperationType.LEAVE),
        operationTypes(removed.operations()));
  }

  @Test
  void joinsOnlineBrokerMissingFromPartitionAsLearner() {
    final PartitionTopology topology =
        new PartitionTopology(
            new PartitionTopology.Identity("demo", 1),
            new PartitionTopology.Placement(2, List.of("broker-0", "broker-1"), "broker-0"),
            PartitionTopology.Membership.initial(
                List.of("broker-0", "broker-1"), List.of(), "broker-0"),
            PartitionStatus.ACTIVE,
            Map.of());

    final var operations = scaleUp(topology, "broker-2");

    assertEquals(1, operations.size());
    assertEquals(TopologyOperationType.JOIN, operations.getFirst().type());
    assertEquals("broker-2", operations.getFirst().targetBroker());
  }

  @Test
  void doesNothingWhenBrokerIsAlreadyAMember() {
    final PartitionTopology topology =
        new PartitionTopology(
            new PartitionTopology.Identity("demo", 1),
            new PartitionTopology.Placement(2, List.of("broker-0", "broker-1"), "broker-0"),
            PartitionTopology.Membership.initial(
                List.of("broker-0", "broker-1"), List.of("broker-2"), "broker-0"),
            PartitionStatus.ACTIVE,
            Map.of());

    assertEquals(List.of(), scaleUp(topology, "broker-2"));
  }

  @Test
  void promotesLearnerWhenPartitionIsUnderReplicated() {
    final PartitionTopology topology =
        new PartitionTopology(
            new PartitionTopology.Identity("demo", 1),
            new PartitionTopology.Placement(2, List.of("broker-0"), "broker-0"),
            PartitionTopology.Membership.initial(
                List.of("broker-0"), List.of("broker-2"), "broker-0"),
            PartitionStatus.ACTIVE,
            Map.of());

    final var operations = scaleUp(topology, "broker-2");

    assertEquals(
        List.of(TopologyOperationType.JOIN, TopologyOperationType.PROMOTE_LEARNER),
        operationTypes(operations));
  }

  @Test
  void retainsServingLeaderWhenItStillBelongsToDesiredVoters() {
    final PartitionTopology current =
        new PartitionTopology(
            new PartitionTopology.Identity("demo", 2),
            new PartitionTopology.Placement(
                3, List.of("broker-0", "broker-1", "broker-2"), "broker-0"),
            PartitionTopology.Membership.initial(
                List.of("broker-0", "broker-1", "broker-2"), List.of(), "broker-0"),
            PartitionStatus.ACTIVE,
            Map.of());
    final PartitionTopology desired =
        new PartitionTopology(
            new PartitionTopology.Identity("demo", 2),
            new PartitionTopology.Placement(
                3, List.of("broker-1", "broker-2", "broker-0"), "broker-1"),
            PartitionTopology.Membership.initial(
                List.of("broker-1", "broker-2", "broker-0"), List.of(), "broker-1"),
            PartitionStatus.ACTIVE,
            Map.of());

    final var operations = rebalance(current, desired, "broker-0");

    assertEquals(List.of(), operations);
  }

  @Test
  void movesOldVoterToLearnerAfterReplacementAndLeaderTransfer() {
    final PartitionTopology current =
        new PartitionTopology(
            new PartitionTopology.Identity("demo", 2),
            new PartitionTopology.Placement(2, List.of("broker-0", "broker-1"), "broker-0"),
            PartitionTopology.Membership.initial(
                List.of("broker-0", "broker-1"), List.of("broker-2"), "broker-0"),
            PartitionStatus.ACTIVE,
            Map.of());
    final PartitionTopology desired =
        new PartitionTopology(
            new PartitionTopology.Identity("demo", 2),
            new PartitionTopology.Placement(2, List.of("broker-1", "broker-2"), "broker-1"),
            PartitionTopology.Membership.initial(
                List.of("broker-1", "broker-2"), List.of("broker-0"), "broker-1"),
            PartitionStatus.ACTIVE,
            Map.of());

    final var operations = rebalance(current, desired, "broker-0");

    assertEquals(
        List.of(
            TopologyOperationType.JOIN,
            TopologyOperationType.PROMOTE_LEARNER,
            TopologyOperationType.RECONFIGURE_PRIORITY,
            TopologyOperationType.RECONFIGURE_PRIORITY,
            TopologyOperationType.TRANSFER_LEADER,
            TopologyOperationType.LEAVE,
            TopologyOperationType.JOIN),
        operations.stream().map(operation -> operation.type()).toList());
    assertEquals(
        List.of("broker-2", "broker-2", "broker-1", "broker-2", "broker-1", "broker-0", "broker-0"),
        operations.stream().map(operation -> operation.targetBroker()).toList());
  }

  @Test
  void doesNotRemoveVotersWhenOnlineTargetCannotRestoreReplicationFactor() {
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
            new PartitionTopology.Placement(3, List.of("broker-0"), "broker-0"),
            PartitionTopology.Membership.initial(List.of("broker-0"), List.of(), "broker-0"),
            PartitionStatus.ACTIVE,
            Map.of());

    assertEquals(List.of(), rebalance(current, desired, "broker-0"));
  }

  @Test
  void rejoinsDesiredLearnerWhenReplicationNeverStarts() {
    final PartitionTopology current =
        topologyWithStalledLearner(
            List.of("broker-0", "broker-2"), List.of("broker-1"), List.of("broker-0", "broker-2"));
    final PartitionTopology desired =
        new PartitionTopology(
            new PartitionTopology.Identity("demo", 3),
            new PartitionTopology.Placement(2, List.of("broker-0", "broker-2"), "broker-0"),
            PartitionTopology.Membership.initial(
                List.of("broker-0", "broker-2"), List.of("broker-1"), "broker-0"),
            PartitionStatus.ACTIVE,
            Map.of());

    final var operations = rebalance(current, desired, "broker-0");

    assertEquals(List.of(TopologyOperationType.JOIN), operationTypes(operations));
    assertEquals("broker-1", operations.getFirst().targetBroker());
  }

  @Test
  void rejoinsStalledLearnerBeforePromotion() {
    final PartitionTopology current =
        topologyWithStalledLearner(
            List.of("broker-0", "broker-2"),
            List.of("broker-1"),
            List.of("broker-0", "broker-1", "broker-2"));
    final PartitionTopology desired =
        new PartitionTopology(
            new PartitionTopology.Identity("demo", 3),
            new PartitionTopology.Placement(
                3, List.of("broker-0", "broker-1", "broker-2"), "broker-0"),
            PartitionTopology.Membership.initial(
                List.of("broker-0", "broker-1", "broker-2"), List.of(), "broker-0"),
            PartitionStatus.ACTIVE,
            Map.of());

    final var operations = rebalance(current, desired, "broker-0");

    assertEquals(TopologyOperationType.JOIN, operations.get(0).type());
    assertEquals(TopologyOperationType.PROMOTE_LEARNER, operations.get(1).type());
    assertEquals("broker-1", operations.get(0).targetBroker());
    assertEquals("broker-1", operations.get(1).targetBroker());
  }

  @Test
  void doesNotRejoinLearnerThatHasCaughtUp() {
    final PartitionTopology current =
        new PartitionTopology(
            new PartitionTopology.Identity("demo", 3),
            new PartitionTopology.Placement(2, List.of("broker-0", "broker-2"), "broker-0"),
            new PartitionTopology.Membership(
                List.of("broker-0", "broker-2"),
                List.of("broker-1"),
                Map.of("broker-0", 100, "broker-2", 10, "broker-1", 0),
                0),
            PartitionStatus.ACTIVE,
            Map.of(
                "broker-1",
                new ReplicaState(
                    new BrokerPartitionState.Status(
                        MemberRole.LEARNER, 0, BrokerPartitionLifecycle.CATCHING_UP, 0, 0),
                    new BrokerPartitionState.Replication(5, 0, true),
                    new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
                    1,
                    BrokerPartitionElectionState.UNKNOWN)));
    final PartitionTopology desired =
        new PartitionTopology(
            new PartitionTopology.Identity("demo", 3),
            new PartitionTopology.Placement(2, List.of("broker-0", "broker-2"), "broker-0"),
            PartitionTopology.Membership.initial(
                List.of("broker-0", "broker-2"), List.of("broker-1"), "broker-0"),
            PartitionStatus.ACTIVE,
            Map.of());

    assertEquals(List.of(), rebalance(current, desired, "broker-0"));
  }

  private static PartitionTopology topologyWithStalledLearner(
      final List<String> voters, final List<String> learners, final List<String> desiredVoters) {
    return new PartitionTopology(
        new PartitionTopology.Identity("demo", 3),
        new PartitionTopology.Placement(desiredVoters.size(), desiredVoters, "broker-0"),
        new PartitionTopology.Membership(
            voters, learners, Map.of("broker-0", 100, "broker-2", 10, "broker-1", 0), 0),
        PartitionStatus.ACTIVE,
        Map.of(
            "broker-1",
            new ReplicaState(
                new BrokerPartitionState.Status(
                    MemberRole.LEARNER, 0, BrokerPartitionLifecycle.CATCHING_UP, 0, 0),
                new BrokerPartitionState.Replication(0, 5, false),
                new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
                1,
                BrokerPartitionElectionState.UNKNOWN)));
  }

  private static List<TopologyOperationType> operationTypes(
      final List<TopologyOperation> operations) {
    return operations.stream().map(operation -> operation.type()).toList();
  }

  private static List<TopologyOperation> scaleUp(
      final PartitionTopology topology, final String brokerId) {
    return PLANNING_SERVICE.calculateBrokerScaleUp(topology, brokerId);
  }

  private static List<TopologyOperation> rebalance(
      final PartitionTopology current,
      final PartitionTopology desired,
      final String observedLeader) {
    return PLANNING_SERVICE
        .calculateTopologyRebalance(new BrokerTopologyRebalance(current, desired, observedLeader))
        .operations();
  }
}
