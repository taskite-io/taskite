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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.broker.topology.QuorumRecoveryCoordinator;
import io.taskite.placementdriver.broker.topology.ReplicaState;
import io.taskite.topology.BrokerPartitionElectionState;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import io.taskite.topology.RaftConfigurationReference;
import io.taskite.topology.RaftNodeState;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

final class QuorumRecoveryCoordinatorTest {

  @Test
  void hasLostQuorumWhenOnlineVotersBelowQuorum() {
    final PartitionTopology topology = topology(List.of("broker-0", "broker-1", "broker-2"));
    final Map<String, BrokerRegistration> byBrokerId =
        brokers(
            Map.of(
                "broker-0", BrokerStatus.ONLINE,
                "broker-1", BrokerStatus.OFFLINE,
                "broker-2", BrokerStatus.OFFLINE));
    assertTrue(QuorumRecoveryCoordinator.hasLostQuorum(topology, byBrokerId));
  }

  @Test
  void retainsQuorumWhenMajorityOnline() {
    final PartitionTopology topology = topology(List.of("broker-0", "broker-1", "broker-2"));
    final Map<String, BrokerRegistration> byBrokerId =
        brokers(
            Map.of(
                "broker-0", BrokerStatus.ONLINE,
                "broker-1", BrokerStatus.ONLINE,
                "broker-2", BrokerStatus.OFFLINE));
    assertFalse(QuorumRecoveryCoordinator.hasLostQuorum(topology, byBrokerId));
  }

  @ParameterizedTest
  @CsvSource({
    "old, new-1, new-2, 1000, FOLLOWER, 0, true",
    "old, old, new-2, 1000, FOLLOWER, 0, false",
    "old, new-1, new-2, 999, FOLLOWER, 0, false",
    "old, new-1, new-2, 1000, LEADER, 0, false",
    "old, '', new-2, 1000, FOLLOWER, 0, false",
    "old, new-1, new-2, 1000, FOLLOWER, 54, false"
  })
  void quorumRecoveryRequiresFreshIncompatibleConfigurationsWithoutLeader(
      String first,
      String second,
      String third,
      long observed,
      String role,
      long secondMatch,
      boolean lost) {
    final var topology = topology(List.of("broker-0", "broker-1", "broker-2"));
    final var registrations = new LinkedHashMap<String, BrokerRegistration>();
    final var digests = List.of(first, second, third);
    for (int index = 0; index < 3; index++) {
      final var report =
          new BrokerPartitionState(
              3,
              new BrokerPartitionState.Status(
                  index == 0 ? MemberRole.parse(role) : MemberRole.FOLLOWER,
                  index == 0 ? 4 : 0,
                  BrokerPartitionLifecycle.ACTIVE,
                  100,
                  0),
              new BrokerPartitionState.Replication(
                  index == 0 ? 54 : index == 1 ? secondMatch : 0, 0, true),
              new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
              new BrokerPartitionElectionState(
                  new RaftConfigurationReference(index == 0 ? 54 : 0, digests.get(index)),
                  new BrokerPartitionElectionState.Priorities(100, 100),
                  new BrokerPartitionElectionState.Observation(
                      index == 0 && role.equals("LEADER")
                          ? RaftNodeState.STATE_LEADER
                          : RaftNodeState.STATE_FOLLOWER,
                      "",
                      false,
                      observed)));
      registrations.put(
          "broker-" + index,
          new BrokerRegistration(
              new BrokerRegistration.Identity("demo", "broker-" + index, "test", "startup"),
              new BrokerRegistration.Endpoint("127.0.0.1", 26501, 26502),
              new BrokerRegistration.Lease(
                  Instant.ofEpochMilli(500), Instant.ofEpochMilli(1000), 30_000),
              BrokerStatus.ONLINE,
              List.of(report)));
    }
    assertEquals(lost, QuorumRecoveryCoordinator.hasLostQuorum(topology, registrations));
  }

  @Test
  void chooseSurvivorPrefersHighestMatchIndexOnlineVoter() {
    final Map<String, ReplicaState> replicaStates = new LinkedHashMap<>();
    replicaStates.put("broker-0", replica(10));
    replicaStates.put("broker-1", replica(42));
    replicaStates.put("broker-2", replica(7));
    final PartitionTopology topology =
        topology(List.of("broker-0", "broker-1", "broker-2"), replicaStates);
    final Map<String, BrokerRegistration> byBrokerId =
        brokers(
            Map.of(
                "broker-0", BrokerStatus.ONLINE,
                "broker-1", BrokerStatus.OFFLINE,
                "broker-2", BrokerStatus.ONLINE));

    // broker-1 has the highest match index but is OFFLINE, so the most-complete ONLINE voter wins.
    assertEquals(
        Optional.of("broker-0"), QuorumRecoveryCoordinator.chooseSurvivor(topology, byBrokerId));
  }

  @Test
  void planResetProducesSingleFencedResetPeersOperation() {
    final List<TopologyOperation> operations =
        QuorumRecoveryCoordinator.planReset(3, "broker-0", 7);
    assertEquals(1, operations.size());
    final TopologyOperation op = operations.getFirst();
    assertEquals(TopologyOperationType.RESET_PEERS, op.type());
    assertEquals("broker-0", op.targetBroker());
    assertEquals(7, op.priority());
  }

  @Test
  void autoRecoveryIsDisabledByDefault() {
    final PartitionTopology topology = topology(List.of("broker-0", "broker-1", "broker-2"));
    final Map<String, BrokerRegistration> byBrokerId =
        brokers(
            Map.of(
                "broker-0", BrokerStatus.ONLINE,
                "broker-1", BrokerStatus.OFFLINE,
                "broker-2", BrokerStatus.OFFLINE));
    assertTrue(new QuorumRecoveryCoordinator().planRecovery(topology, byBrokerId).isEmpty());
  }

  @Test
  void enabledAutoRecoveryPlansAFencedReset() {
    final Map<String, ReplicaState> replicaStates = new LinkedHashMap<>();
    replicaStates.put("broker-0", replica(99));
    replicaStates.put("broker-1", replica(1));
    final PartitionTopology topology = topology(List.of("broker-0", "broker-1"), replicaStates);
    final Map<String, BrokerRegistration> byBrokerId =
        brokers(
            Map.of(
                "broker-0", BrokerStatus.ONLINE,
                "broker-1", BrokerStatus.OFFLINE));

    final Optional<List<TopologyOperation>> plan =
        new QuorumRecoveryCoordinator(true).planRecovery(topology, byBrokerId);

    assertTrue(plan.isPresent());
    assertEquals(1, plan.orElseThrow().size());
    final TopologyOperation op = plan.orElseThrow().getFirst();
    assertEquals(TopologyOperationType.RESET_PEERS, op.type());
    assertEquals("broker-0", op.targetBroker());
  }

  private static PartitionTopology topology(final List<String> voters) {
    return topology(voters, Map.of());
  }

  private static PartitionTopology topology(
      final List<String> voters, final Map<String, ReplicaState> replicaStates) {
    return new PartitionTopology(
        new PartitionTopology.Identity("demo", 3),
        new PartitionTopology.Placement(voters.size(), voters, voters.getFirst()),
        new PartitionTopology.Membership(voters, List.of(), Map.of(), 0),
        PartitionStatus.ACTIVE,
        replicaStates);
  }

  private static ReplicaState replica(final long matchIndex) {
    return new ReplicaState(
        new BrokerPartitionState.Status(
            MemberRole.FOLLOWER, 0, BrokerPartitionLifecycle.ACTIVE, 10, 0),
        new BrokerPartitionState.Replication(matchIndex, 0, true),
        new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
        0,
        BrokerPartitionElectionState.UNKNOWN);
  }

  private static Map<String, BrokerRegistration> brokers(final Map<String, BrokerStatus> statuses) {
    final Map<String, BrokerRegistration> byBrokerId = new LinkedHashMap<>();
    statuses.forEach(
        (brokerId, status) ->
            byBrokerId.put(
                brokerId,
                new BrokerRegistration(
                    new BrokerRegistration.Identity("demo", brokerId, "1.0", ""),
                    new BrokerRegistration.Endpoint("127.0.0.1", 26501, 26601),
                    new BrokerRegistration.Lease(
                        Instant.EPOCH,
                        Instant.EPOCH,
                        BrokerRegistration.DEFAULT_RENEWAL_TIMEOUT_MILLIS),
                    status,
                    List.of())));
    return byBrokerId;
  }
}
