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
package io.taskite.placementdriver.broker.cluster;

import static org.junit.jupiter.api.Assertions.*;

import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.jraft.membership.JRaftPeerMapper;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.broker.topology.ReplicaState;
import io.taskite.placementdriver.state.appliers.BrokerRegisteredApplier;
import io.taskite.placementdriver.state.appliers.BrokerRenewedApplier;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.topology.*;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RaftNodeState;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class BrokerTopologyOperationRequestsTest {
  @Test
  void freezesIpv6BootstrapJoinUsingTheJRaftReportedSource(@TempDir final Path directory) {
    final String host = "2001:db8::1";
    final var peer =
        JRaftPeerMapper.logical("demo").peerId(new RaftPeer("a", host, 26502, true), 10);
    final String digest =
        RaftConfigurationDigest.of(
            List.of(
                new RaftConfigurationDigest.Member("a", peer.getIp(), peer.getPort(), true, 10)));
    try (final var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      state.getBrokerClusterState().remember("demo", 1);
      new BrokerRegisteredApplier(state, () -> Instant.ofEpochMilli(1000))
          .applyState(
              1,
              new BrokerRegistrationRecord()
                  .setClusterId("demo")
                  .setBrokerId("a")
                  .setHost(host)
                  .setPort(26501)
                  .setRaftPort(26502)
                  .setPartitions(List.of(partitionReport(digest))));
      new BrokerRegisteredApplier(state, () -> Instant.ofEpochMilli(1000))
          .applyState(
              2,
              new BrokerRegistrationRecord()
                  .setClusterId("demo")
                  .setBrokerId("b")
                  .setHost("2001:db8::2")
                  .setPort(27501)
                  .setRaftPort(27502));
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(2, List.of("a"), "a"),
                  new PartitionTopology.Membership(List.of("a"), List.of(), Map.of("a", 100), 0),
                  PartitionStatus.ACTIVE,
                  Map.of("a", report(digest)),
                  true));
      final var plan =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "bootstrap-join", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(
                      new TopologyOperation("join-b", TopologyOperationType.JOIN, "b", 0),
                      new TopologyOperation(
                          "promote-b", TopologyOperationType.PROMOTE_LEARNER, "b", 0),
                      new TopologyOperation(
                          "priority-b", TopologyOperationType.RECONFIGURE_PRIORITY, "b", 10)),
                  ChangePlan.Purpose.BOOTSTRAP),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              ChangePlan.Deadline.NONE);

      final var frozen = BrokerTopologyOperationRequests.freeze(state, plan);
      final var join = frozen.nextOperation().configuration();
      assertEquals(digest, join.sourceDigest("demo"));
      assertEquals(12, join.expectedMembershipEpoch());
      assertEquals(List.of(new BrokerPartitionMember("a", true, 10)), join.sourceMembers());
      assertEquals(
          List.of(
              new BrokerPartitionMember("a", true, 10), new BrokerPartitionMember("b", false, 0)),
          join.targetMembers());
      assertEquals(
          join.targetDigest("demo"),
          frozen.operations().get(1).configuration().sourceDigest("demo"));
    }
  }

  @Test
  void priorityRepairFreezesActualSourceAndRejectsIncompleteEvidence(
      @TempDir final Path directory) {
    final var actualMembers =
        List.of(new BrokerPartitionMember("a", true, 10), new BrokerPartitionMember("b", true, 10));
    final String digest =
        new BrokerOperationConfiguration(actualMembers, List.of(), 12, new RecoveryFence(0, 0, ""))
            .sourceDigest("demo");
    final var a = report(digest);
    final var b = report(digest);
    try (final var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      state.getBrokerClusterState().remember("demo", 1);
      new BrokerRegisteredApplier(state, () -> Instant.ofEpochMilli(1000))
          .applyState(
              1,
              new BrokerRegistrationRecord()
                  .setClusterId("demo")
                  .setBrokerId("a")
                  .setHost("127.0.0.1")
                  .setPort(26501)
                  .setRaftPort(26502)
                  .setPartitions(List.of(partitionReport(digest))));
      new BrokerRegisteredApplier(state, () -> Instant.ofEpochMilli(1000))
          .applyState(
              2,
              new BrokerRegistrationRecord()
                  .setClusterId("demo")
                  .setBrokerId("b")
                  .setHost("127.0.0.1")
                  .setPort(27501)
                  .setRaftPort(27502)
                  .setPartitions(List.of(partitionReport(digest))));
      final var source =
          new PartitionTopology(
              new PartitionTopology.Identity("demo", 1),
              new PartitionTopology.Placement(2, List.of("a", "b"), "a"),
              new PartitionTopology.Membership(
                  List.of("a", "b"), List.of(), Map.of("a", 100, "b", 10), 0),
              PartitionStatus.ACTIVE,
              Map.of("a", a, "b", b));
      state.getBrokerPartitionTopologyState().putPartitionTopology(source);
      final var plan =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "repair", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(
                      new TopologyOperation(
                          "repair-a", TopologyOperationType.RECONFIGURE_PRIORITY, "a", 100))),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              ChangePlan.Deadline.NONE);
      final var frozen =
          BrokerTopologyOperationRequests.freeze(state, plan).nextOperation().configuration();
      assertEquals(digest, frozen.sourceDigest("demo"));
      assertNotEquals(frozen.sourceDigest("demo"), frozen.targetDigest("demo"));
      assertEquals(10, frozen.sourceMembers().getFirst().priority());
      assertEquals(100, frozen.targetMembers().getFirst().priority());
      assertEquals(12, frozen.expectedMembershipEpoch());
      new BrokerRegisteredApplier(state, () -> Instant.ofEpochMilli(2000))
          .applyState(
              3,
              new BrokerRegistrationRecord()
                  .setClusterId("demo")
                  .setBrokerId("a")
                  .setHost("::1")
                  .setPort(36501)
                  .setRaftPort(36502)
                  .setPartitions(List.of(partitionReport(digest))));
      final var afterAddressChange =
          BrokerTopologyOperationRequests.freeze(state, plan).nextOperation().configuration();
      assertEquals(frozen.sourceMembers(), afterAddressChange.sourceMembers());
      assertEquals(frozen.targetMembers(), afterAddressChange.targetMembers());
      assertEquals(frozen.sourceDigest("demo"), afterAddressChange.sourceDigest("demo"));
      assertEquals(frozen.targetDigest("demo"), afterAddressChange.targetDigest("demo"));
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  source.identity(),
                  source.placement(),
                  source.membership(),
                  source.status(),
                  Map.of("a", a)));
      final var error =
          assertThrows(
              IllegalStateException.class,
              () -> BrokerTopologyOperationRequests.freeze(state, plan));
      assertTrue(error.getMessage().contains("INCOMPLETE_SOURCE_CONFIGURATION"));
      state.getBrokerPartitionTopologyState().putPartitionTopology(source);
      new BrokerRenewedApplier(state, () -> Instant.ofEpochMilli(2000))
          .applyState(
              3,
              new BrokerRegistrationRecord()
                  .setClusterId("demo")
                  .setBrokerId("a")
                  .setPreservePartitionObservations(true));
      assertThrows(
          IllegalStateException.class,
          () -> BrokerTopologyOperationRequests.freeze(state, plan),
          "initialization keepalive cannot authorize an old priority source");
    }
  }

  @Test
  void recoveryUsesSurvivorVerifiedPersistedSourceAcrossEndpointReplacement(
      @TempDir final Path directory) {
    final var members =
        List.of(
            new BrokerPartitionMember("a", true, 100),
            new BrokerPartitionMember("b", true, 10),
            new BrokerPartitionMember("c", true, 10));
    final var configuration =
        new BrokerOperationConfiguration(members, members, 12, new RecoveryFence(0, 0, ""));
    final String digest = configuration.sourceDigest("demo");
    try (final var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      state.getBrokerClusterState().remember("demo", 1);
      for (final var broker : List.of("a", "b", "c")) {
        new BrokerRegisteredApplier(state, () -> Instant.ofEpochMilli(1000))
            .applyState(
                1,
                new BrokerRegistrationRecord()
                    .setClusterId("demo")
                    .setBrokerId(broker)
                    .setHost("127.0.0.1")
                    .setPort(26501)
                    .setRaftPort(26502)
                    .setPartitions(
                        broker.equals("a") ? List.of(partitionReport(digest)) : List.of()));
      }
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(3, List.of("a", "b", "c"), "a"),
                  new PartitionTopology.Membership(
                      List.of("a", "b", "c"), List.of(), Map.of("a", 100, "b", 10, "c", 10), 0),
                  PartitionStatus.REBALANCING,
                  Map.of("a", report(digest))));
      final var previous =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "old-plan", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(
                      new TopologyOperation(
                          "join", TopologyOperationType.JOIN, "b", 0, configuration))),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.SUPERSEDING),
              ChangePlan.Deadline.NONE);
      state.getBrokerTopologyChangeState().putChangePlan(previous);
      state.getBrokerTopologyChangeState().markPending(previous);
      final var recovery =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "recovery", 1),
                  new ChangePlan.Versions(2, 3),
                  List.of(TopologyOperation.resetPeers(1, "a", 7))),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              ChangePlan.Deadline.NONE);
      final var frozen =
          BrokerTopologyOperationRequests.freeze(state, recovery).nextOperation().configuration();
      assertEquals(digest, frozen.sourceDigest("demo"));
      assertEquals(members, frozen.sourceMembers());
      assertEquals(12, frozen.expectedMembershipEpoch());
      assertEquals(List.of(new BrokerPartitionMember("a", true, 100)), frozen.targetMembers());
      assertEquals(7, frozen.targetRecoveryEpoch());
      new BrokerRenewedApplier(state, () -> Instant.ofEpochMilli(2000))
          .applyState(
              2,
              new BrokerRegistrationRecord()
                  .setClusterId("demo")
                  .setBrokerId("a")
                  .setPreservePartitionObservations(true));
      assertThrows(
          IllegalStateException.class,
          () -> BrokerTopologyOperationRequests.freeze(state, recovery));
    }
  }

  private static BrokerRegistrationRecord.PartitionStateRecord partitionReport(
      final String digest) {
    return new BrokerRegistrationRecord.PartitionStateRecord()
        .setPartitionId(1)
        .setRole(MemberRole.FOLLOWER)
        .setTerm(7)
        .setLifecycleState(BrokerPartitionLifecycle.ACTIVE)
        .setPriority(10)
        .setMatchIndex(12)
        .setReadyForPromotion(true)
        .setLastOpStatus(BrokerTopologyOperationStatus.NONE)
        .setElection(report(digest).election());
  }

  private static ReplicaState report(final String digest) {
    return ReplicaState.observed(
        new BrokerPartitionState(
            1,
            new BrokerPartitionState.Status(
                MemberRole.FOLLOWER, 7, BrokerPartitionLifecycle.ACTIVE, 10, 0),
            new BrokerPartitionState.Replication(12, 0, true),
            new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
            new BrokerPartitionElectionState(
                new RaftConfigurationReference(12, digest),
                new BrokerPartitionElectionState.Priorities(100, 100),
                new BrokerPartitionElectionState.Observation(
                    RaftNodeState.STATE_FOLLOWER, "a", true, 1000))),
        1000);
  }
}
