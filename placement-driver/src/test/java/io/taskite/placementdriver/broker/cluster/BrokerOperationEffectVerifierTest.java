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

import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.broker.topology.ReplicaState;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.topology.*;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RaftNodeState;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class BrokerOperationEffectVerifierTest {
  private static final BrokerOperationConfiguration CONFIGURATION =
      new BrokerOperationConfiguration(
          List.of(),
          List.of(
              new BrokerPartitionMember("a", true, 100), new BrokerPartitionMember("b", true, 10)),
          0,
          new RecoveryFence(0, 0, ""));
  private static final TopologyOperation TRANSFER =
      new TopologyOperation(
          "transfer-a", TopologyOperationType.TRANSFER_LEADER, "a", 0, CONFIGURATION);

  @Test
  void oldLeaderCannotAcknowledgeWhileAnotherVoterHasHigherTerm() {
    final var a = report("LEADER", 10);
    final var b = report("LEADER", 11);
    assertFalse(
        BrokerOperationEffectVerifier.applied(
            topology(a, b), TRANSFER, List.of(broker("a", a), broker("b", b))));
  }

  @Test
  void conflictingLeadersCannotAcknowledgeTransfer() {
    final var a = report("LEADER", 11);
    final var b = report("LEADER", 11);
    assertFalse(
        BrokerOperationEffectVerifier.applied(
            topology(a, b), TRANSFER, List.of(broker("a", a), broker("b", b))));
  }

  @Test
  void emptyStartupReportCannotReusePreviousReplicaReceipt() {
    final var a = report("LEADER", 10);
    final var b = report("FOLLOWER", 10);
    assertFalse(
        BrokerOperationEffectVerifier.applied(
            topology(a, b), TRANSFER, List.of(broker("a"), broker("b", b))));
  }

  @Test
  void uniqueCurrentLeaderCanAcknowledge() {
    final var a = report("LEADER", 11);
    final var b = report("FOLLOWER", 11);
    assertTrue(
        BrokerOperationEffectVerifier.applied(
            topology(a, b), TRANSFER, List.of(broker("a", a), broker("b", b))));
  }

  private static BrokerPartitionState report(final String role, final long term) {
    return new BrokerPartitionState(
        1,
        new BrokerPartitionState.Status(
            MemberRole.parse(role), term, BrokerPartitionLifecycle.ACTIVE, 100, 0),
        new BrokerPartitionState.Replication(20, 0, true),
        new BrokerPartitionState.Operation("transfer-a", BrokerTopologyOperationStatus.APPLIED),
        new BrokerPartitionElectionState(
            new RaftConfigurationReference(12, CONFIGURATION.targetDigest("demo")),
            new BrokerPartitionElectionState.Priorities(100, 100),
            new BrokerPartitionElectionState.Observation(
                RaftNodeState.parse("STATE_" + role),
                "LEADER".equals(role) ? "" : "a",
                true,
                2000)));
  }

  private static BrokerRegistration broker(
      final String id, final BrokerPartitionState... partitions) {
    return new BrokerRegistration(
        new BrokerRegistration.Identity("demo", id, "test", "current-" + id),
        new BrokerRegistration.Endpoint("127.0.0.1", 26501, 26502),
        new BrokerRegistration.Lease(Instant.ofEpochMilli(1000), Instant.ofEpochMilli(2000), 30000),
        BrokerStatus.ONLINE,
        List.of(partitions));
  }

  private static PartitionTopology topology(
      final BrokerPartitionState a, final BrokerPartitionState b) {
    return new PartitionTopology(
        new PartitionTopology.Identity("demo", 1),
        new PartitionTopology.Placement(2, List.of("a", "b"), "a"),
        new PartitionTopology.Membership(
            List.of("a", "b"), List.of(), Map.of("a", 100, "b", 10), 0),
        PartitionStatus.ACTIVE,
        Map.of("a", ReplicaState.observed(a, 2000), "b", ReplicaState.observed(b, 2000)));
  }
}
