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

import static org.junit.jupiter.api.Assertions.*;

import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.topology.BrokerPartitionElectionState;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RaftConfigurationReference;
import io.taskite.topology.RaftNodeState;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

final class PartitionLeadershipResolverTest {
  @Test
  void requiresUniqueCurrentReadyLeaderAtHighestTerm() {
    final var leader = registration("b1", "LEADER", 2, 3, true, 1000);
    assertEquals("b1", resolve(List.of(leader)).leaderId());
    assertEquals(
        "", resolve(List.of(leader, registration("b2", "LEADER", 2, 3, true, 1000))).leaderId());
    assertEquals(
        "", resolve(List.of(leader, registration("b2", "FOLLOWER", 3, 3, false, 1000))).leaderId());
    assertEquals("", resolve(List.of(registration("b1", "LEADER", 2, 3, false, 1000))).leaderId());
  }

  @Test
  void rejectsOldRecoveryEpochOldIncarnationReportAndNonVoterLeader() {
    assertEquals("", resolve(List.of(registration("b1", "LEADER", 2, 2, true, 1000))).leaderId());
    assertEquals("", resolve(List.of(registration("b1", "LEADER", 2, 3, true, 999))).leaderId());
    assertEquals(
        "", resolve(List.of(registration("spare", "LEADER", 4, 3, true, 1000))).leaderId());
  }

  @Test
  void managementDiscoversRaftLeaderBeforeBusinessReadinessWithoutEnablingRouting() {
    final var leader = registration("b1", "LEADER", 2, 3, false, 1000);
    final var registrations = List.of(leader);
    final var management =
        PartitionLeadershipResolver.resolveForManagement(registrations, 1, List.of("b1", "b2"), 3);

    assertEquals("b1", management.leaderId());
    assertEquals(2, management.term());
    assertEquals("", resolve(registrations).leaderId());
  }

  @Test
  void managementRequiresUniqueCurrentRaftLeaderAtHighestTermAndRecoveryEpoch() {
    final var leader = registration("b1", "LEADER", 2, 3, false, 1000);
    for (final var reports :
        List.of(
            List.of(leader, registration("b2", "LEADER", 2, 3, false, 1000)),
            List.of(leader, registration("b2", "FOLLOWER", 3, 3, false, 1000)),
            List.of(registration("b1", "LEADER", 2, 2, false, 1000)),
            List.of(registration("b1", "LEADER", 2, 3, false, 999)),
            List.of(registration("spare", "LEADER", 2, 3, false, 1000)))) {
      assertEquals(
          "",
          PartitionLeadershipResolver.resolveForManagement(reports, 1, List.of("b1", "b2"), 3)
              .leaderId());
    }
  }

  private static PartitionLeadershipResolver.Observation resolve(
      List<BrokerRegistration> registrations) {
    return PartitionLeadershipResolver.resolve(registrations, 1, List.of("b1", "b2"), 3);
  }

  private static BrokerRegistration registration(
      String id, String role, long term, long epoch, boolean ready, long observed) {
    final var report =
        new BrokerPartitionState(
            1,
            new BrokerPartitionState.Status(
                MemberRole.parse(role), term, BrokerPartitionLifecycle.ACTIVE, 100, epoch),
            new BrokerPartitionState.Replication(10, 0, false),
            new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
            new BrokerPartitionElectionState(
                RaftConfigurationReference.UNKNOWN,
                new BrokerPartitionElectionState.Priorities(100, 100),
                new BrokerPartitionElectionState.Observation(
                    RaftNodeState.parse("STATE_" + role), id, ready, observed)));
    return new BrokerRegistration(
        new BrokerRegistration.Identity("demo", id, "test", "incarnation"),
        new BrokerRegistration.Endpoint("127.0.0.1", 26501, 26502),
        new BrokerRegistration.Lease(Instant.ofEpochMilli(1000), Instant.ofEpochMilli(1000), 30000),
        BrokerStatus.ONLINE,
        List.of(report));
  }
}
