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

import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.PartitionLeadershipResolver;
import io.taskite.topology.BrokerPartitionElectionState;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RaftConfigurationReference;
import io.taskite.topology.RaftNodeState;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

final class PartitionLeadershipResolverTest {

  @Test
  void resolvesUniqueLeaderAtHighestObservedTerm() {
    final var observation =
        PartitionLeadershipResolver.resolve(
            List.of(
                broker("broker-0", "LEADER", 11),
                broker("broker-1", "FOLLOWER", 11),
                broker("broker-2", "LEADER", 99)),
            1,
            List.of("broker-0", "broker-1"));

    assertEquals("broker-0", observation.leaderId());
    assertEquals(11, observation.term());
  }

  @Test
  void suppressesLeaderFromLowerTermDuringElection() {
    final var observation =
        PartitionLeadershipResolver.resolve(
            List.of(broker("broker-0", "LEADER", 10), broker("broker-1", "FOLLOWER", 11)),
            1,
            List.of("broker-0", "broker-1"));

    assertEquals("", observation.leaderId());
    assertEquals(11, observation.term());
  }

  @Test
  void currentHigherTermLeaderMustBeServiceReadyBeforeRouting() {
    final var registration = broker("broker-0", "LEADER", 20);
    final var report = registration.partitions().getFirst();
    final var unready =
        new BrokerPartitionState(
            1,
            report.status(),
            report.replication(),
            report.operation(),
            new BrokerPartitionElectionState(
                report.election().configuration(),
                report.election().priorities(),
                new BrokerPartitionElectionState.Observation(
                    RaftNodeState.STATE_LEADER, "broker-0", false, 2000)));
    final var observation =
        PartitionLeadershipResolver.resolve(
            List.of(
                new BrokerRegistration(
                    registration.identity(),
                    registration.endpoint(),
                    registration.lease(),
                    registration.status(),
                    List.of(unready)),
                broker("broker-1", "LEADER", 10)),
            1,
            List.of("broker-0", "broker-1"));
    assertEquals("", observation.leaderId());
    assertEquals(20, observation.term());
  }

  @Test
  void staleHigherTermAndOfflineReportsCannotOverrideCurrentLeader() {
    final var invalidReports =
        List.of(
            broker("broker-0", "LEADER", 99, 2000, 3000, 1000, BrokerStatus.ONLINE),
            broker("broker-0", "LEADER", 99, 1000, 3000, 2000, BrokerStatus.ONLINE),
            broker("broker-0", "LEADER", 99, 1000, 3000, 3000, BrokerStatus.OFFLINE),
            broker("broker-0", "FOLLOWER", 99, 1000, 3000, 0, BrokerStatus.ONLINE));
    for (final var invalid : invalidReports) {
      final var observation =
          PartitionLeadershipResolver.resolve(
              List.of(invalid, broker("broker-1", "LEADER", 10)),
              1,
              List.of("broker-0", "broker-1"));
      assertEquals("broker-1", observation.leaderId());
      assertEquals(10, observation.term());
    }
  }

  @Test
  void suppressesConflictingLeadersAtSameTerm() {
    final var observation =
        PartitionLeadershipResolver.resolve(
            List.of(broker("broker-0", "LEADER", 11), broker("broker-1", "LEADER", 11)),
            1,
            List.of("broker-0", "broker-1"));

    assertEquals("", observation.leaderId());
    assertEquals(11, observation.term());
  }

  private static BrokerRegistration broker(
      final String brokerId, final String role, final long term) {
    return broker(brokerId, role, term, 1000, 2000, 2000, BrokerStatus.ONLINE);
  }

  private static BrokerRegistration broker(
      final String brokerId,
      final String role,
      final long term,
      final long registered,
      final long renewed,
      final long observed,
      final BrokerStatus status) {
    final var legacy = new BrokerPartitionState(1, MemberRole.parse(role), term);
    final var report =
        new BrokerPartitionState(
            1,
            legacy.status(),
            legacy.replication(),
            legacy.operation(),
            new BrokerPartitionElectionState(
                new RaftConfigurationReference(1, "test"),
                new BrokerPartitionElectionState.Priorities(100, 100),
                new BrokerPartitionElectionState.Observation(
                    RaftNodeState.valueOf("STATE_" + role), brokerId, true, observed)));
    return new BrokerRegistration(
        new BrokerRegistration.Identity("demo", brokerId, "test", ""),
        new BrokerRegistration.Endpoint("127.0.0.1", 26501, 26601),
        new BrokerRegistration.Lease(
            Instant.ofEpochMilli(registered), Instant.ofEpochMilli(renewed), 30_000),
        status,
        List.of(report));
  }
}
