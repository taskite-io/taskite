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

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService;
import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.BrokerTopologyPlanner;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.topology.PartitionStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** T9/T10: sticky voter roster reconciliation — the clusterSize-violation regression. */
final class VoterRosterTest {
  private static final int CLUSTER_SIZE = 3;
  private static final BrokerClusterChangePlanningService PLANNING_SERVICE =
      new BrokerClusterChangePlanningService();

  @Test
  void initialFillPicksFirstClusterSizeBrokersOnFreshCluster() {
    final List<BrokerRegistration> online =
        List.of(online("broker-0"), online("broker-1"), online("broker-2"), online("broker-3"));

    final List<String> roster =
        PLANNING_SERVICE.calculateVoterRoster(List.of(), CLUSTER_SIZE, online, List.of());

    assertEquals(List.of("broker-0", "broker-1", "broker-2"), roster);
  }

  @Test
  void initialFillHealsDriftedVoterUnionToBusiestClusterSize() {
    // The reported broken state: b0/b1/b2 vote in P1-3, b3 replaced b0 in P4/P5 -> union of 4.
    final List<BrokerRegistration> online =
        List.of(online("broker-0"), online("broker-1"), online("broker-2"), online("broker-3"));
    final List<PartitionTopology> placement =
        List.of(
            partition(1, "broker-0", "broker-1", "broker-2"),
            partition(2, "broker-1", "broker-2", "broker-0"),
            partition(3, "broker-2", "broker-1", "broker-0"),
            partition(4, "broker-1", "broker-2", "broker-3"),
            partition(5, "broker-1", "broker-2", "broker-3"));

    final List<String> roster =
        PLANNING_SERVICE.calculateVoterRoster(List.of(), CLUSTER_SIZE, online, placement);

    // Busiest three by voter-count: b1(5), b2(5), b0(3) -> b3(2) drops to spare. Union back to 3.
    assertEquals(List.of("broker-1", "broker-2", "broker-0"), roster);
    assertTrue(roster.size() <= CLUSTER_SIZE);
    assertTrue(!roster.contains("broker-3"), "the fourth broker must become a spare, not a voter");
  }

  @Test
  void shortFlapDoesNotChangeRoster() {
    // b0 is still ONLINE (restarted within the lease); the roster is untouched.
    final List<String> current = List.of("broker-0", "broker-1", "broker-2");
    final List<BrokerRegistration> registrations =
        List.of(online("broker-0"), online("broker-1"), online("broker-2"), online("broker-3"));

    final List<String> roster =
        PLANNING_SERVICE.calculateVoterRoster(current, CLUSTER_SIZE, registrations, List.of());

    assertEquals(current, roster);
  }

  @Test
  void offlineMemberYieldsSlotToSpareAndReturneeStaysSpare() {
    final List<String> current = List.of("broker-0", "broker-1", "broker-2");
    // b0 offline (lease expired), b3 online spare -> b3 takes b0's slot in place.
    final List<BrokerRegistration> afterOffline =
        List.of(offline("broker-0"), online("broker-1"), online("broker-2"), online("broker-3"));

    final List<String> replaced =
        PLANNING_SERVICE.calculateVoterRoster(current, CLUSTER_SIZE, afterOffline, List.of());

    assertEquals(List.of("broker-3", "broker-1", "broker-2"), replaced);

    // b0 comes back ONLINE: it must NOT reclaim its old slot; it stays a spare.
    final List<BrokerRegistration> afterReturn =
        List.of(online("broker-0"), online("broker-1"), online("broker-2"), online("broker-3"));
    final List<String> afterReturnRoster =
        PLANNING_SERVICE.calculateVoterRoster(replaced, CLUSTER_SIZE, afterReturn, List.of());

    assertEquals(replaced, afterReturnRoster);
    assertTrue(!afterReturnRoster.contains("broker-0"), "returning broker must remain a spare");
  }

  @Test
  void offlineWithoutSpareKeepsRosterStableForLaterReturn() {
    final List<String> current = List.of("broker-0", "broker-1", "broker-2");
    // No spare online: the offline member keeps its slot so it can resume when it returns.
    final List<BrokerRegistration> registrations =
        List.of(offline("broker-0"), online("broker-1"), online("broker-2"));

    final List<String> roster =
        PLANNING_SERVICE.calculateVoterRoster(current, CLUSTER_SIZE, registrations, List.of());

    assertEquals(current, roster);
  }

  @Test
  void rosterDrivenVoterSetNeverExceedsClusterSizeAcrossChurn() {
    // Once the roster is stable at 3, the derived voter union over all partitions stays at 3
    // regardless of a fourth broker being online — the core invariant the bug violated.
    final List<String> roster = List.of("broker-0", "broker-1", "broker-2");
    final BrokerClusterConfig config = new BrokerClusterConfig(CLUSTER_SIZE, 5, 3, roster, 1);
    final List<BrokerRegistration> online =
        List.of(online("broker-0"), online("broker-1"), online("broker-2"), online("broker-3"));

    final Set<String> voterUnion = new HashSet<>();
    for (int partitionId = 1; partitionId <= config.partitionSize(); partitionId++) {
      final var placement =
          BrokerTopologyPlanner.planPartition(
              "demo",
              config,
              partitionId,
              online,
              BrokerTopologyPlanner.voterRegistrations(config, online));
      voterUnion.addAll(placement.desiredVoters());
      assertTrue(!placement.desiredVoters().contains("broker-3"), "spare must stay a learner");
    }
    assertEquals(Set.of("broker-0", "broker-1", "broker-2"), voterUnion);
    assertEquals(CLUSTER_SIZE, voterUnion.size());
  }

  private static BrokerRegistration online(final String brokerId) {
    return registration(brokerId, BrokerStatus.ONLINE);
  }

  private static BrokerRegistration offline(final String brokerId) {
    return registration(brokerId, BrokerStatus.OFFLINE);
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

  private static PartitionTopology partition(final int partitionId, final String... voters) {
    final List<String> voterList = new ArrayList<>(List.of(voters));
    return new PartitionTopology(
        new PartitionTopology.Identity("demo", partitionId),
        new PartitionTopology.Placement(3, voterList, voterList.isEmpty() ? "" : voterList.get(0)),
        PartitionTopology.Membership.initial(
            voterList, List.of(), voterList.isEmpty() ? "" : voterList.get(0)),
        PartitionStatus.ACTIVE,
        Map.of());
  }
}
