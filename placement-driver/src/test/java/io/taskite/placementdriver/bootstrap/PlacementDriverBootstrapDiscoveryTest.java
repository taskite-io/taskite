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
package io.taskite.placementdriver.bootstrap;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.protocol.PlacementDriverMember;
import io.taskite.placementdriver.protocol.PlacementDriverTopology;
import java.util.List;
import org.junit.jupiter.api.Test;

final class PlacementDriverBootstrapDiscoveryTest {

  @Test
  void acceptsOnlyCompleteLeaderTopology() {
    final PlacementDriverTopology complete =
        topology(
            "meta-0",
            member("meta-0", "LEADER", "10.0.0.1", 29001, 29101),
            member("meta-1", "FOLLOWER", "10.0.0.2", 29002, 29102));
    final PlacementDriverTopology withoutLeader =
        topology("", member("meta-0", "FOLLOWER", "10.0.0.1", 29001, 29101));
    final PlacementDriverTopology missingEndpoint =
        topology("meta-0", member("meta-0", "LEADER", "", 0, 0));
    final PlacementDriverTopology learnerLeader =
        topology("meta-0", member("meta-0", "LEARNER", "10.0.0.1", 29001, 29101));
    final PlacementDriverTopology duplicateMember =
        topology(
            "meta-0",
            member("meta-0", "LEADER", "10.0.0.1", 29001, 29101),
            member("meta-0", "FOLLOWER", "10.0.0.2", 29002, 29102));

    assertTrue(PlacementDriverBootstrapDiscovery.isBootstrappable(complete));
    assertFalse(PlacementDriverBootstrapDiscovery.isBootstrappable(withoutLeader));
    assertFalse(PlacementDriverBootstrapDiscovery.isBootstrappable(missingEndpoint));
    assertFalse(PlacementDriverBootstrapDiscovery.isBootstrappable(learnerLeader));
    assertFalse(PlacementDriverBootstrapDiscovery.isBootstrappable(duplicateMember));
  }

  private static PlacementDriverTopology topology(
      final String leaderId, final PlacementDriverMember... members) {
    return PlacementDriverTopology.newBuilder()
        .setLeaderId(leaderId)
        .addAllMembers(List.of(members))
        .build();
  }

  private static PlacementDriverMember member(
      final String memberId,
      final String role,
      final String host,
      final int grpcPort,
      final int raftPort) {
    return PlacementDriverMember.newBuilder()
        .setMemberId(memberId)
        .setRole(role)
        .setHost(host)
        .setGrpcPort(grpcPort)
        .setRaftPort(raftPort)
        .build();
  }
}
