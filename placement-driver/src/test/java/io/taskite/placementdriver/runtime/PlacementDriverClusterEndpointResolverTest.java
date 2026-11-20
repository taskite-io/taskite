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
package io.taskite.placementdriver.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.taskite.placementdriver.PlacementDriverPeer;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

final class PlacementDriverClusterEndpointResolverTest {
  @Test
  void committedTopologyOverridesStaticSeedEndpoint() {
    final MemberTopologyRecord topology =
        new MemberTopologyRecord()
            .setMembers(
                List.of(
                    new MemberTopologyRecord.MemberRecord()
                        .setMemberId("meta-1")
                        .setHost("10.0.0.42")
                        .setGrpcPort(29042)
                        .setEndpoint("10.0.0.42:29042")));
    final PlacementDriverClusterEndpointResolver resolver =
        new PlacementDriverClusterEndpointResolver(
            "meta-0",
            "127.0.0.1",
            29000,
            List.of(new PlacementDriverPeer("meta-1", "10.0.0.1", 29001, 29101)),
            () -> Optional.of(topology));

    assertEquals("10.0.0.42:29042", resolver.resolveMemberEndpointText("meta-1").orElseThrow());
  }

  @Test
  void ipv6EndpointsUseBracketsForLeaderRedirection() {
    final var topology =
        new MemberTopologyRecord()
            .setMembers(
                List.of(
                    new MemberTopologyRecord.MemberRecord()
                        .setMemberId("meta-1")
                        .setHost("2001:db8::42")
                        .setGrpcPort(26501),
                    new MemberTopologyRecord.MemberRecord()
                        .setMemberId("meta-2")
                        .setEndpoint("[2001:db8::2]:26501")));
    final var resolver =
        new PlacementDriverClusterEndpointResolver(
            "meta-0",
            "::1",
            26501,
            List.of(new PlacementDriverPeer("meta-3", "2001:db8::3", 26501, 26502)),
            () -> Optional.of(topology));

    assertEquals("[::1]:26501", resolver.resolveMemberEndpointText("meta-0").orElseThrow());
    assertEquals(
        "[2001:db8::42]:26501", resolver.resolveMemberEndpointText("meta-1").orElseThrow());
    assertEquals("2001:db8::2", resolver.resolveMemberEndpoint("meta-2").orElseThrow().host());
    assertEquals("[2001:db8::3]:26501", resolver.resolveMemberEndpointText("meta-3").orElseThrow());
    assertEquals(Optional.empty(), resolver.resolveMemberEndpointText("unknown"));
    assertEquals(Optional.empty(), resolver.resolveMemberEndpointText(""));
  }
}
