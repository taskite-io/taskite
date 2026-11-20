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
package io.taskite.placementdriver.processing.placementdriver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.topology.MemberRole;
import java.util.List;
import org.junit.jupiter.api.Test;

final class PlacementDriverMemberTopologyViewTest {
  @Test
  void projectsCommittedTopologyForRequestingFollower() {
    final MemberTopologyRecord topology =
        new MemberTopologyRecord()
            .setLocalMemberId("meta-0")
            .setLeaderId("meta-0")
            .setLeaderEndpoint("127.0.0.1:29001")
            .setTerm(3)
            .setMembers(
                List.of(member("meta-0", "LEADER", true), member("meta-1", "FOLLOWER", false)));

    final MemberTopologyRecord followerView =
        PlacementDriverMemberTopologyView.forMember(topology, "meta-1");

    assertEquals("meta-1", followerView.getLocalMemberId());
    assertEquals("meta-0", followerView.getLeaderId());
    assertFalse(followerView.getMembers().get(0).isSelf());
    assertTrue(followerView.getMembers().get(1).isSelf());
    assertEquals(MemberRole.FOLLOWER, followerView.getMembers().get(1).getRole());
  }

  private static MemberTopologyRecord.MemberRecord member(
      final String memberId, final String role, final boolean self) {
    return new MemberTopologyRecord.MemberRecord()
        .setMemberId(memberId)
        .setEndpoint("127.0.0.1:29001")
        .setRole(MemberRole.parse(role))
        .setSelf(self);
  }
}
