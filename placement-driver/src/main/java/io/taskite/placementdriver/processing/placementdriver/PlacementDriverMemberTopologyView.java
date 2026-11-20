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

import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;

final class PlacementDriverMemberTopologyView {
  private PlacementDriverMemberTopologyView() {}

  static MemberTopologyRecord forMember(
      final MemberTopologyRecord topology, final String requestingMemberId) {
    final String memberId =
        requestingMemberId == null || requestingMemberId.isBlank()
            ? topology.getLocalMemberId()
            : requestingMemberId;
    final var members =
        topology.getMembers().stream()
            .map(
                member ->
                    new MemberTopologyRecord.MemberRecord()
                        .setMemberId(member.getMemberId())
                        .setEndpoint(member.getEndpoint())
                        .setRole(member.getRole())
                        .setSelf(memberId.equals(member.getMemberId()))
                        .setHost(member.getHost())
                        .setHttpPort(member.getHttpPort())
                        .setGrpcPort(member.getGrpcPort())
                        .setRaftPort(member.getRaftPort())
                        .setStatus(member.getStatus())
                        .setIncarnationId(member.getIncarnationId())
                        .setEndpointEpoch(member.getEndpointEpoch())
                        .setCommitIndex(member.getCommitIndex())
                        .setLastApplied(member.getLastApplied())
                        .setLastLogIndex(member.getLastLogIndex())
                        .setReadyForPromotion(member.isReadyForPromotion()))
            .toList();
    return new MemberTopologyRecord()
        .setLocalMemberId(memberId)
        .setLeaderId(topology.getLeaderId())
        .setLeaderEndpoint(topology.getLeaderEndpoint())
        .setTerm(topology.getTerm())
        .setConfigurationEpoch(topology.getConfigurationEpoch())
        .setMembers(members);
  }
}
