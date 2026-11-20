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
package io.taskite.placementdriver.common.mapper;

import io.taskite.placementdriver.protocol.PlacementDriverMember;
import io.taskite.placementdriver.protocol.PlacementDriverTopology;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyUpdateType;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RegistrationStatus;

/** Structured node RPC representation of the internal topology value. */
public final class PlacementDriverTopologyMapper {
  private PlacementDriverTopologyMapper() {}

  public static PlacementDriverTopology toProto(final MemberTopologyRecord topology) {
    final var result =
        PlacementDriverTopology.newBuilder()
            .setUpdateType(topology.getUpdateType().name())
            .setLocalMemberId(topology.getLocalMemberId())
            .setLeaderId(topology.getLeaderId())
            .setLeaderEndpoint(topology.getLeaderEndpoint())
            .setTerm(topology.getTerm())
            .setConfigurationEpoch(topology.getConfigurationEpoch());
    topology
        .getMembers()
        .forEach(
            member ->
                result.addMembers(
                    PlacementDriverMember.newBuilder()
                        .setMemberId(member.getMemberId())
                        .setEndpoint(member.getEndpoint())
                        .setRole(member.getRole().name())
                        .setSelf(member.isSelf())
                        .setHost(member.getHost())
                        .setHttpPort(member.getHttpPort())
                        .setGrpcPort(member.getGrpcPort())
                        .setRaftPort(member.getRaftPort())
                        .setStatus(member.getStatus().name())
                        .setIncarnationId(member.getIncarnationId())
                        .setEndpointEpoch(member.getEndpointEpoch())
                        .setCommitIndex(member.getCommitIndex())
                        .setLastApplied(member.getLastApplied())
                        .setLastLogIndex(member.getLastLogIndex())
                        .setReadyForPromotion(member.isReadyForPromotion())));
    return result.build();
  }

  public static MemberTopologyRecord fromProto(final PlacementDriverTopology topology) {
    return new MemberTopologyRecord()
        .setUpdateType(TopologyUpdateType.valueOf(topology.getUpdateType()))
        .setLocalMemberId(topology.getLocalMemberId())
        .setLeaderId(topology.getLeaderId())
        .setLeaderEndpoint(topology.getLeaderEndpoint())
        .setTerm(topology.getTerm())
        .setConfigurationEpoch(topology.getConfigurationEpoch())
        .setMembers(
            topology.getMembersList().stream()
                .map(
                    member ->
                        new MemberTopologyRecord.MemberRecord()
                            .setMemberId(member.getMemberId())
                            .setEndpoint(member.getEndpoint())
                            .setRole(MemberRole.valueOf(member.getRole()))
                            .setSelf(member.getSelf())
                            .setHost(member.getHost())
                            .setHttpPort(member.getHttpPort())
                            .setGrpcPort(member.getGrpcPort())
                            .setRaftPort(member.getRaftPort())
                            .setStatus(RegistrationStatus.valueOf(member.getStatus()))
                            .setIncarnationId(member.getIncarnationId())
                            .setEndpointEpoch(member.getEndpointEpoch())
                            .setCommitIndex(member.getCommitIndex())
                            .setLastApplied(member.getLastApplied())
                            .setLastLogIndex(member.getLastLogIndex())
                            .setReadyForPromotion(member.getReadyForPromotion()))
                .toList());
  }
}
