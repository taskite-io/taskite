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

import io.taskite.cluster.raft.RaftMember;
import io.taskite.cluster.raft.RaftPartition;
import io.taskite.cluster.raft.RaftRole;
import java.util.List;
import java.util.Optional;

public final class PlacementDriverClusterLeaderLocator {
  private final String localNodeId;
  private final RaftPartition raftPartition;
  private final PlacementDriverClusterEndpointResolver endpointResolver;

  public PlacementDriverClusterLeaderLocator(
      final String localNodeId,
      final RaftPartition raftPartition,
      final PlacementDriverClusterEndpointResolver endpointResolver) {
    this.localNodeId = localNodeId;
    this.raftPartition = raftPartition;
    this.endpointResolver = endpointResolver;
  }

  public boolean isLocalLeader() {
    return raftPartition.role() == RaftRole.LEADER && localNodeId.equals(raftPartition.leaderId());
  }

  public String localMemberId() {
    return localNodeId;
  }

  public Optional<String> locateLeaderMemberId() {
    final List<String> leaders =
        raftPartition.members().stream()
            .filter(member -> member.role() == RaftRole.LEADER)
            .map(RaftMember::memberId)
            .toList();
    if (leaders.size() != 1) {
      return Optional.empty();
    }
    final String leaderId = leaders.getFirst();
    if (localNodeId.equals(leaderId) && !isLocalLeader()) {
      return Optional.empty();
    }
    return Optional.of(leaderId);
  }

  public Optional<PlacementDriverEndpoint> locateLeaderEndpoint() {
    return locateLeaderMemberId().flatMap(endpointResolver::resolveMemberEndpoint);
  }

  public Optional<String> locateLeaderEndpointText() {
    return locateLeaderMemberId().flatMap(endpointResolver::resolveMemberEndpointText);
  }

  public Optional<String> locateOtherLeaderEndpointText() {
    return locateLeaderMemberId()
        .filter(leaderId -> !localNodeId.equals(leaderId))
        .flatMap(endpointResolver::resolveMemberEndpointText);
  }
}
