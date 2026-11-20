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

import io.taskite.placementdriver.PlacementDriverPeer;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.util.HostPort;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public final class PlacementDriverClusterEndpointResolver {
  private final String localNodeId;
  private final PlacementDriverEndpoint localEndpoint;
  private final List<PlacementDriverPeer> peers;
  private final Supplier<Optional<MemberTopologyRecord>> topologySupplier;

  public PlacementDriverClusterEndpointResolver(
      final String localNodeId,
      final String localHost,
      final int localGrpcPort,
      final List<PlacementDriverPeer> peers) {
    this(localNodeId, localHost, localGrpcPort, peers, Optional::empty);
  }

  public PlacementDriverClusterEndpointResolver(
      final String localNodeId,
      final String localHost,
      final int localGrpcPort,
      final List<PlacementDriverPeer> peers,
      final Supplier<Optional<MemberTopologyRecord>> topologySupplier) {
    this.localNodeId = localNodeId;
    localEndpoint = new PlacementDriverEndpoint(localHost, localGrpcPort);
    this.peers = List.copyOf(peers);
    this.topologySupplier = topologySupplier;
  }

  public Optional<PlacementDriverEndpoint> resolveMemberEndpoint(final String memberId) {
    if (memberId == null || memberId.isBlank()) {
      return Optional.empty();
    }
    if (localNodeId.equals(memberId)) {
      return Optional.of(localEndpoint);
    }
    final Optional<PlacementDriverEndpoint> committed =
        topologySupplier.get().stream()
            .flatMap(topology -> topology.getMembers().stream())
            .filter(member -> memberId.equals(member.getMemberId()))
            .map(PlacementDriverClusterEndpointResolver::endpoint)
            .filter(endpoint -> !endpoint.host().isBlank() && endpoint.port() > 0)
            .findFirst();
    if (committed.isPresent()) {
      return committed;
    }
    return peers.stream()
        .filter(peer -> memberId.equals(peer.memberId()))
        .findFirst()
        .map(peer -> new PlacementDriverEndpoint(peer.host(), peer.grpcPort()));
  }

  public Optional<String> resolveMemberEndpointText(final String memberId) {
    return resolveMemberEndpoint(memberId).map(PlacementDriverClusterEndpointResolver::format);
  }

  private static String format(final PlacementDriverEndpoint endpoint) {
    return HostPort.format(endpoint.host(), endpoint.port());
  }

  private static PlacementDriverEndpoint endpoint(final MemberTopologyRecord.MemberRecord member) {
    if (!member.getHost().isBlank() && member.getGrpcPort() > 0) {
      return new PlacementDriverEndpoint(member.getHost(), member.getGrpcPort());
    }
    final String value = member.getEndpoint();
    try {
      return PlacementDriverEndpoint.parse(value);
    } catch (final IllegalArgumentException ignored) {
      return new PlacementDriverEndpoint("", 0);
    }
  }
}
