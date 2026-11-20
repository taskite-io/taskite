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

import io.taskite.cluster.raft.RaftPeer;
import io.taskite.util.HostPort;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public final class PlacementDriverTopologyManifestReader {
  public static final String FILE_NAME = ".topology";

  public Optional<PlacementDriverTopologyManifest> read(
      final Path dataDirectory, final String localMemberId) {
    final Path manifestPath = dataDirectory.resolve(FILE_NAME);
    if (!Files.exists(manifestPath)) {
      return Optional.empty();
    }
    try {
      final PlacementDriverTopologyManifest manifest =
          PlacementDriverTopologyManifestCodec.decode(Files.readAllBytes(manifestPath));
      validate(manifest, localMemberId);
      return Optional.of(manifest);
    } catch (final IOException error) {
      throw new IllegalStateException("failed to read " + manifestPath, error);
    }
  }

  public List<RaftPeer> toRaftPeers(final PlacementDriverTopologyManifest manifest) {
    return manifest.members().stream()
        .map(
            member ->
                new RaftPeer(
                    member.memberId(),
                    member.host(),
                    member.raftPort(),
                    member.role() == PlacementDriverTopologyManifest.Role.VOTER))
        .toList();
  }

  static void validate(final PlacementDriverTopologyManifest manifest, final String localMemberId) {
    if (manifest.manifestVersion() != PlacementDriverTopologyManifest.CURRENT_VERSION
        || !PlacementDriverTopologyManifest.CLUSTER_ID.equals(manifest.clusterId())
        || manifest.partitionId() != PlacementDriverTopologyManifest.PARTITION_ID
        || !localMemberId.equals(manifest.localMemberId())
        || manifest.source() == null
        || manifest.savedAt() == null
        || manifest.leaderId() == null
        || manifest.topologyVersion() < 0
        || manifest.configurationEpoch() < 0
        || manifest.stateIndex() < 0
        || (manifest.snapshotIndex() != null && manifest.snapshotIndex() < 0)
        || manifest.raftTerm() < 0) {
      throw new IllegalArgumentException("invalid placement-driver topology manifest identity");
    }
    if (manifest.members().isEmpty()
        || manifest.members().stream()
            .noneMatch(member -> localMemberId.equals(member.memberId()))) {
      throw new IllegalArgumentException(
          "placement-driver topology manifest does not contain the local member");
    }
    final Set<String> memberIds = new HashSet<>();
    final Set<String> raftEndpoints = new HashSet<>();
    final Set<String> grpcEndpoints = new HashSet<>();
    int voterCount = 0;
    for (final PlacementDriverTopologyManifest.Member member : manifest.members()) {
      if (member.memberId() == null
          || member.memberId().isBlank()
          || member.host() == null
          || member.host().isBlank()
          || member.role() == null
          || member.incarnationId() == null
          || member.incarnationId().isBlank()
          || member.endpointEpoch() < 0
          || !validPort(member.grpcPort())
          || !validPort(member.raftPort())
          || !memberIds.add(member.memberId())
          || !raftEndpoints.add(HostPort.format(member.host(), member.raftPort()))
          || !grpcEndpoints.add(HostPort.format(member.host(), member.grpcPort()))) {
        throw new IllegalArgumentException("invalid placement-driver topology manifest member");
      }
      if (member.role() == PlacementDriverTopologyManifest.Role.VOTER) {
        voterCount++;
      }
    }
    if (voterCount == 0
        || (!manifest.leaderId().isBlank() && !memberIds.contains(manifest.leaderId()))) {
      throw new IllegalArgumentException(
          "placement-driver topology manifest has no voter or has an unknown leader");
    }
  }

  private static boolean validPort(final int port) {
    return port > 0 && port <= 65_535;
  }
}
