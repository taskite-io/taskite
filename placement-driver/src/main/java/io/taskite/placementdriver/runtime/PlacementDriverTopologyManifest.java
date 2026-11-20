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

import java.time.Instant;
import java.util.List;

/** Node-local bootstrap cache stored at {@code ${taskite.pd.storage.data-dir}/.topology}. */
public record PlacementDriverTopologyManifest(
    int manifestVersion,
    String clusterId,
    int partitionId,
    String localMemberId,
    Source source,
    long topologyVersion,
    long configurationEpoch,
    long stateIndex,
    Long snapshotIndex,
    long raftTerm,
    String leaderId,
    Instant savedAt,
    List<Member> members,
    Checksum checksum) {
  public static final int CURRENT_VERSION = 1;
  public static final String CLUSTER_ID = "placement-driver";
  public static final int PARTITION_ID = 1;

  public PlacementDriverTopologyManifest {
    members = List.copyOf(members == null ? List.of() : members);
  }

  public PlacementDriverTopologyManifest withChecksum(final String value) {
    return new PlacementDriverTopologyManifest(
        manifestVersion,
        clusterId,
        partitionId,
        localMemberId,
        source,
        topologyVersion,
        configurationEpoch,
        stateIndex,
        snapshotIndex,
        raftTerm,
        leaderId,
        savedAt,
        members,
        new Checksum("SHA-256", value));
  }

  public enum Source {
    SNAPSHOT_SAVE,
    SNAPSHOT_LOAD,
    SHUTDOWN
  }

  public enum Role {
    VOTER,
    LEARNER
  }

  public record Member(
      String memberId,
      String host,
      int httpPort,
      int grpcPort,
      int raftPort,
      Role role,
      String incarnationId,
      long endpointEpoch) {}

  public record Checksum(String algorithm, String value) {}
}
