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
package io.taskite.cluster.raft;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Immutable configuration for a raft partition ({@link
 * io.taskite.cluster.raft.jraft.JRaftPartition}): identity, the initial raft membership/term, its
 * role, where its RocksDB runtime lives, and the election cadence. Built via {@link
 * #builder(String, int, String)}.
 *
 * <p>jraft owns replication/log/snapshot/transport internally, so this config no longer carries the
 * in-house engine's transport, snapshot store, journal sizing or heartbeat cadence — the jraft path
 * drives those itself.
 */
public record RaftPartitionConfig(
    String clusterId,
    int partitionId,
    String localBrokerId,
    long term,
    RaftRole role,
    List<RaftPeer> peers,
    Path dataDirectory,
    Duration electionTimeout) {

  public RaftPartitionConfig {
    Objects.requireNonNull(clusterId, "clusterId");
    Objects.requireNonNull(localBrokerId, "localBrokerId");
    Objects.requireNonNull(role, "role");
    Objects.requireNonNull(dataDirectory, "dataDirectory");
    Objects.requireNonNull(electionTimeout, "electionTimeout");
    peers = List.copyOf(peers == null ? List.of() : peers);
  }

  public static Builder builder(String clusterId, int partitionId, String localBrokerId) {
    return new Builder(clusterId, partitionId, localBrokerId);
  }

  public static final class Builder {
    private final String clusterId;
    private final int partitionId;
    private final String localBrokerId;
    private long term;
    private RaftRole role = RaftRole.FOLLOWER;
    private List<RaftPeer> peers = List.of();
    private Path dataDirectory;
    private Duration electionTimeout = Duration.ofSeconds(1);

    private Builder(String clusterId, int partitionId, String localBrokerId) {
      this.clusterId = clusterId;
      this.partitionId = partitionId;
      this.localBrokerId = localBrokerId;
    }

    public Builder term(long term) {
      this.term = term;
      return this;
    }

    public Builder role(RaftRole role) {
      this.role = role;
      return this;
    }

    public Builder peers(List<RaftPeer> peers) {
      this.peers = peers;
      return this;
    }

    public Builder dataDirectory(Path dataDirectory) {
      this.dataDirectory = dataDirectory;
      return this;
    }

    public Builder electionTimeout(final Duration electionTimeout) {
      this.electionTimeout = electionTimeout;
      return this;
    }

    public RaftPartitionConfig build() {
      return new RaftPartitionConfig(
          clusterId, partitionId, localBrokerId, term, role, peers, dataDirectory, electionTimeout);
    }
  }
}
