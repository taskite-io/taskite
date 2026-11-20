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
package io.taskite.streamplatform.runtime;

import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.snapshot.SnapshotPolicy;
import io.taskite.streamplatform.logstream.LogBatchSizing;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/** Immutable configuration for one stream-backed raft partition. */
public record StreamPartitionConfig(
    String clusterId,
    int partitionId,
    String localMemberId,
    long initialTerm,
    RaftRole initialRole,
    List<RaftPeer> peers,
    Path dataDirectory,
    int maxInFlight,
    int maxCommandsInBatch,
    int maxRecordsInBatch,
    Duration maxBatchDuration,
    int maxBatchBytes,
    SnapshotPolicy snapshotPolicy,
    Duration electionTimeout,
    Duration heartbeatInterval,
    boolean ownsScheduler) {

  public static final SnapshotPolicy DEFAULT_SNAPSHOT_POLICY = SnapshotPolicy.defaults();
  public static final Duration DEFAULT_ELECTION_TIMEOUT = Duration.ofSeconds(5);
  public static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofMillis(300);
  public static final int DEFAULT_MAX_COMMANDS_IN_BATCH = 1;
  public static final int DEFAULT_MAX_RECORDS_IN_BATCH = 0;
  public static final Duration DEFAULT_MAX_BATCH_DURATION = Duration.ZERO;
  public static final int DEFAULT_MAX_BATCH_BYTES = LogBatchSizing.DEFAULT_MAX_BATCH_BYTES;
  public static final int MIN_MAX_BATCH_BYTES = LogBatchSizing.MIN_CONFIGURED_MAX_BATCH_BYTES;

  public StreamPartitionConfig {
    Objects.requireNonNull(clusterId, "clusterId");
    Objects.requireNonNull(localMemberId, "localMemberId");
    Objects.requireNonNull(initialRole, "initialRole");
    peers = List.copyOf(Objects.requireNonNull(peers, "peers"));
    Objects.requireNonNull(dataDirectory, "dataDirectory");
    Objects.requireNonNull(maxBatchDuration, "maxBatchDuration");
    Objects.requireNonNull(snapshotPolicy, "snapshotPolicy");
    Objects.requireNonNull(electionTimeout, "electionTimeout");
    Objects.requireNonNull(heartbeatInterval, "heartbeatInterval");
    maxInFlight = Math.max(1, maxInFlight);
    if (maxCommandsInBatch < 1) {
      throw new IllegalArgumentException("maxCommandsInBatch must be at least 1");
    }
    if (maxRecordsInBatch < 0) {
      throw new IllegalArgumentException("maxRecordsInBatch must not be negative");
    }
    if (maxBatchDuration.isNegative()) {
      throw new IllegalArgumentException("maxBatchDuration must not be negative");
    }
    LogBatchSizing.validateConfiguredMaxBatchBytes(maxBatchBytes);
    if (electionTimeout.isZero()
        || electionTimeout.isNegative()
        || electionTimeout.toMillis() < 1
        || electionTimeout.toMillis() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "electionTimeout must be between 1ms and " + Integer.MAX_VALUE + "ms");
    }
    if (heartbeatInterval.isNegative() || heartbeatInterval.toMillis() < 1) {
      throw new IllegalArgumentException("heartbeatInterval must be at least 1ms");
    }
  }

  public static Builder builder(
      final String clusterId,
      final int partitionId,
      final String localMemberId,
      final Path dataDirectory) {
    return new Builder(clusterId, partitionId, localMemberId, dataDirectory);
  }

  public static final class Builder {
    private final String clusterId;
    private final int partitionId;
    private final String localMemberId;
    private final Path dataDirectory;
    private long initialTerm;
    private RaftRole initialRole = RaftRole.FOLLOWER;
    private List<RaftPeer> peers = List.of();
    private int maxInFlight = 1;
    private int maxCommandsInBatch = DEFAULT_MAX_COMMANDS_IN_BATCH;
    private int maxRecordsInBatch = DEFAULT_MAX_RECORDS_IN_BATCH;
    private Duration maxBatchDuration = DEFAULT_MAX_BATCH_DURATION;
    private int maxBatchBytes = DEFAULT_MAX_BATCH_BYTES;
    private SnapshotPolicy snapshotPolicy = DEFAULT_SNAPSHOT_POLICY;
    private Duration electionTimeout = DEFAULT_ELECTION_TIMEOUT;
    private Duration heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
    private boolean ownsScheduler;

    private Builder(
        final String clusterId,
        final int partitionId,
        final String localMemberId,
        final Path dataDirectory) {
      this.clusterId = clusterId;
      this.partitionId = partitionId;
      this.localMemberId = localMemberId;
      this.dataDirectory = dataDirectory;
    }

    public Builder initialTerm(final long initialTerm) {
      this.initialTerm = initialTerm;
      return this;
    }

    public Builder initialRole(final RaftRole initialRole) {
      this.initialRole = initialRole;
      return this;
    }

    public Builder peers(final List<RaftPeer> peers) {
      this.peers = peers;
      return this;
    }

    public Builder maxInFlight(final int maxInFlight) {
      this.maxInFlight = maxInFlight;
      return this;
    }

    public Builder maxCommandsInBatch(final int maxCommandsInBatch) {
      this.maxCommandsInBatch = maxCommandsInBatch;
      return this;
    }

    public Builder maxRecordsInBatch(final int maxRecordsInBatch) {
      this.maxRecordsInBatch = maxRecordsInBatch;
      return this;
    }

    public Builder maxBatchDuration(final Duration maxBatchDuration) {
      this.maxBatchDuration = maxBatchDuration;
      return this;
    }

    public Builder maxBatchBytes(final int maxBatchBytes) {
      this.maxBatchBytes = maxBatchBytes;
      return this;
    }

    public Builder snapshotPolicy(final SnapshotPolicy policy) {
      snapshotPolicy = policy;
      return this;
    }

    public Builder electionTimeout(final Duration timeout) {
      electionTimeout = timeout;
      return this;
    }

    public Builder heartbeatInterval(final Duration interval) {
      heartbeatInterval = interval;
      return this;
    }

    public Builder ownsScheduler(final boolean ownsScheduler) {
      this.ownsScheduler = ownsScheduler;
      return this;
    }

    public StreamPartitionConfig build() {
      return new StreamPartitionConfig(
          clusterId,
          partitionId,
          localMemberId,
          initialTerm,
          initialRole,
          peers,
          dataDirectory,
          maxInFlight,
          maxCommandsInBatch,
          maxRecordsInBatch,
          maxBatchDuration,
          maxBatchBytes,
          snapshotPolicy,
          electionTimeout,
          heartbeatInterval,
          ownsScheduler);
    }
  }
}
