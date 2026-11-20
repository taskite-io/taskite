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
package io.taskite.broker;

import io.taskite.broker.common.ExponentialBackoff;
import io.taskite.snapshot.SnapshotPolicy;
import io.taskite.streamplatform.api.backpressure.BackpressureConfiguration;
import io.taskite.streamplatform.runtime.StreamPartitionConfig;
import io.taskite.util.HostPort;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

/**
 * Local Broker startup configuration. Cluster sizing and membership are resolved from PD.
 *
 * <p>{@code dataDirectory} is the shared data root. A partition is stored under {@code
 * dataDirectory/clusterId/brokerId/partitions/partitionId}.
 *
 * <p>{@code port} is the public business gRPC endpoint. {@code raftPort} is a separate, explicitly
 * configured Broker-to-Broker transport endpoint because Raft replication has independent
 * connectivity, load, timeout, and access-control requirements. No port offset is inferred.
 */
public record BrokerConfig(
    Identity identity,
    Node node,
    Registration registration,
    Engine engine,
    BackpressureConfiguration backpressure) {
  public BrokerConfig(
      final Identity identity,
      final Node node,
      final Registration registration,
      final Engine engine) {
    this(identity, node, registration, engine, BackpressureConfiguration.defaults());
  }

  public static final Duration DEFAULT_RENEWAL_TIMEOUT = Duration.ofSeconds(30);
  public static final Duration DEFAULT_SNAPSHOT_INTERVAL = Duration.ofMinutes(15);
  public static final long DEFAULT_SNAPSHOT_LOG_SIZE_THRESHOLD_BYTES =
      SnapshotPolicy.DEFAULT_LOG_SIZE_THRESHOLD_BYTES;
  public static final Duration DEFAULT_ELECTION_TIMEOUT = Duration.ofSeconds(5);
  public static final int DEFAULT_MAX_COMMANDS_IN_BATCH = 1;
  public static final int DEFAULT_MAX_RECORDS_IN_BATCH = 0;
  public static final Duration DEFAULT_MAX_BATCH_DURATION = Duration.ZERO;
  public static final int DEFAULT_MAX_BATCH_BYTES = StreamPartitionConfig.DEFAULT_MAX_BATCH_BYTES;

  public BrokerConfig {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(node, "node");
    Objects.requireNonNull(registration, "registration");
    Objects.requireNonNull(engine, "engine");
    backpressure = backpressure == null ? BackpressureConfiguration.defaults() : backpressure;
    backpressure.validate();
    if (engine.snapshot().logSizeThresholdBytes() > 0
        && engine.snapshot().logSizeThresholdBytes() < engine.processing().maxBatchBytes()) {
      throw new IllegalArgumentException(
          "snapshot logSizeThresholdBytes must be zero or at least maxBatchBytes");
    }
  }

  public String clusterId() {
    return identity.clusterId();
  }

  public String brokerId() {
    return identity.brokerId();
  }

  public String version() {
    return identity.version();
  }

  public Network network() {
    return node.network();
  }

  public Path dataDirectory() {
    return node.dataDirectory();
  }

  public Snapshot snapshot() {
    return engine.snapshot();
  }

  public Consensus consensus() {
    return engine.consensus();
  }

  public Processing processing() {
    return engine.processing();
  }

  public record Identity(String clusterId, String brokerId, String version) {
    public Identity {
      Objects.requireNonNull(clusterId, "clusterId");
      Objects.requireNonNull(brokerId, "brokerId");
      Objects.requireNonNull(version, "version");
    }
  }

  public record Node(Network network, Path dataDirectory) {
    public Node {
      Objects.requireNonNull(network, "network");
      Objects.requireNonNull(dataDirectory, "dataDirectory");
    }
  }

  public record Engine(
      Snapshot snapshot,
      Consensus consensus,
      Processing processing,
      boolean workerJobLeaseTokenEnabled) {
    public Engine(Snapshot snapshot, Consensus consensus, Processing processing) {
      this(snapshot, consensus, processing, true);
    }

    public Engine {
      Objects.requireNonNull(snapshot, "snapshot");
      Objects.requireNonNull(consensus, "consensus");
      Objects.requireNonNull(processing, "processing");
    }
  }

  public record Network(String host, int commandPort, int raftPort) {
    public Network {
      Objects.requireNonNull(host, "host");
      host = HostPort.normalizeHost(host);
      if (host.isBlank()) {
        throw new IllegalArgumentException("host must not be blank");
      }
      validatePort(commandPort, "commandPort");
      validatePort(raftPort, "raftPort");
      if (commandPort == raftPort) {
        throw new IllegalArgumentException("raftPort must be different from the business API port");
      }
    }

    private static void validatePort(final int port, final String name) {
      if (port <= 0 || port > 65_535) {
        throw new IllegalArgumentException(name + " must be between 1 and 65535");
      }
    }
  }

  /**
   * PD registration policy.
   *
   * @param renewInterval delay between completed renewals
   * @param renewalTimeout local PD RPC timeout, also reported during registration; PD controls the
   *     offline lease independently
   */
  public record Registration(
      String placementDrivers,
      Duration renewInterval,
      Duration renewalTimeout,
      Duration topologyRefreshInterval,
      Backpressure backpressure) {
    public Registration(
        final String placementDrivers,
        final Duration renewInterval,
        final Duration renewalTimeout,
        final Duration topologyRefreshInterval) {
      this(
          placementDrivers,
          renewInterval,
          renewalTimeout,
          topologyRefreshInterval,
          Backpressure.defaults());
    }

    public Registration {
      placementDrivers = placementDrivers == null ? "" : placementDrivers.trim();
      requirePositive(renewInterval, "renewInterval");
      requirePositive(renewalTimeout, "renewalTimeout");
      requirePositive(topologyRefreshInterval, "topologyRefreshInterval");
      backpressure = backpressure == null ? Backpressure.defaults() : backpressure;
      if (renewInterval.compareTo(renewalTimeout) >= 0) {
        throw new IllegalArgumentException("renewInterval must be less than renewalTimeout");
      }
    }
  }

  public record Backpressure(
      Duration minDelay, Duration maxDelay, double backoffFactor, double jitterFactor) {
    public Backpressure {
      requirePositive(minDelay, "backpressure.minDelay");
      requirePositive(maxDelay, "backpressure.maxDelay");
      if (minDelay.compareTo(maxDelay) > 0) {
        throw new IllegalArgumentException("backpressure.minDelay must not exceed maxDelay");
      }
      if (!Double.isFinite(backoffFactor) || backoffFactor < 1) {
        throw new IllegalArgumentException(
            "backpressure.backoffFactor must be finite and at least 1");
      }
      if (!Double.isFinite(jitterFactor) || jitterFactor < 0 || jitterFactor > 1) {
        throw new IllegalArgumentException(
            "backpressure.jitterFactor must be finite and between 0 and 1");
      }
    }

    public static Backpressure defaults() {
      return new Backpressure(
          Duration.ofMillis(ExponentialBackoff.DEFAULT_MIN_DELAY),
          Duration.ofMillis(ExponentialBackoff.DEFAULT_MAX_DELAY),
          ExponentialBackoff.DEFAULT_BACKOFF_FACTOR,
          ExponentialBackoff.DEFAULT_JITTER_FACTOR);
    }
  }

  public record Snapshot(Duration interval, long logSizeThresholdBytes) {
    public Snapshot {
      Objects.requireNonNull(interval, "interval");
      if (interval.isNegative()) {
        throw new IllegalArgumentException("snapshot interval must not be negative");
      }
      if (logSizeThresholdBytes < 0) {
        throw new IllegalArgumentException("snapshot logSizeThresholdBytes must not be negative");
      }
      if (interval.isZero() && logSizeThresholdBytes == 0) {
        throw new IllegalArgumentException(
            "snapshot interval and logSizeThresholdBytes must not both be zero");
      }
    }

    public SnapshotPolicy policy() {
      return SnapshotPolicy.of(interval, logSizeThresholdBytes);
    }
  }

  public record Consensus(Duration electionTimeout) {
    public Consensus {
      requirePositive(electionTimeout, "electionTimeout");
      if (electionTimeout.toMillis() > Integer.MAX_VALUE) {
        throw new IllegalArgumentException(
            "electionTimeout must not exceed " + Integer.MAX_VALUE + "ms");
      }
    }
  }

  public record Processing(
      int maxCommandsInBatch, int maxRecordsInBatch, Duration maxBatchDuration, int maxBatchBytes) {
    public Processing {
      Objects.requireNonNull(maxBatchDuration, "maxBatchDuration");
      if (maxCommandsInBatch < 1) {
        throw new IllegalArgumentException("maxCommandsInBatch must be at least 1");
      }
      if (maxRecordsInBatch < 0) {
        throw new IllegalArgumentException("maxRecordsInBatch must not be negative");
      }
      if (maxBatchDuration.isNegative()) {
        throw new IllegalArgumentException("maxBatchDuration must not be negative");
      }
      if (maxBatchBytes < StreamPartitionConfig.MIN_MAX_BATCH_BYTES) {
        throw new IllegalArgumentException(
            "maxBatchBytes must be at least " + StreamPartitionConfig.MIN_MAX_BATCH_BYTES);
      }
    }
  }

  public static Processing defaultProcessing() {
    return new Processing(
        DEFAULT_MAX_COMMANDS_IN_BATCH,
        DEFAULT_MAX_RECORDS_IN_BATCH,
        DEFAULT_MAX_BATCH_DURATION,
        DEFAULT_MAX_BATCH_BYTES);
  }

  private static void requirePositive(final Duration duration, final String name) {
    Objects.requireNonNull(duration, name);
    if (duration.isNegative() || duration.toMillis() < 1) {
      throw new IllegalArgumentException(name + " must be at least 1ms");
    }
  }
}
