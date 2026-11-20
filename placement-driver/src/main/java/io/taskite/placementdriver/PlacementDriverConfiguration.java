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
package io.taskite.placementdriver;

import io.taskite.broker.client.BrokerClientConfig;
import io.taskite.snapshot.SnapshotPolicy;
import io.taskite.streamplatform.api.backpressure.BackpressureConfiguration;
import io.taskite.streamplatform.runtime.StreamPartitionConfig;
import io.taskite.util.HostPort;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.ConstructorBinding;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;

/**
 * Immutable placement-driver settings bound by Spring Boot from the {@code taskite.pd} namespace.
 *
 * <p>{@code grpcPort} serves control-plane clients. {@code raftPort} is an independent PD-to-PD
 * consensus endpoint; no port offset is inferred.
 */
@ConfigurationProperties(prefix = "taskite.pd", ignoreUnknownFields = false)
public final class PlacementDriverConfiguration {
  public static final int DEFAULT_CHANGE_HISTORY_LIMIT = 50;
  public static final Duration DEFAULT_BROKER_LEASE_TIMEOUT = Duration.ofSeconds(30);
  public static final Duration DEFAULT_WORKER_LEASE_TIMEOUT = Duration.ofSeconds(30);
  public static final Duration DEFAULT_WORKER_OFFLINE_RETENTION = Duration.ofMinutes(10);
  public static final Duration DEFAULT_SNAPSHOT_INTERVAL = SnapshotPolicy.DEFAULT_INTERVAL;
  public static final long DEFAULT_SNAPSHOT_LOG_SIZE_THRESHOLD_BYTES =
      SnapshotPolicy.DEFAULT_LOG_SIZE_THRESHOLD_BYTES;

  private final String nodeId;
  private final Network network;
  private final Consensus consensus;
  private final Storage storage;
  private final int changeHistoryLimit;
  private final BrokerLeaseConfiguration broker;
  private final WorkerLeaseConfiguration worker;
  private final BackpressureConfiguration backpressure;

  @ConstructorBinding
  public PlacementDriverConfiguration(
      @DefaultValue("meta-0") final String nodeId,
      @DefaultValue final Network network,
      @DefaultValue final Consensus consensus,
      @DefaultValue final Storage storage,
      @DefaultValue("50") final int changeHistoryLimit,
      @DefaultValue final BrokerLeaseConfiguration broker,
      @DefaultValue final WorkerLeaseConfiguration worker,
      @DefaultValue final BackpressureConfiguration backpressure) {
    this.nodeId = nodeId;
    this.network = network == null ? Network.defaults() : network;
    this.consensus = consensus == null ? Consensus.defaults() : consensus;
    final Storage configuredStorage = storage == null ? Storage.defaults() : storage;
    this.storage =
        new Storage(
            configuredStorage.dataDir() == null
                ? Path.of("data/meta-cluster/" + defaultNodeId(nodeId))
                : configuredStorage.dataDir(),
            configuredStorage.snapshot());
    this.changeHistoryLimit = changeHistoryLimit;
    this.broker = broker == null ? BrokerLeaseConfiguration.defaults() : broker;
    this.worker = worker == null ? WorkerLeaseConfiguration.defaults() : worker;
    this.backpressure = backpressure == null ? BackpressureConfiguration.defaults() : backpressure;
  }

  public String nodeId() {
    return nodeId;
  }

  public Network network() {
    return network;
  }

  public Consensus consensus() {
    return consensus;
  }

  public Storage storage() {
    return storage;
  }

  public int changeHistoryLimit() {
    return changeHistoryLimit;
  }

  public BrokerLeaseConfiguration broker() {
    return broker;
  }

  public WorkerLeaseConfiguration worker() {
    return worker;
  }

  public RegistrationPolicy registrationPolicy() {
    return new RegistrationPolicy(
        broker.leaseTimeout(), worker.leaseTimeout(), worker.offlineRetention());
  }

  public BackpressureConfiguration backpressure() {
    return backpressure;
  }

  public void validate() {
    backpressure.validate();
    if (nodeId == null || nodeId.isBlank()) {
      throw new IllegalArgumentException("placement-driver nodeId must not be blank");
    }
    if (network == null || network.host() == null || network.host().isBlank()) {
      throw new IllegalArgumentException("placement-driver host must not be blank");
    }
    if (network.grpcPort() < 0 || network.grpcPort() > 65_535) {
      throw new IllegalArgumentException("placement-driver grpcPort must be in range 0..65535");
    }
    if (network.raftPort() <= 0 || network.raftPort() > 65_535) {
      throw new IllegalArgumentException("placement-driver raftPort must be in range 1..65535");
    }
    if (network.grpcPort() == network.raftPort()) {
      throw new IllegalArgumentException(
          "placement-driver raftPort must be different from grpcPort");
    }
    if (consensus == null
        || consensus.electionTimeout() == null
        || consensus.heartbeatInterval() == null
        || consensus.electionTimeout().isNegative()
        || consensus.electionTimeout().toMillis() < 1
        || consensus.heartbeatInterval().isNegative()
        || consensus.heartbeatInterval().toMillis() < 1) {
      throw new IllegalArgumentException(
          "placement-driver election and heartbeat intervals must be at least 1ms");
    }
    if (consensus.electionTimeout().compareTo(consensus.heartbeatInterval()) <= 0) {
      throw new IllegalArgumentException(
          "placement-driver election timeout must exceed heartbeat interval");
    }
    storage.snapshot().policy();
    broker.client().validate();
    if (changeHistoryLimit <= 0) {
      throw new IllegalArgumentException(
          "placement-driver changeHistoryLimit must be greater than zero");
    }
    if (broker == null
        || broker.leaseTimeout() == null
        || broker.leaseTimeout().isNegative()
        || broker.leaseTimeout().toMillis() < 1) {
      throw new IllegalArgumentException(
          "placement-driver broker lease timeout must be greater than zero");
    }
    if (worker == null
        || worker.leaseTimeout() == null
        || worker.leaseTimeout().isNegative()
        || worker.leaseTimeout().toMillis() < 1) {
      throw new IllegalArgumentException(
          "placement-driver worker lease timeout must be greater than zero");
    }
    if (worker.offlineRetention() == null || worker.offlineRetention().isNegative()) {
      throw new IllegalArgumentException(
          "placement-driver worker offline retention must not be negative");
    }
    if (storage.dataDir() == null) {
      throw new IllegalArgumentException("placement-driver dataDir must not be null");
    }
    final List<PlacementDriverPeer> peers = consensus.bootstrapPeers();
    final Set<String> peerIds = new HashSet<>();
    for (final PlacementDriverPeer peer : peers) {
      if (peer.memberId() == null || peer.memberId().isBlank()) {
        throw new IllegalArgumentException("placement-driver peer nodeId must not be blank");
      }
      if (peer.host() == null || peer.host().isBlank()) {
        throw new IllegalArgumentException(
            "placement-driver peer host must not be blank: " + peer.memberId());
      }
      if (peer.grpcPort() <= 0 || peer.grpcPort() > 65_535) {
        throw new IllegalArgumentException(
            "placement-driver peer grpcPort must be in range 1..65535: " + peer.memberId());
      }
      if (peer.raftPort() <= 0 || peer.raftPort() > 65_535) {
        throw new IllegalArgumentException(
            "placement-driver peer raftPort must be in range 1..65535: " + peer.memberId());
      }
      if (peer.grpcPort() == peer.raftPort()) {
        throw new IllegalArgumentException(
            "placement-driver peer grpcPort and raftPort must differ: " + peer.memberId());
      }
      if (nodeId.equals(peer.memberId())) {
        throw new IllegalArgumentException(
            "placement-driver peers must not contain the local node");
      }
      if (!peerIds.add(peer.memberId())) {
        throw new IllegalArgumentException(
            "placement-driver peers contain duplicate node id: " + peer.memberId());
      }
    }
    if (!peers.isEmpty()
        && isLoopback(network.host())
        && peers.stream().anyMatch(peer -> !isLoopback(peer.host()))) {
      throw new IllegalArgumentException(
          "placement-driver host must be reachable by remote peers; "
              + "a loopback taskite.pd.network.host cannot be mixed with non-loopback "
              + "TASKITE_PD_CONSENSUS_BOOTSTRAP_PEERS");
    }
    if (!peers.isEmpty() && isWildcard(network.host())) {
      throw new IllegalArgumentException(
          "placement-driver host must be an advertised address, not a wildcard address");
    }
  }

  private static boolean isLoopback(final String host) {
    final String normalized = host == null ? "" : host.trim().toLowerCase(Locale.ROOT);
    return normalized.equals("localhost")
        || normalized.equals("::1")
        || normalized.equals("[::1]")
        || normalized.startsWith("127.");
  }

  private static boolean isWildcard(final String host) {
    final String normalized = host == null ? "" : host.trim().toLowerCase(Locale.ROOT);
    return normalized.equals("0.0.0.0") || normalized.equals("::") || normalized.equals("[::]");
  }

  private static List<PlacementDriverPeer> parsePeers(final String raw) {
    if (raw == null || raw.isBlank()) {
      return List.of();
    }
    final List<PlacementDriverPeer> parsed = new ArrayList<>();
    for (final String rawToken : raw.split(",")) {
      final String token = rawToken.trim();
      final String[] parts = token.split("@", 2);
      if (parts.length != 2) {
        throw invalidPeer(token);
      }
      final int raftPortSeparator = parts[1].lastIndexOf(':');
      final int grpcPortSeparator =
          raftPortSeparator <= 0 ? -1 : parts[1].lastIndexOf(':', raftPortSeparator - 1);
      if (parts[0].isBlank()
          || grpcPortSeparator <= 0
          || raftPortSeparator == parts[1].length() - 1) {
        throw invalidPeer(token);
      }
      final String nodeId = parts[0].trim();
      final String host = parts[1].substring(0, grpcPortSeparator).trim();
      final String grpcPort = parts[1].substring(grpcPortSeparator + 1, raftPortSeparator).trim();
      final String raftPort = parts[1].substring(raftPortSeparator + 1).trim();
      if (nodeId.isBlank() || host.isBlank() || grpcPort.isBlank() || raftPort.isBlank()) {
        throw invalidPeer(token);
      }
      try {
        parsed.add(
            new PlacementDriverPeer(
                nodeId, host, Integer.parseInt(grpcPort), Integer.parseInt(raftPort)));
      } catch (final NumberFormatException error) {
        throw invalidPeer(token);
      }
    }
    return sortedPeers(parsed);
  }

  private static List<PlacementDriverPeer> sortedPeers(final List<PlacementDriverPeer> peers) {
    final List<PlacementDriverPeer> sorted = new ArrayList<>(peers == null ? List.of() : peers);
    sorted.sort(Comparator.comparing(PlacementDriverPeer::memberId));
    return List.copyOf(sorted);
  }

  private static IllegalArgumentException invalidPeer(final String peer) {
    return new IllegalArgumentException(
        "peer must use nodeId@host:grpcPort:raftPort format: " + peer);
  }

  private static String defaultNodeId(final String nodeId) {
    return nodeId == null || nodeId.isBlank() ? "meta-0" : nodeId;
  }

  public record Network(String host, int grpcPort, int raftPort) {
    public Network(
        @DefaultValue("127.0.0.1") final String host,
        @DefaultValue("26501") final int grpcPort,
        @DefaultValue("26502") final int raftPort) {
      this.host = host == null ? null : HostPort.normalizeHost(host);
      this.grpcPort = grpcPort;
      this.raftPort = raftPort;
    }

    static Network defaults() {
      return new Network("127.0.0.1", 26501, 26502);
    }
  }

  public record Consensus(
      Duration electionTimeout,
      Duration heartbeatInterval,
      List<PlacementDriverPeer> bootstrapPeers) {
    @ConstructorBinding
    public Consensus(
        @DefaultValue("1200ms") final Duration electionTimeout,
        @DefaultValue("300ms") final Duration heartbeatInterval,
        @DefaultValue("") final String bootstrapPeers) {
      this(electionTimeout, heartbeatInterval, parsePeers(bootstrapPeers));
    }

    public Consensus(final Duration electionTimeout, final Duration heartbeatInterval) {
      this(electionTimeout, heartbeatInterval, List.of());
    }

    public Consensus {
      bootstrapPeers = sortedPeers(bootstrapPeers);
    }

    static Consensus defaults() {
      return new Consensus(Duration.ofMillis(1200), Duration.ofMillis(300));
    }
  }

  public record Storage(Path dataDir, Snapshot snapshot) {
    @ConstructorBinding
    public Storage(@DefaultValue("") final String dataDir, @DefaultValue final Snapshot snapshot) {
      this(dataDir == null || dataDir.isBlank() ? null : Path.of(dataDir), snapshot);
    }

    public Storage(final Path dataDir, final Snapshot snapshot) {
      this.dataDir = dataDir == null || dataDir.toString().isBlank() ? null : dataDir;
      this.snapshot = snapshot == null ? Snapshot.defaults() : snapshot;
    }

    static Storage defaults() {
      return new Storage((Path) null, Snapshot.defaults());
    }
  }

  public record Snapshot(Duration interval, DataSize logSizeThreshold) {
    public Snapshot(
        @DefaultValue("15m") final Duration interval,
        @DefaultValue("128MB") final DataSize logSizeThreshold) {
      this.interval = interval;
      this.logSizeThreshold = logSizeThreshold;
    }

    public SnapshotPolicy policy() {
      if (logSizeThreshold == null) {
        throw new IllegalArgumentException(
            "placement-driver snapshot log size threshold must not be null");
      }
      final long thresholdBytes = logSizeThreshold.toBytes();
      if (thresholdBytes > 0 && thresholdBytes < StreamPartitionConfig.DEFAULT_MAX_BATCH_BYTES) {
        throw new IllegalArgumentException(
            "placement-driver snapshot log size threshold must be zero or at least "
                + StreamPartitionConfig.DEFAULT_MAX_BATCH_BYTES
                + " bytes");
      }
      return SnapshotPolicy.of(interval, thresholdBytes);
    }

    static Snapshot defaults() {
      return new Snapshot(
          DEFAULT_SNAPSHOT_INTERVAL, DataSize.ofBytes(DEFAULT_SNAPSHOT_LOG_SIZE_THRESHOLD_BYTES));
    }
  }

  public record BrokerLeaseConfiguration(Duration leaseTimeout, BrokerClientConfiguration client) {
    @ConstructorBinding
    public BrokerLeaseConfiguration(
        @DefaultValue("30s") final Duration leaseTimeout,
        @DefaultValue final BrokerClientConfiguration client) {
      this.leaseTimeout = leaseTimeout;
      this.client = client == null ? BrokerClientConfiguration.defaults() : client;
    }

    public BrokerLeaseConfiguration(final Duration leaseTimeout) {
      this(leaseTimeout, BrokerClientConfiguration.defaults());
    }

    static BrokerLeaseConfiguration defaults() {
      return new BrokerLeaseConfiguration(DEFAULT_BROKER_LEASE_TIMEOUT);
    }
  }

  public record BrokerClientConfiguration(Duration requestTimeout, int maxRetries) {
    public BrokerClientConfiguration(
        @DefaultValue("5s") final Duration requestTimeout,
        @DefaultValue("2") final int maxRetries) {
      this.requestTimeout = requestTimeout;
      this.maxRetries = maxRetries;
    }

    public void validate() {
      if (requestTimeout == null || requestTimeout.isNegative() || requestTimeout.toMillis() < 1) {
        throw new IllegalArgumentException(
            "taskite.pd.broker.client.request-timeout must be at least 1ms");
      }
      if (maxRetries < 0) {
        throw new IllegalArgumentException(
            "taskite.pd.broker.client.max-retries must not be negative");
      }
    }

    static BrokerClientConfiguration defaults() {
      return new BrokerClientConfiguration(
          BrokerClientConfig.DEFAULT_REQUEST_TIMEOUT, BrokerClientConfig.DEFAULT_MAX_RETRIES);
    }
  }

  public record WorkerLeaseConfiguration(Duration leaseTimeout, Duration offlineRetention) {
    @ConstructorBinding
    public WorkerLeaseConfiguration(
        @DefaultValue("30s") final Duration leaseTimeout,
        @DefaultValue("10m") final Duration offlineRetention) {
      this.leaseTimeout = leaseTimeout;
      this.offlineRetention = offlineRetention;
    }

    static WorkerLeaseConfiguration defaults() {
      return new WorkerLeaseConfiguration(
          DEFAULT_WORKER_LEASE_TIMEOUT, DEFAULT_WORKER_OFFLINE_RETENTION);
    }
  }
}
