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
package io.taskite.dist.broker;

import io.taskite.broker.BrokerConfig;
import io.taskite.streamplatform.api.backpressure.BackpressureConfiguration;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.ConstructorBinding;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;

/** Spring binding adapter; creates immutable runtime configuration after binding. */
@ConfigurationProperties(prefix = "taskite.broker", ignoreUnknownFields = false)
public final class BrokerConfiguration {
  private boolean enabled = true;
  private Cluster cluster = new Cluster("", "broker-0");
  private Network network = Network.defaults();
  private Registration registration = Registration.defaults();
  private Storage storage = Storage.defaults();
  private Consensus consensus = Consensus.defaults();
  private Processing processing = Processing.defaults();
  private BackpressureConfiguration backpressure = BackpressureConfiguration.defaults();
  private String version = "0.1.0-SNAPSHOT";
  private WorkerJobLeaseToken workerJobLeaseToken = new WorkerJobLeaseToken(true);

  public WorkerJobLeaseToken getWorkerJobLeaseToken() {
    return workerJobLeaseToken;
  }

  public void setWorkerJobLeaseToken(WorkerJobLeaseToken value) {
    workerJobLeaseToken = Objects.requireNonNull(value);
  }

  public record WorkerJobLeaseToken(boolean enabled) {
    public WorkerJobLeaseToken(@DefaultValue("true") boolean enabled) {
      this.enabled = enabled;
    }
  }

  /** Framework-only binding boundary retains the documented YAML property names. */
  public BrokerConfiguration() {}

  public BrokerConfiguration(final Settings settings) {
    enabled = settings.enabled();
    cluster = new Cluster(settings.identity().clusterId(), settings.identity().brokerId());
    version = settings.identity().version();
    network = settings.node().network();
    storage = settings.node().storage();
    registration = settings.registration();
    consensus = settings.engine().consensus();
    processing = settings.engine().processing();
  }

  public boolean enabled() {
    return enabled;
  }

  public Cluster cluster() {
    return cluster;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(final boolean enabled) {
    this.enabled = enabled;
  }

  public Cluster getCluster() {
    return cluster;
  }

  public void setCluster(final Cluster cluster) {
    this.cluster = cluster;
  }

  public Network getNetwork() {
    return network;
  }

  public void setNetwork(final Network network) {
    this.network = network;
  }

  public Registration getRegistration() {
    return registration;
  }

  public void setRegistration(final Registration registration) {
    this.registration = registration;
  }

  public Storage getStorage() {
    return storage;
  }

  public void setStorage(final Storage storage) {
    this.storage = storage;
  }

  public Consensus getConsensus() {
    return consensus;
  }

  public void setConsensus(final Consensus consensus) {
    this.consensus = consensus;
  }

  public Processing getProcessing() {
    return processing;
  }

  public void setProcessing(final Processing processing) {
    this.processing = processing;
  }

  public String getVersion() {
    return version;
  }

  public BackpressureConfiguration getBackpressure() {
    return backpressure;
  }

  public void setBackpressure(final BackpressureConfiguration backpressure) {
    this.backpressure = backpressure;
  }

  public void setVersion(final String version) {
    this.version = version;
  }

  public BrokerConfig toBrokerConfig() {
    if (cluster.clusterId() == null || cluster.clusterId().isBlank()) {
      throw new IllegalArgumentException("taskite.broker.cluster.cluster-id must not be blank");
    }
    if (registration.placementDrivers() == null || registration.placementDrivers().isBlank()) {
      throw new IllegalArgumentException(
          "taskite.broker.registration.placement-drivers must not be blank");
    }
    return new BrokerConfig(
        new BrokerConfig.Identity(cluster.clusterId(), cluster.brokerId(), version),
        new BrokerConfig.Node(
            new BrokerConfig.Network(network.host(), network.commandPort(), network.raftPort()),
            storage.dataDir()),
        new BrokerConfig.Registration(
            registration.placementDrivers(),
            registration.renewInterval(),
            registration.renewalTimeout(),
            registration.topologyRefreshInterval(),
            registration.backpressure().toBackpressure()),
        new BrokerConfig.Engine(
            new BrokerConfig.Snapshot(
                storage.snapshot().interval(), storage.snapshot().logSizeThreshold().toBytes()),
            new BrokerConfig.Consensus(consensus.electionTimeout()),
            new BrokerConfig.Processing(
                processing.maxCommandsInBatch(),
                processing.maxRecordsInBatch(),
                processing.maxBatchDuration(),
                processing.maxBatchBytes()),
            workerJobLeaseToken.enabled()),
        backpressure);
  }

  public record Settings(
      boolean enabled,
      BrokerConfig.Identity identity,
      Node node,
      Registration registration,
      Engine engine) {
    public Settings {
      Objects.requireNonNull(identity, "identity");
      node = node == null ? new Node(null, null) : node;
      registration = registration == null ? Registration.defaults() : registration;
      engine = engine == null ? new Engine(null, null) : engine;
    }
  }

  public record Node(Network network, Storage storage) {
    public Node {
      network = network == null ? Network.defaults() : network;
      storage = storage == null ? Storage.defaults() : storage;
    }
  }

  public record Engine(Consensus consensus, Processing processing) {
    public Engine {
      consensus = consensus == null ? Consensus.defaults() : consensus;
      processing = processing == null ? Processing.defaults() : processing;
    }
  }

  public record Network(String host, int commandPort, int raftPort) {
    public Network(
        @DefaultValue("127.0.0.1") final String host,
        @DefaultValue("26501") final int commandPort,
        @DefaultValue("26502") final int raftPort) {
      this.host = host;
      this.commandPort = commandPort;
      this.raftPort = raftPort;
    }

    private static Network defaults() {
      return new Network("127.0.0.1", 26501, 26502);
    }
  }

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

    @ConstructorBinding
    public Registration(
        @DefaultValue("") final String placementDrivers,
        @DefaultValue("1s") final Duration renewInterval,
        @DefaultValue("30s") final Duration renewalTimeout,
        @DefaultValue("5s") final Duration topologyRefreshInterval,
        @DefaultValue final Backpressure backpressure) {
      this.placementDrivers = placementDrivers;
      this.renewInterval = renewInterval;
      this.renewalTimeout = renewalTimeout;
      this.topologyRefreshInterval = topologyRefreshInterval;
      this.backpressure = backpressure == null ? Backpressure.defaults() : backpressure;
    }

    private static Registration defaults() {
      return new Registration(
          "", Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofSeconds(5));
    }
  }

  public record Backpressure(
      Duration minDelay, Duration maxDelay, double backoffFactor, double jitterFactor) {
    public Backpressure(
        @DefaultValue("50ms") final Duration minDelay,
        @DefaultValue("5s") final Duration maxDelay,
        @DefaultValue("1.6") final double backoffFactor,
        @DefaultValue("0.1") final double jitterFactor) {
      this.minDelay = minDelay;
      this.maxDelay = maxDelay;
      this.backoffFactor = backoffFactor;
      this.jitterFactor = jitterFactor;
    }

    public BrokerConfig.Backpressure toBackpressure() {
      return new BrokerConfig.Backpressure(minDelay, maxDelay, backoffFactor, jitterFactor);
    }

    private static Backpressure defaults() {
      final var defaults = BrokerConfig.Backpressure.defaults();
      return new Backpressure(
          defaults.minDelay(),
          defaults.maxDelay(),
          defaults.backoffFactor(),
          defaults.jitterFactor());
    }
  }

  public record Cluster(String clusterId, String brokerId) {
    public Cluster(
        @DefaultValue("") final String clusterId, @DefaultValue("broker-0") final String brokerId) {
      this.clusterId = clusterId;
      this.brokerId = brokerId;
    }
  }

  public record Storage(Path dataDir, Snapshot snapshot) {
    public Storage(
        @DefaultValue("target/broker-data") final Path dataDir,
        @DefaultValue final Snapshot snapshot) {
      this.dataDir = dataDir;
      this.snapshot = snapshot == null ? Snapshot.defaults() : snapshot;
    }

    private static Storage defaults() {
      return new Storage(Path.of("target/broker-data"), Snapshot.defaults());
    }
  }

  public record Snapshot(Duration interval, DataSize logSizeThreshold) {
    public Snapshot(
        @DefaultValue("15m") final Duration interval,
        @DefaultValue("128MB") final DataSize logSizeThreshold) {
      this.interval = interval;
      this.logSizeThreshold = logSizeThreshold;
    }

    private static Snapshot defaults() {
      return new Snapshot(
          BrokerConfig.DEFAULT_SNAPSHOT_INTERVAL,
          DataSize.ofBytes(BrokerConfig.DEFAULT_SNAPSHOT_LOG_SIZE_THRESHOLD_BYTES));
    }
  }

  public record Consensus(Duration electionTimeout) {
    public Consensus(@DefaultValue("5s") final Duration electionTimeout) {
      this.electionTimeout = electionTimeout;
    }

    private static Consensus defaults() {
      return new Consensus(BrokerConfig.DEFAULT_ELECTION_TIMEOUT);
    }
  }

  public record Processing(
      int maxCommandsInBatch, int maxRecordsInBatch, Duration maxBatchDuration, int maxBatchBytes) {
    public Processing(
        @DefaultValue("1") final int maxCommandsInBatch,
        @DefaultValue("0") final int maxRecordsInBatch,
        @DefaultValue("0ms") final Duration maxBatchDuration,
        @DefaultValue("3145728") final int maxBatchBytes) {
      this.maxCommandsInBatch = maxCommandsInBatch;
      this.maxRecordsInBatch = maxRecordsInBatch;
      this.maxBatchDuration = maxBatchDuration;
      this.maxBatchBytes = maxBatchBytes;
    }

    private static Processing defaults() {
      return new Processing(
          BrokerConfig.DEFAULT_MAX_COMMANDS_IN_BATCH,
          BrokerConfig.DEFAULT_MAX_RECORDS_IN_BATCH,
          BrokerConfig.DEFAULT_MAX_BATCH_DURATION,
          BrokerConfig.DEFAULT_MAX_BATCH_BYTES);
    }
  }
}
