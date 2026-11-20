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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.taskite.streamplatform.api.backpressure.BackpressureConfiguration;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.SimpleCommandLinePropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.util.unit.DataSize;

final class PlacementDriverConfigurationTest {
  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner().withUserConfiguration(TestConfiguration.class);

  @Test
  void partialAndDisabledBackpressureUseSharedDefaults() {
    contextRunner
        .withPropertyValues("taskite.pd.backpressure.aimd.initial-limit=5")
        .run(
            context -> {
              final var config = context.getBean(PlacementDriverConfiguration.class);
              assertEquals(5, config.backpressure().aimd().initialLimit());
              assertEquals(1, config.backpressure().aimd().minLimit());
              assertEquals(true, config.backpressure().enabled());
              config.validate();
            });
    contextRunner
        .withPropertyValues("taskite.pd.backpressure.enabled=false")
        .run(
            context -> {
              final var config = context.getBean(PlacementDriverConfiguration.class);
              assertEquals(false, config.backpressure().enabled());
              assertEquals(100, config.backpressure().aimd().initialLimit());
            });
  }

  @Test
  void shouldBindBackpressureAndRejectInvalidParameters() {
    contextRunner
        .withPropertyValues(
            "taskite.pd.backpressure.algorithm=vegas",
            "taskite.pd.backpressure.vegas.initial-limit=33",
            "taskite.pd.backpressure.vegas.alpha=4",
            "taskite.pd.backpressure.vegas.beta=8")
        .run(
            context -> {
              final var config = context.getBean(PlacementDriverConfiguration.class);
              config.validate();
              assertEquals(
                  BackpressureConfiguration.Algorithm.VEGAS, config.backpressure().algorithm());
              assertEquals(33, config.backpressure().vegas().initialLimit());
              assertEquals(1000, config.backpressure().vegas().maxLimit());
              assertEquals(true, config.backpressure().enabled());
            });
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new BackpressureConfiguration(
                    true,
                    BackpressureConfiguration.Algorithm.AIMD,
                    new BackpressureConfiguration.Aimd(Duration.ZERO, null, null, null, null),
                    null,
                    null)
                .validate());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new BackpressureConfiguration(
                    true,
                    BackpressureConfiguration.Algorithm.AIMD,
                    new BackpressureConfiguration.Aimd(Duration.ofMillis(200), 10, 20, 100, 0.9),
                    null,
                    null)
                .validate());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new BackpressureConfiguration(
                    true,
                    BackpressureConfiguration.Algorithm.VEGAS,
                    null,
                    new BackpressureConfiguration.Vegas(20, 6, 3, 100),
                    null)
                .validate());
  }

  @Test
  void shouldApplyDefaults() {
    contextRunner.run(
        context -> {
          final PlacementDriverConfiguration configuration =
              context.getBean(PlacementDriverConfiguration.class);

          assertEquals("meta-0", configuration.nodeId());
          assertEquals("127.0.0.1", configuration.network().host());
          assertEquals(26501, configuration.network().grpcPort());
          assertEquals(26502, configuration.network().raftPort());
          assertEquals(Duration.ofMillis(1200), configuration.consensus().electionTimeout());
          assertEquals(Duration.ofMillis(300), configuration.consensus().heartbeatInterval());
          assertEquals(Duration.ofMinutes(15), configuration.storage().snapshot().interval());
          assertEquals(
              DataSize.ofMegabytes(128), configuration.storage().snapshot().logSizeThreshold());
          assertEquals(50, configuration.changeHistoryLimit());
          assertEquals(Duration.ofSeconds(30), configuration.broker().leaseTimeout());
          assertEquals(Duration.ofSeconds(30), configuration.worker().leaseTimeout());
          assertEquals(Duration.ofMinutes(10), configuration.worker().offlineRetention());
          assertEquals(Path.of("data/meta-cluster/meta-0"), configuration.storage().dataDir());
          assertEquals(List.of(), configuration.consensus().bootstrapPeers());
          assertEquals(Duration.ofSeconds(5), configuration.broker().client().requestTimeout());
          assertEquals(2, configuration.broker().client().maxRetries());
        });
  }

  @Test
  void shouldRejectInvalidSnapshotPolicy() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PlacementDriverConfiguration.Snapshot(Duration.ZERO, DataSize.ofBytes(0)).policy());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PlacementDriverConfiguration.Snapshot(
                    Duration.ofMinutes(15), DataSize.ofMegabytes(1))
                .policy());
  }

  @Test
  void shouldAllowDisabledAndLargeSnapshotSizeThresholds() {
    assertEquals(
        0,
        new PlacementDriverConfiguration.Snapshot(Duration.ofMinutes(15), DataSize.ofBytes(0))
            .policy()
            .logSizeThresholdBytes());
    assertEquals(
        DataSize.ofGigabytes(1).toBytes(),
        new PlacementDriverConfiguration.Snapshot(Duration.ZERO, DataSize.ofGigabytes(1))
            .policy()
            .logSizeThresholdBytes());
  }

  @Test
  void shouldBindYamlConfiguration() throws Exception {
    final String yaml =
        """
        taskite:
          pd:
            node-id: meta-2
            network:
              host: 10.0.0.2
              grpc-port: 29202
              raft-port: 29302
            consensus:
              election-timeout: 1800ms
              heartbeat-interval: 400ms
              bootstrap-peers: "meta-1@10.0.0.1:29201:29301,meta-0@10.0.0.0:29200:29300"
            storage:
              data-dir: /var/lib/pd/meta-2
              snapshot:
                interval: 20m
                log-size-threshold: 256MB
            change-history-limit: 75
            broker:
              lease-timeout: 41s
              client:
                request-timeout: 8s
                max-retries: 4
            worker:
              lease-timeout: 42s
              offline-retention: 44s
        """;
    final StandardEnvironment environment = new StandardEnvironment();
    final ByteArrayResource resource =
        new ByteArrayResource(yaml.getBytes(StandardCharsets.UTF_8)) {
          @Override
          public String getFilename() {
            return "placement-driver.yaml";
          }
        };
    new YamlPropertySourceLoader()
        .load("yaml", resource)
        .forEach(environment.getPropertySources()::addLast);

    final PlacementDriverConfiguration configuration = bind(environment);

    assertEquals("meta-2", configuration.nodeId());
    assertEquals("10.0.0.2", configuration.network().host());
    assertEquals(29202, configuration.network().grpcPort());
    assertEquals(29302, configuration.network().raftPort());
    assertEquals(Duration.ofMillis(1800), configuration.consensus().electionTimeout());
    assertEquals(Duration.ofMillis(400), configuration.consensus().heartbeatInterval());
    assertEquals(Duration.ofMinutes(20), configuration.storage().snapshot().interval());
    assertEquals(DataSize.ofMegabytes(256), configuration.storage().snapshot().logSizeThreshold());
    assertEquals(75, configuration.changeHistoryLimit());
    assertEquals(Duration.ofSeconds(41), configuration.broker().leaseTimeout());
    assertEquals(Duration.ofSeconds(42), configuration.worker().leaseTimeout());
    assertEquals(Duration.ofSeconds(44), configuration.worker().offlineRetention());
    assertEquals(Path.of("/var/lib/pd/meta-2"), configuration.storage().dataDir());
    assertEquals(Duration.ofSeconds(8), configuration.broker().client().requestTimeout());
    assertEquals(4, configuration.broker().client().maxRetries());
    assertEquals(List.of("meta-0", "meta-1"), peerIds(configuration));
  }

  @Test
  void shouldBindEnvironmentVariables() throws Exception {
    final StandardEnvironment environment = new StandardEnvironment();
    environment
        .getPropertySources()
        .addFirst(
            new SystemEnvironmentPropertySource(
                "test-environment",
                Map.ofEntries(
                    Map.entry("TASKITE_PD_NODE_ID", "meta-env"),
                    Map.entry("TASKITE_PD_NETWORK_HOST", "10.1.0.1"),
                    Map.entry("TASKITE_PD_NETWORK_GRPC_PORT", "29301"),
                    Map.entry("TASKITE_PD_NETWORK_RAFT_PORT", "29401"),
                    Map.entry("TASKITE_PD_CONSENSUS_ELECTION_TIMEOUT", "2s"),
                    Map.entry("TASKITE_PD_CONSENSUS_HEARTBEAT_INTERVAL", "500ms"),
                    Map.entry("TASKITE_PD_STORAGE_SNAPSHOT_INTERVAL", "30m"),
                    Map.entry("TASKITE_PD_STORAGE_SNAPSHOT_LOG_SIZE_THRESHOLD", "192MB"),
                    Map.entry("TASKITE_PD_CHANGE_HISTORY_LIMIT", "25"),
                    Map.entry("TASKITE_PD_BROKER_LEASE_TIMEOUT", "51s"),
                    Map.entry("TASKITE_PD_WORKER_LEASE_TIMEOUT", "52s"),
                    Map.entry("TASKITE_PD_WORKER_OFFLINE_RETENTION", "54s"),
                    Map.entry("TASKITE_PD_STORAGE_DATA_DIR", "/var/lib/pd/meta-env"),
                    Map.entry(
                        "TASKITE_PD_CONSENSUS_BOOTSTRAP_PEERS",
                        "meta-other@10.1.0.2:29302:29402"))));
    final String yaml =
        """
        taskite:
          pd:
            node-id: ${TASKITE_PD_NODE_ID:meta-0}
            change-history-limit: ${TASKITE_PD_CHANGE_HISTORY_LIMIT:50}
            network:
              host: ${TASKITE_PD_NETWORK_HOST:127.0.0.1}
              grpc-port: ${TASKITE_PD_NETWORK_GRPC_PORT:26501}
              raft-port: ${TASKITE_PD_NETWORK_RAFT_PORT:26502}
            consensus:
              election-timeout: ${TASKITE_PD_CONSENSUS_ELECTION_TIMEOUT:1200ms}
              heartbeat-interval: ${TASKITE_PD_CONSENSUS_HEARTBEAT_INTERVAL:300ms}
              bootstrap-peers: ${TASKITE_PD_CONSENSUS_BOOTSTRAP_PEERS:}
            storage:
              data-dir: ${TASKITE_PD_STORAGE_DATA_DIR:}
              snapshot:
                interval: ${TASKITE_PD_STORAGE_SNAPSHOT_INTERVAL:15m}
                log-size-threshold: ${TASKITE_PD_STORAGE_SNAPSHOT_LOG_SIZE_THRESHOLD:128MB}
            broker:
              lease-timeout: ${TASKITE_PD_BROKER_LEASE_TIMEOUT:30s}
            worker:
              lease-timeout: ${TASKITE_PD_WORKER_LEASE_TIMEOUT:30s}
              offline-retention: ${TASKITE_PD_WORKER_OFFLINE_RETENTION:10m}
        """;
    final ByteArrayResource resource =
        new ByteArrayResource(yaml.getBytes(StandardCharsets.UTF_8)) {
          @Override
          public String getFilename() {
            return "application.yaml";
          }
        };
    new YamlPropertySourceLoader()
        .load("application", resource)
        .forEach(environment.getPropertySources()::addLast);

    final PlacementDriverConfiguration configuration = bind(environment);

    assertEquals("meta-env", configuration.nodeId());
    assertEquals("10.1.0.1", configuration.network().host());
    assertEquals(29301, configuration.network().grpcPort());
    assertEquals(29401, configuration.network().raftPort());
    assertEquals(Duration.ofMinutes(30), configuration.storage().snapshot().interval());
    assertEquals(DataSize.ofMegabytes(192), configuration.storage().snapshot().logSizeThreshold());
    assertEquals(25, configuration.changeHistoryLimit());
    assertEquals(Duration.ofSeconds(51), configuration.broker().leaseTimeout());
    assertEquals(Duration.ofSeconds(52), configuration.worker().leaseTimeout());
    assertEquals(Duration.ofSeconds(54), configuration.worker().offlineRetention());
    assertEquals(Path.of("/var/lib/pd/meta-env"), configuration.storage().dataDir());
    assertEquals(List.of("meta-other"), peerIds(configuration));
  }

  @Test
  void shouldPreferCommandLineArguments() {
    final StandardEnvironment environment = new StandardEnvironment();
    environment
        .getPropertySources()
        .addLast(
            new SystemEnvironmentPropertySource(
                "test-environment", Map.of("TASKITE_PD_NODE_ID", "meta-env")));
    environment
        .getPropertySources()
        .addFirst(new SimpleCommandLinePropertySource("--taskite.pd.node-id=meta-cli"));

    assertEquals("meta-cli", bind(environment).nodeId());
  }

  @Test
  void shouldRejectInvalidPeers() {
    final PlacementDriverConfiguration localPeer = configuration("meta-0@127.0.0.1:29001:29101");
    final PlacementDriverConfiguration duplicatePeer =
        configuration("meta-1@127.0.0.1:29002:29102,meta-1@127.0.0.2:29003:29103");
    final PlacementDriverConfiguration invalidGrpcPort =
        configuration("meta-1@127.0.0.1:70000:29102");
    final PlacementDriverConfiguration invalidRaftPort =
        configuration("meta-1@127.0.0.1:29002:70000");
    final PlacementDriverConfiguration sharedPeerPort =
        configuration("meta-1@127.0.0.1:29002:29002");
    final PlacementDriverConfiguration unreachableAdvertisedHost =
        configuration(
            "127.0.0.1",
            29001,
            29101,
            50,
            defaultsBroker(),
            defaultsWorker(),
            "meta-1@10.0.0.2:29002:29102");
    final PlacementDriverConfiguration wildcardAdvertisedHost =
        configuration(
            "0.0.0.0",
            29001,
            29101,
            50,
            defaultsBroker(),
            defaultsWorker(),
            "meta-1@10.0.0.2:29002:29102");

    assertThrows(IllegalArgumentException.class, localPeer::validate);
    assertThrows(IllegalArgumentException.class, duplicatePeer::validate);
    assertThrows(IllegalArgumentException.class, invalidGrpcPort::validate);
    assertThrows(IllegalArgumentException.class, invalidRaftPort::validate);
    assertThrows(IllegalArgumentException.class, sharedPeerPort::validate);
    assertThrows(IllegalArgumentException.class, unreachableAdvertisedHost::validate);
    assertThrows(IllegalArgumentException.class, wildcardAdvertisedHost::validate);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            configuration("127.0.0.1", 29001, 29001, 50, defaultsBroker(), defaultsWorker(), "")
                .validate());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            configuration("127.0.0.1", 29001, 29101, 0, defaultsBroker(), defaultsWorker(), "")
                .validate());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            configuration(
                    "127.0.0.1",
                    29001,
                    29101,
                    50,
                    new PlacementDriverConfiguration.BrokerLeaseConfiguration(Duration.ZERO),
                    defaultsWorker(),
                    "")
                .validate());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            configuration(
                    "127.0.0.1",
                    29001,
                    29101,
                    50,
                    defaultsBroker(),
                    new PlacementDriverConfiguration.WorkerLeaseConfiguration(
                        Duration.ZERO, Duration.ofMillis(1)),
                    "")
                .validate());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            configuration(
                    "127.0.0.1",
                    29001,
                    29101,
                    50,
                    defaultsBroker(),
                    new PlacementDriverConfiguration.WorkerLeaseConfiguration(
                        Duration.ofMillis(1), Duration.ofMillis(-1)),
                    "")
                .validate());
    assertThrows(IllegalArgumentException.class, () -> configuration("invalid-peer"));
    assertThrows(IllegalArgumentException.class, () -> configuration("meta-1@127.0.0.1:29002"));
  }

  @Test
  void shouldNormalizeIpv6HostsAndPeerPorts() {
    final var config =
        configuration(
            "[2001:db8::10]",
            26501,
            26502,
            50,
            defaultsBroker(),
            defaultsWorker(),
            "meta-1@[2001:db8::11]:26501:26502,meta-2@2001:db8::12:26503:26504");
    config.validate();
    assertEquals("2001:db8::10", config.network().host());
    assertEquals(
        List.of(
            new PlacementDriverPeer("meta-1", "2001:db8::11", 26501, 26502),
            new PlacementDriverPeer("meta-2", "2001:db8::12", 26503, 26504)),
        config.consensus().bootstrapPeers());
  }

  private static PlacementDriverConfiguration configuration(final String peers) {
    return configuration("127.0.0.1", 29001, 29101, 50, defaultsBroker(), defaultsWorker(), peers);
  }

  private static PlacementDriverConfiguration configuration(
      final String host,
      final int grpcPort,
      final int raftPort,
      final int changeHistoryLimit,
      final PlacementDriverConfiguration.BrokerLeaseConfiguration broker,
      final PlacementDriverConfiguration.WorkerLeaseConfiguration worker,
      final String peers) {
    return new PlacementDriverConfiguration(
        "meta-0",
        new PlacementDriverConfiguration.Network(host, grpcPort, raftPort),
        new PlacementDriverConfiguration.Consensus(
            Duration.ofMillis(1200), Duration.ofMillis(300), peers),
        new PlacementDriverConfiguration.Storage(Path.of("data"), null),
        changeHistoryLimit,
        broker,
        worker,
        null);
  }

  private static PlacementDriverConfiguration.BrokerLeaseConfiguration defaultsBroker() {
    return PlacementDriverConfiguration.BrokerLeaseConfiguration.defaults();
  }

  private static PlacementDriverConfiguration.WorkerLeaseConfiguration defaultsWorker() {
    return PlacementDriverConfiguration.WorkerLeaseConfiguration.defaults();
  }

  private static PlacementDriverConfiguration bind(final StandardEnvironment environment) {
    return Binder.get(environment)
        .bind("taskite.pd", Bindable.of(PlacementDriverConfiguration.class))
        .orElseThrow(() -> new IllegalStateException("pd configuration was not bound"));
  }

  private static List<String> peerIds(final PlacementDriverConfiguration configuration) {
    return configuration.consensus().bootstrapPeers().stream()
        .map(PlacementDriverPeer::memberId)
        .toList();
  }

  @Configuration(proxyBeanMethods = false)
  @EnableConfigurationProperties(PlacementDriverConfiguration.class)
  static class TestConfiguration {}
}
