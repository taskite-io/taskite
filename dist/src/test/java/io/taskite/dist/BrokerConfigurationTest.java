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
package io.taskite.dist;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.taskite.broker.BrokerConfig;
import io.taskite.broker.partition.BrokerPartitionManagerConfig;
import io.taskite.dist.broker.BrokerConfiguration;
import io.taskite.placementdriver.PlacementDriverConfiguration;
import io.taskite.streamplatform.api.backpressure.BackpressureConfiguration;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.properties.bind.BindHandler;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.handler.NoUnboundElementsBindHandler;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.unit.DataSize;

final class BrokerConfigurationTest {

  @Test
  void bindsSharedProcessingDefaultsAndEnvironmentFromRealBrokerProfile() throws Exception {
    final var environment = new StandardEnvironment();
    environment
        .getPropertySources()
        .addFirst(
            new SystemEnvironmentPropertySource(
                "broker-identity",
                Map.of(
                    "TASKITE_BROKER_CLUSTER_CLUSTER_ID", "cluster-a",
                    "TASKITE_BROKER_REGISTRATION_PLACEMENT_DRIVERS", "127.0.0.1:26501")));
    for (final var source :
        new YamlPropertySourceLoader()
            .load("broker-profile", new ClassPathResource("application-broker.yaml"))) {
      environment.getPropertySources().addLast(source);
    }
    final var defaults = bind(environment).engine();
    assertEquals(true, defaults.workerJobLeaseTokenEnabled());
    assertEquals(
        new BrokerConfig.Processing(1, 0, Duration.ZERO, 3 * 1024 * 1024), defaults.processing());

    environment
        .getPropertySources()
        .addFirst(
            new SystemEnvironmentPropertySource(
                "processing",
                Map.of(
                    "TASKITE_BROKER_WORKER_JOB_LEASE_TOKEN_ENABLED", "false",
                    "TASKITE_BROKER_PROCESSING_MAX_COMMANDS_IN_BATCH", "7",
                    "TASKITE_BROKER_PROCESSING_MAX_RECORDS_IN_BATCH", "5",
                    "TASKITE_BROKER_PROCESSING_MAX_BATCH_BYTES", "8192",
                    "TASKITE_BROKER_PROCESSING_MAX_BATCH_DURATION", "20ms")));
    final var configured = bind(environment).engine();
    assertEquals(false, configured.workerJobLeaseTokenEnabled());
    assertEquals(
        new BrokerConfig.Processing(7, 5, Duration.ofMillis(20), 8192), configured.processing());

    environment
        .getPropertySources()
        .addFirst(
            new MapPropertySource(
                "invalid-budget", Map.of("taskite.broker.processing.max-batch-bytes", "1023")));
    assertThrows(IllegalArgumentException.class, () -> bind(environment));
  }

  @ParameterizedTest
  @ValueSource(strings = {"application.yaml", "application-placement-driver.yaml"})
  void bindsWorkerLeaseDefaultsAndEnvironmentFromBothPlacementDriverProfiles(final String profile)
      throws Exception {
    final var environment = new StandardEnvironment();
    for (final var source :
        new YamlPropertySourceLoader().load("pd-base", new ClassPathResource("application.yaml"))) {
      environment.getPropertySources().addLast(source);
    }
    for (final var source :
        new YamlPropertySourceLoader().load("pd-profile", new ClassPathResource(profile))) {
      environment.getPropertySources().addLast(source);
    }
    assertEquals(
        Duration.ofSeconds(30),
        Binder.get(environment)
            .bind("taskite.pd", Bindable.of(PlacementDriverConfiguration.class))
            .get()
            .worker()
            .leaseTimeout());
    environment
        .getPropertySources()
        .addFirst(
            new SystemEnvironmentPropertySource(
                "worker-lease",
                Map.of(
                    "TASKITE_PD_WORKER_LEASE_TIMEOUT", "45s",
                    "TASKITE_PD_WORKER_OFFLINE_RETENTION", "20m")));
    final var config =
        Binder.get(environment)
            .bind("taskite.pd", Bindable.of(PlacementDriverConfiguration.class))
            .get();
    config.validate();
    assertEquals(Duration.ofSeconds(45), config.worker().leaseTimeout());
    assertEquals(Duration.ofMinutes(20), config.worker().offlineRetention());
  }

  @Test
  void bindsPlacementDriverBackpressureFromRealProfileAndEnvironment() throws Exception {
    final var environment = new StandardEnvironment();
    environment
        .getPropertySources()
        .addFirst(
            new SystemEnvironmentPropertySource(
                "backpressure-environment",
                Map.ofEntries(
                    Map.entry("TASKITE_PD_BACKPRESSURE_ENABLED", "false"),
                    Map.entry("TASKITE_PD_BACKPRESSURE_ALGORITHM", "vegas"),
                    Map.entry("TASKITE_PD_BACKPRESSURE_AIMD_REQUEST_TIMEOUT", "140ms"),
                    Map.entry("TASKITE_PD_BACKPRESSURE_AIMD_INITIAL_LIMIT", "12"),
                    Map.entry("TASKITE_PD_BACKPRESSURE_AIMD_MIN_LIMIT", "2"),
                    Map.entry("TASKITE_PD_BACKPRESSURE_AIMD_MAX_LIMIT", "900"),
                    Map.entry("TASKITE_PD_BACKPRESSURE_AIMD_BACKOFF_RATIO", "0.8"),
                    Map.entry("TASKITE_PD_BACKPRESSURE_VEGAS_INITIAL_LIMIT", "16"),
                    Map.entry("TASKITE_PD_BACKPRESSURE_VEGAS_ALPHA", "4"),
                    Map.entry("TASKITE_PD_BACKPRESSURE_VEGAS_BETA", "9"),
                    Map.entry("TASKITE_PD_BACKPRESSURE_VEGAS_MAX_LIMIT", "200"),
                    Map.entry("TASKITE_PD_BACKPRESSURE_FIXED_LIMIT", "7"))));
    for (final var source :
        new YamlPropertySourceLoader()
            .load("pd-profile", new ClassPathResource("application.yaml"))) {
      environment.getPropertySources().addLast(source);
    }
    final var config =
        Binder.get(environment)
            .bind("taskite.pd", Bindable.of(PlacementDriverConfiguration.class))
            .get();
    config.validate();
    assertEquals(false, config.backpressure().enabled());
    assertEquals(BackpressureConfiguration.Algorithm.VEGAS, config.backpressure().algorithm());
    assertEquals(
        new BackpressureConfiguration.Aimd(Duration.ofMillis(140), 12, 2, 900, 0.8),
        config.backpressure().aimd());
    assertEquals(new BackpressureConfiguration.Vegas(16, 4, 9, 200), config.backpressure().vegas());
    assertEquals(7, config.backpressure().fixed().limit());
  }

  @Test
  void bindsPartitionBackpressureFromRealBrokerProfileAndEnvironment() throws Exception {
    final var environment = new StandardEnvironment();
    environment
        .getPropertySources()
        .addFirst(
            new SystemEnvironmentPropertySource(
                "backpressure-environment",
                Map.of(
                    "TASKITE_BROKER_CLUSTER_CLUSTER_ID", "cluster-a",
                    "TASKITE_BROKER_REGISTRATION_PLACEMENT_DRIVERS", "127.0.0.1:26501",
                    "TASKITE_BROKER_BACKPRESSURE_ALGORITHM", "fixed",
                    "TASKITE_BROKER_BACKPRESSURE_FIXED_LIMIT", "3")));
    for (final var source :
        new YamlPropertySourceLoader()
            .load("broker-profile", new ClassPathResource("application-broker.yaml"))) {
      environment.getPropertySources().addLast(source);
    }
    final var config = bind(environment);
    assertEquals(BackpressureConfiguration.Algorithm.FIXED, config.backpressure().algorithm());
    assertEquals(3, config.backpressure().fixed().limit());
    assertEquals(100, config.backpressure().aimd().initialLimit());
    assertEquals(
        config.backpressure(), BrokerPartitionManagerConfig.from(config, 1, false).backpressure());
  }

  @Test
  void partialAndDisabledPartitionBackpressureKeepSharedDefaults() {
    final var environment = snapshotEnvironment("15m", "128MB");
    environment
        .getPropertySources()
        .addFirst(
            new MapPropertySource(
                "backpressure", Map.of("taskite.broker.backpressure.aimd.initial-limit", "5")));
    final var config = bind(environment);
    assertEquals(true, config.backpressure().enabled());
    assertEquals(5, config.backpressure().aimd().initialLimit());
    assertEquals(1, config.backpressure().aimd().minLimit());
    assertEquals(0.9, config.backpressure().aimd().backoffRatio());
    environment
        .getPropertySources()
        .addFirst(
            new MapPropertySource(
                "disabled", Map.of("taskite.broker.backpressure.enabled", "false")));
    assertEquals(false, bind(environment).backpressure().enabled());
  }

  @Test
  void convertsSpringConfigurationToBrokerConfig() {
    final var configuration =
        new BrokerConfiguration(
            new BrokerConfiguration.Settings(
                true,
                new BrokerConfig.Identity("cluster-a", "broker-2", "1.0.0"),
                new BrokerConfiguration.Node(
                    new BrokerConfiguration.Network("10.0.0.2", 27501, 27601),
                    new BrokerConfiguration.Storage(
                        Path.of("data/broker-2"),
                        new BrokerConfiguration.Snapshot(
                            Duration.ofMinutes(2), DataSize.ofMegabytes(64)))),
                new BrokerConfiguration.Registration(
                    "10.0.0.1:29001",
                    Duration.ofSeconds(2),
                    Duration.ofSeconds(40),
                    Duration.ofSeconds(6)),
                new BrokerConfiguration.Engine(
                    new BrokerConfiguration.Consensus(Duration.ofSeconds(7)), null)));

    final var config = configuration.toBrokerConfig();

    assertEquals("cluster-a", config.clusterId());
    assertEquals("broker-2", config.brokerId());
    assertEquals("10.0.0.2", config.network().host());
    assertEquals(27501, config.network().commandPort());
    assertEquals(27601, config.network().raftPort());
    assertEquals(Path.of("data/broker-2"), config.dataDirectory());
    assertEquals("10.0.0.1:29001", config.registration().placementDrivers());
    assertEquals(Duration.ofSeconds(2), config.registration().renewInterval());
    assertEquals(Duration.ofSeconds(40), config.registration().renewalTimeout());
    assertEquals(Duration.ofSeconds(6), config.registration().topologyRefreshInterval());
    assertEquals(BrokerConfig.Backpressure.defaults(), config.registration().backpressure());
    assertEquals(Duration.ofMinutes(2), config.snapshot().interval());
    assertEquals(DataSize.ofMegabytes(64).toBytes(), config.snapshot().logSizeThresholdBytes());
    assertEquals(Duration.ofSeconds(7), config.consensus().electionTimeout());
    assertEquals(BrokerConfig.defaultProcessing(), config.processing());
    assertEquals("1.0.0", config.version());
  }

  @Test
  void translatesBrokerAndMonitoringFlags() {
    assertArrayEquals(
        new String[] {
          "--taskite.broker.cluster.cluster-id=cluster-a",
          "--taskite.broker.cluster.broker-id=broker-2",
          "--taskite.broker.network.command-port=27501",
          "--taskite.broker.network.raft-port=27601",
          "--taskite.broker.registration.renew-interval=2s",
          "--taskite.broker.registration.backpressure.min-delay=25ms",
          "--taskite.broker.registration.backpressure.max-delay=500ms",
          "--taskite.broker.registration.backpressure.backoff-factor=2",
          "--taskite.broker.registration.backpressure.jitter-factor=0.2",
          "--taskite.broker.storage.snapshot.interval=2m",
          "--taskite.broker.storage.snapshot.log-size-threshold=64MB",
          "--taskite.broker.consensus.election-timeout=7s",
          "--management.server.port=9700",
          "--spring.config.location=file:config/application.yaml"
        },
        StandaloneBroker.translate(
            new String[] {
              "--taskite.broker.cluster.cluster-id=cluster-a",
              "--taskite.broker.cluster.broker-id=broker-2",
              "--taskite.broker.network.command-port=27501",
              "--taskite.broker.network.raft-port=27601",
              "--taskite.broker.registration.renew-interval=2s",
              "--taskite.broker.registration.backpressure.min-delay=25ms",
              "--taskite.broker.registration.backpressure.max-delay=500ms",
              "--taskite.broker.registration.backpressure.backoff-factor=2",
              "--taskite.broker.registration.backpressure.jitter-factor=0.2",
              "--taskite.broker.storage.snapshot.interval=2m",
              "--taskite.broker.storage.snapshot.log-size-threshold=64MB",
              "--taskite.broker.consensus.election-timeout=7s",
              "--management.server.port=9700",
              "--spring.config.location=file:config/application.yaml"
            }));
  }

  @Test
  void translatesPlacementDriverSnapshotFlags() {
    assertArrayEquals(
        new String[] {
          "--taskite.pd.storage.snapshot.interval=20m",
          "--taskite.pd.storage.snapshot.log-size-threshold=256MB"
        },
        StandalonePlacementDriver.translate(
            new String[] {
              "--taskite.pd.storage.snapshot.interval=20m",
              "--taskite.pd.storage.snapshot.log-size-threshold=256MB"
            }));
  }

  @Test
  void brokerAndPlacementDriverUseTheSameSnapshotDefaults() {
    assertEquals(
        BrokerConfig.DEFAULT_SNAPSHOT_INTERVAL,
        PlacementDriverConfiguration.DEFAULT_SNAPSHOT_INTERVAL);
    assertEquals(
        BrokerConfig.DEFAULT_SNAPSHOT_LOG_SIZE_THRESHOLD_BYTES,
        PlacementDriverConfiguration.DEFAULT_SNAPSHOT_LOG_SIZE_THRESHOLD_BYTES);
  }

  @Test
  void rejectsMissingRequiredBrokerConfiguration() {
    final var configuration = new BrokerConfiguration();

    assertThrows(IllegalArgumentException.class, configuration::toBrokerConfig);
  }

  @Test
  void bindsDurationAndProcessingProperties() {
    final StandardEnvironment environment = new StandardEnvironment();
    environment
        .getPropertySources()
        .addFirst(
            new MapPropertySource(
                "test",
                Map.ofEntries(
                    Map.entry("taskite.broker.cluster.cluster-id", "cluster-a"),
                    Map.entry("taskite.broker.registration.placement-drivers", "127.0.0.1:26501"),
                    Map.entry("taskite.broker.registration.renew-interval", "2s"),
                    Map.entry("taskite.broker.registration.renewal-timeout", "1m"),
                    Map.entry("taskite.broker.registration.backpressure.min-delay", "25ms"),
                    Map.entry("taskite.broker.registration.backpressure.max-delay", "500ms"),
                    Map.entry("taskite.broker.registration.backpressure.backoff-factor", "2.5"),
                    Map.entry("taskite.broker.registration.backpressure.jitter-factor", "0.2"),
                    Map.entry("taskite.broker.storage.snapshot.interval", "0ms"),
                    Map.entry("taskite.broker.storage.snapshot.log-size-threshold", "64MB"),
                    Map.entry("taskite.broker.processing.max-commands-in-batch", "8"),
                    Map.entry("taskite.broker.processing.max-records-in-batch", "256"),
                    Map.entry("taskite.broker.processing.max-batch-duration", "5ms"),
                    Map.entry("taskite.broker.backpressure.algorithm", "fixed"),
                    Map.entry("taskite.broker.backpressure.fixed.limit", "128"),
                    Map.entry("taskite.broker.processing.max-batch-bytes", "2097152"))));

    final var config = bind(environment);

    assertEquals(Duration.ofSeconds(2), config.registration().renewInterval());
    assertEquals(Duration.ofMinutes(1), config.registration().renewalTimeout());
    assertEquals(
        new BrokerConfig.Backpressure(Duration.ofMillis(25), Duration.ofMillis(500), 2.5, 0.2),
        config.registration().backpressure());
    assertEquals(Duration.ZERO, config.snapshot().interval());
    assertEquals(DataSize.ofMegabytes(64).toBytes(), config.snapshot().logSizeThresholdBytes());
    assertEquals(8, config.processing().maxCommandsInBatch());
    assertEquals(256, config.processing().maxRecordsInBatch());
    assertEquals(Duration.ofMillis(5), config.processing().maxBatchDuration());
    assertEquals(128, config.backpressure().fixed().limit());
    assertEquals(2097152, config.processing().maxBatchBytes());
  }

  @Test
  void bindsRegistrationEnvironmentVariablesThroughApplicationYaml() throws Exception {
    final StandardEnvironment environment = new StandardEnvironment();
    environment
        .getPropertySources()
        .addFirst(
            new SystemEnvironmentPropertySource(
                "registration-environment",
                Map.of(
                    "TASKITE_BROKER_CLUSTER_CLUSTER_ID", "cluster-a",
                    "TASKITE_BROKER_REGISTRATION_PLACEMENT_DRIVERS", "pd.internal:26501",
                    "TASKITE_BROKER_REGISTRATION_RENEW_INTERVAL", "2s",
                    "TASKITE_BROKER_REGISTRATION_RENEWAL_TIMEOUT", "40s",
                    "TASKITE_BROKER_REGISTRATION_TOPOLOGY_REFRESH_INTERVAL", "6s",
                    "TASKITE_BROKER_REGISTRATION_BACKPRESSURE_MIN_DELAY", "25ms",
                    "TASKITE_BROKER_REGISTRATION_BACKPRESSURE_MAX_DELAY", "500ms",
                    "TASKITE_BROKER_REGISTRATION_BACKPRESSURE_BACKOFF_FACTOR", "2",
                    "TASKITE_BROKER_REGISTRATION_BACKPRESSURE_JITTER_FACTOR", "0.2")));
    for (final var source :
        new YamlPropertySourceLoader()
            .load("application-broker", new ClassPathResource("application-broker.yaml"))) {
      environment.getPropertySources().addLast(source);
    }

    final var registration = bind(environment).registration();
    assertEquals("pd.internal:26501", registration.placementDrivers());
    assertEquals(Duration.ofSeconds(2), registration.renewInterval());
    assertEquals(Duration.ofSeconds(40), registration.renewalTimeout());
    assertEquals(Duration.ofSeconds(6), registration.topologyRefreshInterval());
    assertEquals(
        new BrokerConfig.Backpressure(Duration.ofMillis(25), Duration.ofMillis(500), 2, 0.2),
        registration.backpressure());
  }

  @Test
  void rejectsInvalidBackpressureIntervals() {
    for (final var intervals :
        new Duration[][] {
          {Duration.ZERO, Duration.ofSeconds(1)},
          {Duration.ofMillis(10), Duration.ofMillis(5)},
          {Duration.ofNanos(500), Duration.ofSeconds(1)}
        }) {
      assertThrows(
          IllegalArgumentException.class,
          () -> new BrokerConfig.Backpressure(intervals[0], intervals[1], 1.6, 0.1));
    }
    for (final double factor : new double[] {0.5, Double.NaN, Double.POSITIVE_INFINITY}) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              new BrokerConfig.Backpressure(
                  Duration.ofMillis(50), Duration.ofSeconds(5), factor, 0.1));
    }
    for (final double jitter : new double[] {-0.1, 1.1, Double.NaN, Double.POSITIVE_INFINITY}) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              new BrokerConfig.Backpressure(
                  Duration.ofMillis(50), Duration.ofSeconds(5), 1.6, jitter));
    }
  }

  @Test
  void partialBackpressureConfigurationUsesSharedDefaults() {
    final var environment = snapshotEnvironment("15m", "128MB");
    assertEquals(
        BrokerConfig.Backpressure.defaults(), bind(environment).registration().backpressure());
    environment
        .getPropertySources()
        .addFirst(
            new MapPropertySource(
                "backpressure",
                Map.of("taskite.broker.registration.backpressure.max-delay", "250ms")));
    final var config = bind(environment).registration().backpressure();
    assertEquals(Duration.ofMillis(50), config.minDelay());
    assertEquals(Duration.ofMillis(250), config.maxDelay());
    assertEquals(1.6, config.backoffFactor());
    assertEquals(0.1, config.jitterFactor());
  }

  @Test
  void strictBindingRejectsRemovedFlatBackpressureProperties() {
    for (final String property :
        new String[] {"backpressure-initial-interval", "backpressure-max-interval"}) {
      final var environment = snapshotEnvironment("15m", "128MB");
      environment
          .getPropertySources()
          .addFirst(
              new MapPropertySource(
                  "removed-backpressure",
                  Map.of("taskite.broker.registration." + property, "50ms")));
      assertThrows(
          RuntimeException.class,
          () ->
              Binder.get(environment)
                  .bind(
                      "taskite.broker",
                      Bindable.of(BrokerConfiguration.class),
                      new NoUnboundElementsBindHandler(BindHandler.DEFAULT)));
    }
  }

  @Test
  void bindsDisabledAndLargeSnapshotSizeThresholds() {
    final StandardEnvironment large = snapshotEnvironment("0ms", "1GB");
    assertEquals(DataSize.ofGigabytes(1).toBytes(), bind(large).snapshot().logSizeThresholdBytes());

    final StandardEnvironment disabled = snapshotEnvironment("15m", "0B");
    assertEquals(0, bind(disabled).snapshot().logSizeThresholdBytes());
  }

  @Test
  void rejectsRemovedSnapshotEntryThresholdInStrictBinding() {
    final StandardEnvironment environment = snapshotEnvironment("15m", "128MB");
    environment
        .getPropertySources()
        .addFirst(
            new MapPropertySource(
                "removed-property",
                Map.of("taskite.broker.storage.snapshot-entry-threshold", "1024")));

    assertThrows(
        RuntimeException.class,
        () ->
            Binder.get(environment)
                .bind(
                    "taskite.broker",
                    Bindable.of(BrokerConfiguration.class),
                    new NoUnboundElementsBindHandler(BindHandler.DEFAULT)));
  }

  @Test
  void rejectsInvalidGroupedConfiguration() {
    assertThrows(
        IllegalArgumentException.class, () -> new BrokerConfig.Network("127.0.0.1", 26501, 26501));
    assertThrows(IllegalArgumentException.class, () -> new BrokerConfig.Snapshot(Duration.ZERO, 0));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new BrokerConfig.Processing(0, 0, Duration.ZERO, BrokerConfig.DEFAULT_MAX_BATCH_BYTES));
    assertThrows(
        IllegalArgumentException.class,
        () -> new BrokerConfig.Processing(1, 0, Duration.ZERO, Integer.BYTES));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new BrokerConfig(
                new BrokerConfig.Identity("cluster-a", "broker-0", "test"),
                new BrokerConfig.Node(
                    new BrokerConfig.Network("127.0.0.1", 26501, 26502), Path.of("data")),
                new BrokerConfig.Registration(
                    "127.0.0.1:29001",
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(30),
                    Duration.ofSeconds(5)),
                new BrokerConfig.Engine(
                    new BrokerConfig.Snapshot(
                        Duration.ofMinutes(15), DataSize.ofMegabytes(1).toBytes()),
                    new BrokerConfig.Consensus(Duration.ofSeconds(5)),
                    BrokerConfig.defaultProcessing())));
  }

  private static BrokerConfig bind(final StandardEnvironment environment) {
    return Binder.get(environment)
        .bind("taskite.broker", Bindable.of(BrokerConfiguration.class))
        .orElseThrow(() -> new IllegalStateException("broker configuration was not bound"))
        .toBrokerConfig();
  }

  private static StandardEnvironment snapshotEnvironment(
      final String interval, final String logSizeThreshold) {
    final StandardEnvironment environment = new StandardEnvironment();
    environment
        .getPropertySources()
        .addFirst(
            new MapPropertySource(
                "snapshot",
                Map.of(
                    "taskite.broker.cluster.cluster-id",
                    "cluster-a",
                    "taskite.broker.registration.placement-drivers",
                    "127.0.0.1:26501",
                    "taskite.broker.storage.snapshot.interval",
                    interval,
                    "taskite.broker.storage.snapshot.log-size-threshold",
                    logSizeThreshold)));
    return environment;
  }
}
