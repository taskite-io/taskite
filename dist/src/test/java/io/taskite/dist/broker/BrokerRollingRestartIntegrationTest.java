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

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.taskite.broker.Broker;
import io.taskite.broker.BrokerConfig;
import io.taskite.placementdriver.PlacementDriverApplication;
import io.taskite.placementdriver.bootstrap.PlacementDriverSpringLifecycle;
import io.taskite.placementdriver.client.GrpcClient;
import io.taskite.placementdriver.protocol.CreateJobSubscriptionRequest;
import io.taskite.placementdriver.protocol.DistributeCommandRequest;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;

/**
 * Real PD registration, ADD/JOIN/promotion, readiness, durable resource writes and rolling restart.
 */
final class BrokerRollingRestartIntegrationTest {
  @Test
  @Timeout(90)
  void readinessHttpTracksRealBrokerAvailability(@TempDir final Path directory) throws Exception {
    try (var pd =
        new SpringApplicationBuilder(PlacementDriverApplication.class)
            .run(
                "--server.port=0",
                "--taskite.pd.node-id=readiness-pd",
                "--taskite.pd.network.host=127.0.0.1",
                "--taskite.pd.network.grpc-port=0",
                "--taskite.pd.network.raft-port=" + port(),
                "--taskite.pd.consensus.election-timeout=150ms",
                "--taskite.pd.consensus.heartbeat-interval=50ms",
                "--taskite.pd.storage.data-dir=" + directory.resolve("pd"),
                "--taskite.pd.consensus.bootstrap-peers=",
                "--spring.main.banner-mode=off")) {
      final int pdPort = pd.getBean(PlacementDriverSpringLifecycle.class).grpcPort();
      final int httpPort =
          pd.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
      final var http = HttpClient.newHttpClient();
      final String base = "http://127.0.0.1:" + httpPort;
      await(
          () -> {
            try {
              return "LEADER".equals(json(http, base + "/api/v1/pd/health").path("role").asText());
            } catch (Exception ignored) {
              return false;
            }
          });
      final var created =
          http.send(
              HttpRequest.newBuilder(URI.create(base + "/api/v1/broker/clusters"))
                  .header("Content-Type", "application/json")
                  .POST(
                      HttpRequest.BodyPublishers.ofString(
                          "{\"clusterId\":\"readiness\",\"clusterSize\":1,\"partitionSize\":1,\"replicationFactor\":1}"))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertTrue(created.statusCode() == 200 || created.statusCode() == 201, created.body());
      try (var broker =
          new SpringApplicationBuilder(BrokerApplication.class)
              .profiles("broker")
              .run(
                  "--taskite.broker.cluster.cluster-id=readiness",
                  "--taskite.broker.cluster.broker-id=ready-b0",
                  "--taskite.broker.network.host=127.0.0.1",
                  "--taskite.broker.network.command-port=" + port(),
                  "--taskite.broker.network.raft-port=" + port(),
                  "--taskite.broker.storage.data-dir=" + directory.resolve("broker"),
                  "--taskite.broker.registration.placement-drivers=127.0.0.1:" + pdPort,
                  "--taskite.broker.registration.renew-interval=200ms",
                  "--taskite.broker.registration.topology-refresh-interval=200ms",
                  "--taskite.broker.consensus.election-timeout=500ms",
                  "--server.port=-1",
                  "--management.server.port=0",
                  "--spring.main.banner-mode=off")) {
        final int management =
            broker.getEnvironment().getRequiredProperty("local.management.port", Integer.class);
        final URI probe =
            URI.create("http://127.0.0.1:" + management + "/actuator/health/readiness");
        await(
            () -> {
              try {
                return http.send(
                            HttpRequest.newBuilder(probe).GET().build(),
                            HttpResponse.BodyHandlers.ofString())
                        .statusCode()
                    == 200;
              } catch (Exception ignored) {
                return false;
              }
            });
        broker.getBean(BrokerSpringLifecycle.class).stop();
        final var unavailable =
            http.send(
                HttpRequest.newBuilder(probe).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(503, unavailable.statusCode(), unavailable.body());
      }
    }
  }

  @Test
  @Timeout(180)
  void rf3PromotionAndRollingRestartPreserveReadinessAndDeployedResources(
      @TempDir final Path directory) throws Exception {
    try (var pd =
        new SpringApplicationBuilder(PlacementDriverApplication.class)
            .run(
                "--server.port=0",
                "--taskite.pd.node-id=rolling-pd",
                "--taskite.pd.network.host=127.0.0.1",
                "--taskite.pd.network.grpc-port=0",
                "--taskite.pd.network.raft-port=" + port(),
                "--taskite.pd.consensus.election-timeout=150ms",
                "--taskite.pd.consensus.heartbeat-interval=50ms",
                "--taskite.pd.storage.data-dir=" + directory.resolve("pd"),
                "--taskite.pd.consensus.bootstrap-peers=",
                "--spring.main.banner-mode=off")) {
      final int pdPort = pd.getBean(PlacementDriverSpringLifecycle.class).grpcPort();
      final int httpPort =
          pd.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
      final var http = HttpClient.newHttpClient();
      final String base = "http://127.0.0.1:" + httpPort;
      await(
          () -> {
            try {
              return "LEADER".equals(json(http, base + "/api/v1/pd/health").path("role").asText());
            } catch (Exception ignored) {
              return false;
            }
          });
      final var created =
          http.send(
              HttpRequest.newBuilder(URI.create(base + "/api/v1/broker/clusters"))
                  .header("Content-Type", "application/json")
                  .POST(
                      HttpRequest.BodyPublishers.ofString(
                          "{\"clusterId\":\"rolling\",\"clusterSize\":3,\"partitionSize\":1,\"replicationFactor\":3}"))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertTrue(created.statusCode() == 200 || created.statusCode() == 201, created.body());
      final List<BrokerConfig> configs = new ArrayList<>();
      final List<Broker> brokers = new ArrayList<>();
      try (var client = GrpcClient.connect("127.0.0.1:" + pdPort)) {
        for (int i = 0; i < 3; i++) {
          final var config =
              new BrokerConfig(
                  new BrokerConfig.Identity("rolling", "b" + i, "test"),
                  new BrokerConfig.Node(
                      new BrokerConfig.Network("127.0.0.1", port(), port()),
                      directory.resolve("brokers")),
                  new BrokerConfig.Registration(
                      "127.0.0.1:" + pdPort,
                      Duration.ofMillis(200),
                      Duration.ofSeconds(30),
                      Duration.ofMillis(200)),
                  new BrokerConfig.Engine(
                      new BrokerConfig.Snapshot(Duration.ofMinutes(15), 128L * 1024 * 1024),
                      new BrokerConfig.Consensus(Duration.ofMillis(500)),
                      BrokerConfig.defaultProcessing()));
          configs.add(config);
          final var broker = new Broker(config);
          assertFalse(broker.ready());
          brokers.add(broker);
          broker.start().join(30, TimeUnit.SECONDS);
          final int count = i + 1;
          try {
            await(() -> converged(http, base, brokers, count));
          } catch (AssertionError error) {
            System.out.println(
                "ROLLING_STAGE_"
                    + count
                    + " "
                    + json(http, base + "/api/v1/broker/clusters/rolling/topology"));
            throw error;
          }
        }
        final long key = deploy(client, "before");
        await(
            () -> brokers.stream().allMatch(broker -> broker.jobSubscription(1, key).isPresent()));
        for (int i = 0; i < 3; i++) {
          brokers.get(i).stop().join(20, TimeUnit.SECONDS);
          assertFalse(brokers.get(i).ready());
          final var restarted = new Broker(configs.get(i));
          brokers.set(i, restarted);
          restarted.start().join(30, TimeUnit.SECONDS);
          try {
            await(() -> converged(http, base, brokers, 3));
          } catch (final AssertionError error) {
            try {
              System.out.println(
                  "ROLLING_RESTART_"
                      + i
                      + " topology="
                      + json(http, base + "/api/v1/broker/clusters/rolling/topology")
                      + " local="
                      + brokers.stream()
                          .map(
                              broker ->
                                  Map.of(
                                      "brokerId",
                                      broker.config().brokerId(),
                                      "ready",
                                      broker.ready(),
                                      "resourcePresent",
                                      broker.jobSubscription(1, key).isPresent()))
                          .toList());
            } catch (final Throwable diagnosticFailure) {
              error.addSuppressed(diagnosticFailure);
            }
            throw error;
          }
          await(() -> restarted.jobSubscription(1, key).isPresent());
          final long after = deploy(client, "after" + i);
          await(
              () ->
                  brokers.stream()
                      .allMatch(broker -> broker.jobSubscription(1, after).isPresent()));
        }
      } finally {
        for (final var broker : brokers) {
          broker.stop().join(20, TimeUnit.SECONDS);
        }
      }
    }
  }

  private static boolean converged(
      final HttpClient http, final String base, final List<Broker> brokers, final int count) {
    try {
      final var placement = json(http, base + "/api/v1/broker/clusters/rolling/topology");
      final var partition = placement.path("partitions").get(0);
      if (partition == null
          || partition.path("replicas").size() != count - 1
          || partition.path("learners").size() != 0
          || partition.path("leaderId").asText().isBlank()
          || brokers.stream().anyMatch(broker -> !broker.ready())) {
        return false;
      }
      for (final var broker : placement.path("brokers")) {
        final var replica = broker.path("partitions").get(0);
        if (replica == null
            || replica.path("priority").asInt() <= 0
            || !replica.path("election").path("serviceReady").asBoolean()) {
          return false;
        }
      }
      return true;
    } catch (Exception ignored) {
      return false;
    }
  }

  private static long deploy(final GrpcClient client, final String id) throws Exception {
    return client
        .distributeCommand(
            DistributeCommandRequest.newBuilder()
                .setClusterId("rolling")
                .setCreateJobSubscription(
                    CreateJobSubscriptionRequest.newBuilder()
                        .setName(id)
                        .setJobType("rolling")
                        .setVariables("{}")
                        .setRetries(3)
                        .setPriority(50))
                .build())
        .get(10, TimeUnit.SECONDS)
        .getDistributionKey();
  }

  private static JsonNode json(final HttpClient http, final String url) throws Exception {
    final var response =
        http.send(
            HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(2)).GET().build(),
            HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode(), response.body());
    return new ObjectMapper().readTree(response.body());
  }

  private static int port() throws Exception {
    while (true) {
      try (var socket = new ServerSocket(0)) {
        if (socket.getLocalPort() < 65_535) {
          return socket.getLocalPort();
        }
      }
    }
  }

  private static void await(final BooleanSupplier condition) throws Exception {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(40);
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(50);
    }
    fail("Broker/PD did not converge in 40s");
  }
}
