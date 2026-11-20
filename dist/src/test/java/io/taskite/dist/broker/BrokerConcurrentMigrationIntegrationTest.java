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
import io.taskite.broker.common.ExponentialBackoff;
import io.taskite.broker.common.PlacementDriverErrorClassifier;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.builder.SpringApplicationBuilder;

/** Real five-partition replication, concurrent restart and controlled whole-cluster replacement. */
final class BrokerConcurrentMigrationIntegrationTest {
  @ParameterizedTest
  @CsvSource({
    "2,false,false,false",
    "3,false,false,false",
    "2,true,false,false",
    "3,true,false,false",
    "2,false,true,false",
    "3,false,true,false",
    "2,false,false,true",
    "3,false,false,true"
  })
  @Timeout(240)
  void concurrentMigrationPreservesResourcesAndNewWrites(
      final int count,
      final boolean replacement,
      final boolean restoreQuorum,
      final boolean changedEndpoint,
      @TempDir final Path directory)
      throws Exception {
    try (final var pd =
        new SpringApplicationBuilder(PlacementDriverApplication.class)
            .run(
                "--server.port=0",
                "--taskite.pd.node-id=migration-pd",
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
      final String base = "http://127.0.0.1:" + httpPort + "/api/v1";
      await(
          () -> {
            try {
              return "LEADER".equals(get(http, base + "/pd/health").path("role").asText());
            } catch (final Exception ignored) {
              return false;
            }
          });
      send(
          http,
          base + "/broker/clusters",
          "POST",
          "{\"clusterId\":\"migration\",\"clusterSize\":3,\"partitionSize\":5,\"replicationFactor\":3}");
      final List<Broker> all = new ArrayList<>();
      final List<Broker> active = new ArrayList<>();
      try (final var client = GrpcClient.connect("127.0.0.1:" + pdPort)) {
        for (int i = 0; i < 3; i++) {
          final var broker = broker(directory, pdPort, "b" + i);
          all.add(broker);
          active.add(broker);
          broker.start().join(30, TimeUnit.SECONDS);
        }
        awaitReady(http, base, active, 0, "initial placement");
        final long before = deploy(client, "before");
        await(() -> resources(active, before));
        final List<Broker> old = List.copyOf(active.subList(0, count));
        if (replacement) {
          final List<Broker> added = new ArrayList<>();
          for (int i = 0; i < count; i++) {
            final var broker = broker(directory, pdPort, "new" + i);
            all.add(broker);
            added.add(broker);
            broker.start().join(30, TimeUnit.SECONDS);
          }
          awaitReady(http, base, active, count, "replacement learners");
          final var ids = new ObjectMapper();
          final String body =
              ids.writeValueAsString(
                  Map.of(
                      "brokers",
                      Map.of(
                          "add",
                          added.stream().map(b -> b.config().brokerId()).toList(),
                          "remove",
                          old.stream().map(b -> b.config().brokerId()).toList(),
                          "count",
                          3)));
          send(http, base + "/broker/clusters/migration/configuration", "PATCH", body);
          for (int i = 0; i < count; i++) {
            active.set(i, added.get(i));
          }
          awaitReady(http, base, active, 0, "replacement placement");
          await(() -> resources(active, before));
          // Old replicas remain alive until the durable member change and resource ACKs finish.
          for (final var broker : old) {
            broker.stop().join(20, TimeUnit.SECONDS);
          }
        } else {
          final var stops =
              old.stream()
                  .map(b -> CompletableFuture.runAsync(() -> b.stop().join(20, TimeUnit.SECONDS)))
                  .toArray(CompletableFuture[]::new);
          CompletableFuture.allOf(stops).get(30, TimeUnit.SECONDS);
          if (restoreQuorum) {
            // New identities cannot replace the old voting majority after it has disappeared.
            for (int i = 0; i < count; i++) {
              final var spare = broker(directory, pdPort, "new" + i);
              all.add(spare);
              spare.start().join(30, TimeUnit.SECONDS);
            }
            await(
                () -> {
                  try {
                    final var topology = get(http, base + "/broker/clusters/migration/topology");
                    final var partitions = topology.path("partitions");
                    return partitions.size() == 5
                        && StreamSupport.stream(partitions.spliterator(), false)
                            .allMatch(
                                p ->
                                    !p.path("serviceReady").asBoolean()
                                        && p.path("leaderId").asText().isBlank());
                  } catch (final Exception ignored) {
                    return false;
                  }
                });
            assertTrue(active.stream().anyMatch(b -> !b.ready()));
          }
          for (int i = 0; i < count; i++) {
            final var original = old.get(i).config();
            final var restarted =
                new Broker(
                    changedEndpoint
                        ? new BrokerConfig(
                            original.identity(),
                            new BrokerConfig.Node(
                                new BrokerConfig.Network(
                                    "::1", original.network().raftPort(), port()),
                                original.dataDirectory()),
                            original.registration(),
                            original.engine())
                        : original);
            all.add(restarted);
            active.set(i, restarted);
            restarted.start().join(30, TimeUnit.SECONDS);
          }
          if (restoreQuorum) {
            // Returning original endpoints and storage restores the committed configuration.
            // PD can then complete its ordinary replacement plans while retaining the old data.
            await(
                () -> {
                  try {
                    active.clear();
                    final var roster =
                        get(http, base + "/broker/clusters/migration/configuration")
                            .path("voterRoster");
                    for (final var id : roster) {
                      active.add(
                          all.stream()
                              .filter(b -> b.config().brokerId().equals(id.asText()))
                              .reduce((a, b) -> b)
                              .orElseThrow());
                    }
                    // The target roster can still change while recovery plans are executing.
                    // Observe it again until all partitions and durable plans have converged.
                    return active.size() == 3 && ready(http, base, active, -1);
                  } catch (final Exception ignored) {
                    return false;
                  }
                },
                () -> "recovery plans: " + diagnostics(http, base, active));
          }
          awaitReady(http, base, active, restoreQuorum ? -1 : 0, "restored placement");
          await(() -> resources(active, before));
        }
        final long after = deploy(client, "after");
        await(() -> resources(active, after));
        await(
            () ->
                resources(active, before)
                    && resources(active, after)
                    && ready(http, base, active, restoreQuorum ? -1 : 0),
            () -> "final resources and readiness: " + diagnostics(http, base, active));
      } finally {
        for (final var broker : all) {
          broker.stop().join(20, TimeUnit.SECONDS);
        }
      }
    }
  }

  private static Broker broker(final Path directory, final int pdPort, final String id)
      throws Exception {
    return new Broker(
        new BrokerConfig(
            new BrokerConfig.Identity("migration", id, "test"),
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
                BrokerConfig.defaultProcessing())));
  }

  private static boolean ready(
      final HttpClient http, final String base, final List<Broker> active, final int learners) {
    try {
      final var topology = get(http, base + "/broker/clusters/migration/topology");
      if (topology.path("partitions").size() != 5 || active.stream().anyMatch(b -> !b.ready())) {
        return false;
      }
      final var expected =
          active.stream().map(b -> b.config().brokerId()).collect(Collectors.toSet());
      for (final var p : topology.path("partitions")) {
        if (!partitionReady(p, expected, learners)
            || !configurationObserved(topology, p, expected)) {
          return false;
        }
      }
      return http.send(
                  HttpRequest.newBuilder(
                          URI.create(base + "/broker/clusters/migration/changes/active"))
                      .timeout(Duration.ofSeconds(2))
                      .GET()
                      .build(),
                  HttpResponse.BodyHandlers.discarding())
              .statusCode()
          == 404;
    } catch (final Exception ignored) {
      return false;
    }
  }

  private static boolean resources(final List<Broker> brokers, final long key) {
    return brokers.stream()
        .allMatch(
            b -> IntStream.rangeClosed(1, 5).allMatch(p -> b.jobSubscription(p, key).isPresent()));
  }

  private static boolean partitionReady(
      final JsonNode partition, final Set<String> expected, final int learners) {
    final var voters = new HashSet<String>();
    voters.add(partition.path("leaderId").asText());
    partition.path("replicas").forEach(replica -> voters.add(replica.asText()));
    return voters.equals(expected)
        && partition.path("serviceReady").asBoolean()
        && "ACTIVE".equals(partition.path("status").asText())
        && (learners < 0 || partition.path("learners").size() == learners);
  }

  private static boolean configurationObserved(
      final JsonNode topology, final JsonNode partition, final Set<String> voters) {
    final Set<String> members = new HashSet<>(voters);
    partition.path("learners").forEach(learner -> members.add(learner.asText()));
    String digest = null;
    for (final String member : members) {
      final var report =
          StreamSupport.stream(topology.path("brokers").spliterator(), false)
              .filter(broker -> member.equals(broker.path("brokerId").asText()))
              .filter(broker -> "ONLINE".equals(broker.path("status").asText()))
              .flatMap(
                  broker -> StreamSupport.stream(broker.path("partitions").spliterator(), false))
              .filter(p -> p.path("partitionId").asInt() == partition.path("partitionId").asInt())
              .findFirst()
              .orElse(null);
      if (report == null
          || report.path("recoveryEpoch").asLong() != partition.path("recoveryEpoch").asLong()
          || report.path("election").path("configurationIndex").asLong()
              > report.path("matchIndex").asLong()
          || (voters.contains(member)
              ? report.path("priority").asInt() <= 0
              : report.path("priority").asInt() != 0)) {
        return false;
      }
      final String observed = report.path("election").path("configurationDigest").asText();
      if (observed.isBlank() || digest != null && !digest.equals(observed)) {
        return false;
      }
      digest = observed;
    }
    return true;
  }

  private static void awaitReady(
      final HttpClient http,
      final String base,
      final List<Broker> active,
      final int learners,
      final String stage)
      throws Exception {
    final var expected =
        active.stream().map(broker -> broker.config().brokerId()).collect(Collectors.toSet());
    // Migration admission and successor plans can change completion order. Require a previously
    // unfinished partition to converge within each wait; the whole scenario remains capped at 240s.
    final Set<Integer> completed = new HashSet<>();
    while (completed.size() < 5) {
      await(
          () -> {
            try {
              final var topology = get(http, base + "/broker/clusters/migration/topology");
              boolean progressed = false;
              for (final var candidate : topology.path("partitions")) {
                final int partitionId = candidate.path("partitionId").asInt();
                if (partitionId >= 1
                    && partitionId <= 5
                    && partitionReady(candidate, expected, learners)
                    && completed.add(partitionId)) {
                  progressed = true;
                }
              }
              return progressed;
            } catch (final Exception ignored) {
              return false;
            }
          },
          () ->
              stage
                  + ", converged partitions "
                  + completed
                  + ": "
                  + diagnostics(http, base, active));
    }
    await(
        () -> ready(http, base, active, learners),
        () -> stage + ": " + diagnostics(http, base, active));
  }

  private static String diagnostics(
      final HttpClient http, final String base, final List<Broker> brokers) {
    final var local =
        brokers.stream()
            .map(broker -> Map.of("brokerId", broker.config().brokerId(), "ready", broker.ready()))
            .toList();
    try {
      final var topology = get(http, base + "/broker/clusters/migration/topology");
      final var change =
          http.send(
              HttpRequest.newBuilder(URI.create(base + "/broker/clusters/migration/changes/active"))
                  .timeout(Duration.ofSeconds(5))
                  .GET()
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      return "local="
          + local
          + ", topology="
          + topology
          + ", activeChange=HTTP "
          + change.statusCode()
          + ": "
          + change.body();
    } catch (final Exception | AssertionError error) {
      return "local=" + local + ", diagnostics failed: " + error;
    }
  }

  private static long deploy(final GrpcClient client, final String id) throws Exception {
    final var request =
        DistributeCommandRequest.newBuilder()
            .setClusterId("migration")
            .setCreateJobSubscription(
                CreateJobSubscriptionRequest.newBuilder()
                    .setName(id)
                    .setJobType("migration")
                    .setVariables("{}")
                    .setRetries(3)
                    .setPriority(50))
            .build();
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    final var backoff = ExponentialBackoff.builder().maxDelay(1000).build();
    long delay = 0;
    while (true) {
      try {
        return client
            .distributeCommand(request)
            .get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)
            .getDistributionKey();
      } catch (final ExecutionException error) {
        if (!PlacementDriverErrorClassifier.isBackpressure(error)
            || System.nanoTime() >= deadline) {
          throw error;
        }
        // Admission rejects before append, so retrying cannot duplicate an accepted deployment.
        delay = backoff.nextDelay(delay);
        TimeUnit.NANOSECONDS.sleep(
            Math.min(TimeUnit.MILLISECONDS.toNanos(delay), deadline - System.nanoTime()));
      }
    }
  }

  private static JsonNode get(final HttpClient http, final String url) throws Exception {
    return send(http, url, "GET", null);
  }

  private static JsonNode send(
      final HttpClient http, final String url, final String method, final String body)
      throws Exception {
    final var response =
        http.send(
            HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/json")
                .method(
                    method,
                    body == null
                        ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertTrue(response.statusCode() >= 200 && response.statusCode() < 300, response.body());
    return new ObjectMapper().readTree(response.body());
  }

  private static int port() throws Exception {
    while (true) {
      try (final var socket = new ServerSocket(0)) {
        if (socket.getLocalPort() < 65_535) {
          return socket.getLocalPort();
        }
      }
    }
  }

  private static void await(final BooleanSupplier condition) throws Exception {
    await(condition, () -> "condition remained false");
  }

  private static void await(final BooleanSupplier condition, final Supplier<String> message)
      throws Exception {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(100);
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(100);
    }
    fail("migration did not converge within 100s: " + message.get());
  }
}
