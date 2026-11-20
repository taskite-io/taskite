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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.taskite.placementdriver.bootstrap.PlacementDriverSpringLifecycle;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

final class PlacementDriverMembershipIntegrationTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  @Timeout(value = 90, unit = TimeUnit.SECONDS)
  void admitsLearnerAndOnlyPromotesThroughAdminRequest(@TempDir final Path dataDir)
      throws Exception {
    final int leaderPort = freePort();
    final int leaderRaftPort = freePort();
    final int joiningPort = freePort();
    final int joiningRaftPort = freePort();
    final Path joiningDir = dataDir.resolve("meta-1");
    final Path removalMarker = joiningDir.resolve("removal-marker");
    try (PdNode leader =
        PdNode.start("meta-0", leaderPort, leaderRaftPort, "", dataDir.resolve("meta-0"))) {
      await(() -> "LEADER".equals(leader.health().path("role").asText()));
      try (PdNode joining =
          PdNode.start(
              "meta-1",
              joiningPort,
              joiningRaftPort,
              "meta-0@127.0.0.1:" + leaderPort + ":" + leaderRaftPort,
              joiningDir)) {
        await(() -> contains(leader.configuration().path("actual").path("learners"), "meta-1"));
        Files.writeString(removalMarker, "keep");
        assertEquals(1, leader.configuration().path("target").path("count").asInt());

        final HttpResponse<String> promote =
            leader.patch("/api/v1/pd/configuration", "{\"add\":[\"meta-1\"],\"count\":2}");
        assertEquals(202, promote.statusCode(), promote.body());
        final String promoteId = MAPPER.readTree(promote.body()).path("changeId").asText();
        await(
            () -> "COMPLETED".equals(leader.change(promoteId).path("status").asText()),
            () ->
                "promotion="
                    + leader.change(promoteId)
                    + ", configuration="
                    + leader.configuration()
                    + ", joining="
                    + joining.health());
        await(() -> contains(leader.configuration().path("actual").path("voters"), "meta-1"));

        final HttpResponse<String> remove =
            leader.patch("/api/v1/pd/configuration", "{\"remove\":[\"meta-1\"],\"count\":1}");
        assertEquals(202, remove.statusCode(), remove.body());
        final String removeId = MAPPER.readTree(remove.body()).path("changeId").asText();
        await(() -> "COMPLETED".equals(leader.change(removeId).path("status").asText()));
        await(
            () ->
                !contains(leader.configuration().path("actual").path("voters"), "meta-1")
                    && !contains(leader.configuration().path("actual").path("learners"), "meta-1"));

        await(() -> !joining.isActive());
        await(joining::allPortsClosed);
        assertTrue(Files.exists(joiningDir), "removed member must preserve its local data");
        assertEquals("keep", Files.readString(removalMarker));
        assertTrue(
            !contains(leader.configuration().path("actual").path("learners"), "meta-1"),
            "removed member must not be readmitted by renewal");
        assertEquals("LEADER", leader.health().path("role").asText());
      }
      try (PdNode restarted =
          PdNode.start(
              "meta-1",
              joiningPort,
              joiningRaftPort,
              "meta-0@127.0.0.1:" + leaderPort + ":" + leaderRaftPort,
              joiningDir)) {
        await(() -> contains(leader.configuration().path("actual").path("learners"), "meta-1"));
        assertTrue(restarted.isActive());
        assertTrue(Files.exists(joiningDir));
        assertEquals("keep", Files.readString(removalMarker));
        assertEquals(1, leader.configuration().path("target").path("count").asInt());
        final HttpResponse<String> promote =
            leader.patch("/api/v1/pd/configuration", "{\"add\":[\"meta-1\"],\"count\":2}");
        assertEquals(202, promote.statusCode(), promote.body());
        final String promoteId = MAPPER.readTree(promote.body()).path("changeId").asText();
        await(
            () -> "COMPLETED".equals(leader.change(promoteId).path("status").asText()),
            () ->
                "promotion="
                    + leader.change(promoteId)
                    + ", configuration="
                    + leader.configuration()
                    + ", restarted="
                    + restarted.health());
        await(() -> contains(leader.configuration().path("actual").path("voters"), "meta-1"));
      }
    }
  }

  @Test
  @Timeout(value = 90, unit = TimeUnit.SECONDS)
  void sameServiceCanRestartButAnotherEndpointCannotUseItsId(@TempDir final Path dataDir)
      throws Exception {
    final int leaderPort = freePort();
    final int leaderRaftPort = freePort();
    final int joiningPort = freePort();
    final int joiningRaftPort = freePort();
    final String peers = "meta-0@127.0.0.1:" + leaderPort + ":" + leaderRaftPort;
    final Path joiningDir = dataDir.resolve("meta-1");
    try (PdNode leader =
        PdNode.start("meta-0", leaderPort, leaderRaftPort, "", dataDir.resolve("meta-0"))) {
      await(() -> "LEADER".equals(leader.health().path("role").asText()));
      try (PdNode joining =
          PdNode.start("meta-1", joiningPort, joiningRaftPort, peers, joiningDir)) {
        await(() -> contains(leader.configuration().path("actual").path("learners"), "meta-1"));
        final Path marker = Files.writeString(joiningDir.resolve("restart-marker"), "keep");
        final int duplicatePort = freePort();
        final int duplicateRaftPort = freePort();
        final Path duplicateDir = Files.createDirectories(dataDir.resolve("duplicate"));
        final Path duplicateMarker = Files.writeString(duplicateDir.resolve("marker"), "keep");
        final RuntimeException failure =
            assertThrows(
                RuntimeException.class,
                () ->
                    PdNode.start("meta-1", duplicatePort, duplicateRaftPort, peers, duplicateDir));
        assertTrue(hasCause(failure, "PD_MEMBER_ID_CONFLICT"), failure.toString());
        assertTrue(joining.isActive());
        assertTrue(Files.exists(marker));
        assertTrue(Files.exists(duplicateMarker), "rejected startup must preserve its local data");
        assertTrue(!PdNode.isListening(duplicatePort));
        assertTrue(!PdNode.isListening(duplicateRaftPort));
      }
      assertTrue(Files.exists(joiningDir.resolve("restart-marker")));
      try (PdNode restarted =
          PdNode.start("meta-1", joiningPort, joiningRaftPort, peers, joiningDir)) {
        await(
            () ->
                "FOLLOWER".equals(restarted.health().path("role").asText())
                    || "LEARNER".equals(restarted.health().path("role").asText()));
        assertTrue(restarted.isActive());
        assertTrue(Files.exists(joiningDir.resolve("restart-marker")));
        assertEquals("LEADER", leader.health().path("role").asText());
      }
    }
  }

  private static boolean hasCause(final Throwable error, final String expected) {
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      if (cause.getMessage() != null && cause.getMessage().contains(expected)) {
        return true;
      }
    }
    return false;
  }

  private static boolean contains(final JsonNode values, final String expected) {
    for (final JsonNode value : values) {
      if (expected.equals(value.asText())) {
        return true;
      }
    }
    return false;
  }

  private static void await(final BooleanSupplier condition) throws Exception {
    await(condition, () -> "condition did not converge within 30 seconds");
  }

  private static void await(final BooleanSupplier condition, final Supplier<String> message)
      throws Exception {
    final long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (System.nanoTime() < deadline) {
      try {
        if (condition.getAsBoolean()) {
          return;
        }
      } catch (final RuntimeException ignored) {
        // Elections and membership changes are observed asynchronously.
      }
      Thread.sleep(100);
    }
    assertTrue(condition.getAsBoolean(), message);
  }

  private static int freePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static final class PdNode implements AutoCloseable {
    private final ConfigurableApplicationContext context;
    private final URI baseUri;
    private final int grpcPort;
    private final int raftPort;
    private final HttpClient client = HttpClient.newHttpClient();

    private PdNode(
        final ConfigurableApplicationContext context,
        final URI baseUri,
        final int grpcPort,
        final int raftPort) {
      this.context = context;
      this.baseUri = baseUri;
      this.grpcPort = grpcPort;
      this.raftPort = raftPort;
    }

    static PdNode start(
        final String nodeId,
        final int grpcPort,
        final int raftPort,
        final String peers,
        final Path dataDir) {
      final ConfigurableApplicationContext context =
          new SpringApplicationBuilder(PlacementDriverApplication.class)
              .run(
                  "--server.port=0",
                  "--taskite.pd.node-id=" + nodeId,
                  "--taskite.pd.network.host=127.0.0.1",
                  "--taskite.pd.network.grpc-port=" + grpcPort,
                  "--taskite.pd.network.raft-port=" + raftPort,
                  "--taskite.pd.consensus.election-timeout=300ms",
                  "--taskite.pd.consensus.heartbeat-interval=100ms",
                  "--taskite.pd.storage.data-dir=" + dataDir,
                  "--taskite.pd.consensus.bootstrap-peers=" + peers);
      final int httpPort = context.getEnvironment().getProperty("local.server.port", Integer.class);
      assertEquals(grpcPort, context.getBean(PlacementDriverSpringLifecycle.class).grpcPort());
      return new PdNode(context, URI.create("http://127.0.0.1:" + httpPort), grpcPort, raftPort);
    }

    boolean isActive() {
      return context.isActive();
    }

    boolean allPortsClosed() {
      return !isListening(baseUri.getPort()) && !isListening(grpcPort) && !isListening(raftPort);
    }

    private static boolean isListening(final int port) {
      try (Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress("127.0.0.1", port), 100);
        return true;
      } catch (final IOException ignored) {
        return false;
      }
    }

    JsonNode health() {
      return get("/api/v1/pd/health");
    }

    JsonNode configuration() {
      return get("/api/v1/pd/configuration");
    }

    JsonNode change(final String changeId) {
      return get("/api/v1/pd/changes/" + changeId);
    }

    HttpResponse<String> patch(final String path, final String body) {
      try {
        return client.send(
            HttpRequest.newBuilder(baseUri.resolve(path))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .method("PATCH", HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString());
      } catch (final Exception error) {
        throw new IllegalStateException(error);
      }
    }

    private JsonNode get(final String path) {
      try {
        final HttpResponse<String> response =
            client.send(
                HttpRequest.newBuilder(baseUri.resolve(path))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
          throw new IllegalStateException(response.body());
        }
        return MAPPER.readTree(response.body());
      } catch (final Exception error) {
        throw new IllegalStateException(error);
      }
    }

    @Override
    public void close() {
      context.close();
    }
  }
}
