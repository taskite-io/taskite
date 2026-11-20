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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.logging.LoggingApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;

final class PlacementDriverFollowerRpcRoutingIntegrationTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void pdConsoleQueriesPreserveResultsOnFollowersAndAfterLeaderChange(@TempDir final Path tempDir)
      throws Exception {
    try (PdCluster cluster = PdCluster.start(tempDir)) {
      final PdNode leader = cluster.awaitNodeWithRole("LEADER");
      final List<PdNode> followers = cluster.awaitNodesWithRole("FOLLOWER", 2);
      assertPdConsoleQueries(leader, "LEADER");
      for (final PdNode follower : followers) {
        assertPdConsoleQueries(follower, "FOLLOWER");
      }

      leader.close();

      final PdNode newLeader = cluster.awaitNodeWithRole("LEADER");
      final PdNode newFollower = cluster.awaitNodeWithRole("FOLLOWER");
      assertTrue(!newLeader.nodeId().equals(leader.nodeId()));
      assertPdConsoleQueries(newLeader, "LEADER");
      assertPdConsoleQueries(newFollower, "FOLLOWER");
    }
  }

  private static void assertPdConsoleQueries(final PdNode node, final String role)
      throws Exception {
    final HttpResponse<String> topology = node.awaitRequest("GET", "/api/v1/pd/topology", "");
    final HttpResponse<String> configuration =
        node.awaitRequest("GET", "/api/v1/pd/configuration", "");
    final HttpResponse<String> active = node.awaitRequest("GET", "/api/v1/pd/changes/active", "");
    final HttpResponse<String> missing =
        node.awaitRequest("GET", "/api/v1/pd/changes/missing-change", "");
    final JsonNode topologyBody = MAPPER.readTree(topology.body());
    final JsonNode configurationBody = MAPPER.readTree(configuration.body());
    assertAll(
        () -> assertEquals(role, node.health().path("role").asText()),
        () -> assertEquals(200, topology.statusCode(), topology.body()),
        () -> assertEquals(node.nodeId(), topologyBody.path("localMemberId").asText()),
        () -> assertTrue(!topologyBody.path("leaderId").asText().isBlank()),
        () -> assertEquals(3, topologyBody.path("members").size()),
        () -> assertEquals(200, configuration.statusCode(), configuration.body()),
        () -> assertEquals(3, configurationBody.path("target").path("count").asInt()),
        () -> assertEquals(3, configurationBody.path("actual").path("voters").size()),
        () -> assertEquals(404, active.statusCode(), active.body()),
        () ->
            assertEquals(
                "ACTIVE_PD_MEMBER_CHANGE_NOT_FOUND",
                MAPPER.readTree(active.body()).path("error").asText()),
        () -> assertEquals(404, missing.statusCode(), missing.body()),
        () ->
            assertEquals(
                "PD_MEMBER_CHANGE_NOT_FOUND",
                MAPPER.readTree(missing.body()).path("error").asText()));
  }

  @Test
  void followerRoutesHttpCommandsAndQueriesToLeaderGrpc(@TempDir final Path tempDir)
      throws Exception {
    try (PdCluster cluster = PdCluster.start(tempDir)) {
      final PdNode leader = cluster.awaitNodeWithRole("LEADER");
      final List<PdNode> followers = cluster.awaitNodesWithRole("FOLLOWER", 2);
      final PdNode firstFollower = followers.get(0);
      final PdNode secondFollower = followers.get(1);
      assertNotNull(leader);
      assertNotNull(firstFollower);

      final String request =
          "{\"clusterId\":\"routed\",\"clusterSize\":1,\"partitionSize\":1,"
              + "\"replicationFactor\":1}";
      final HttpResponse<String> created =
          firstFollower.awaitRequest("POST", "/api/v1/broker/clusters", request);
      final HttpResponse<String> queried =
          secondFollower.awaitRequest("GET", "/api/v1/broker/clusters/routed/configuration", "");
      final JsonNode configuration = MAPPER.readTree(queried.body());

      assertAll(
          () ->
              assertTrue(
                  created.statusCode() == 200 || created.statusCode() == 201, created.body()),
          () -> assertEquals("routed", MAPPER.readTree(created.body()).path("clusterId").asText()),
          () -> assertEquals(200, queried.statusCode(), queried.body()),
          () -> assertEquals("routed", configuration.path("clusterId").asText()),
          () -> assertEquals(1, configuration.path("clusterSize").asInt()),
          () -> assertEquals("FOLLOWER", firstFollower.health().path("role").asText()),
          () -> assertEquals("FOLLOWER", secondFollower.health().path("role").asText()));
    }
  }

  @Test
  void followerRoutesToNewLeaderAfterLeaderChange(@TempDir final Path tempDir) throws Exception {
    try (PdCluster cluster = PdCluster.start(tempDir)) {
      final PdNode originalLeader = cluster.awaitNodeWithRole("LEADER");
      final PdNode originalFollower = cluster.awaitNodeWithRole("FOLLOWER");
      final String beforeFailover =
          "{\"clusterId\":\"before-failover\",\"clusterSize\":1,\"partitionSize\":1,"
              + "\"replicationFactor\":1}";
      final HttpResponse<String> createdBefore =
          originalFollower.awaitRequest("POST", "/api/v1/broker/clusters", beforeFailover);
      assertTrue(
          createdBefore.statusCode() == 200 || createdBefore.statusCode() == 201,
          createdBefore.body());

      originalLeader.close();

      final PdNode newLeader = cluster.awaitNodeWithRole("LEADER");
      final PdNode newFollower = cluster.awaitNodeWithRole("FOLLOWER");
      final HttpResponse<String> recovered =
          newFollower.awaitRequest(
              "GET", "/api/v1/broker/clusters/before-failover/configuration", "");
      final String afterFailover =
          "{\"clusterId\":\"after-failover\",\"clusterSize\":1,\"partitionSize\":1,"
              + "\"replicationFactor\":1}";
      final HttpResponse<String> createdAfter =
          newFollower.awaitRequest("POST", "/api/v1/broker/clusters", afterFailover);
      final HttpResponse<String> queriedAfter =
          newFollower.awaitRequest(
              "GET", "/api/v1/broker/clusters/after-failover/configuration", "");

      assertAll(
          () -> assertTrue(!newLeader.nodeId().equals(originalLeader.nodeId())),
          () -> assertEquals(200, recovered.statusCode(), recovered.body()),
          () ->
              assertEquals(
                  "before-failover", MAPPER.readTree(recovered.body()).path("clusterId").asText()),
          () ->
              assertTrue(
                  createdAfter.statusCode() == 200 || createdAfter.statusCode() == 201,
                  createdAfter.body()),
          () -> assertEquals(200, queriedAfter.statusCode(), queriedAfter.body()),
          () ->
              assertEquals(
                  "after-failover",
                  MAPPER.readTree(queriedAfter.body()).path("clusterId").asText()));
    }
  }

  private static final class PdCluster implements AutoCloseable {
    private final List<PdNode> nodes;

    private PdCluster(final List<PdNode> nodes) {
      this.nodes = nodes;
    }

    static PdCluster start(final Path dataDir) throws Exception {
      final int[] grpcPorts = {freePort(), freePort(), freePort()};
      final int[] raftPorts = {freePort(), freePort(), freePort()};
      final List<PdNode> nodes = new ArrayList<>();
      final List<CompletableFuture<PdNode>> startups = new ArrayList<>();
      try {
        for (int index = 0; index < grpcPorts.length; index++) {
          final int nodeIndex = index;
          startups.add(
              CompletableFuture.supplyAsync(
                  () ->
                      PdNode.start(
                          nodeIndex, grpcPorts, raftPorts, dataDir.resolve("meta-" + nodeIndex))));
        }
        for (final var startup : startups) {
          nodes.add(startup.join());
        }
        return new PdCluster(nodes);
      } catch (final Throwable error) {
        startups.forEach(startup -> startup.thenAccept(PdNode::close));
        for (final PdNode node : nodes) {
          node.close();
        }
        throw error;
      }
    }

    PdNode awaitNodeWithRole(final String role) throws Exception {
      return awaitNodesWithRole(role, 1).getFirst();
    }

    List<PdNode> awaitNodesWithRole(final String role, final int count) throws Exception {
      for (int attempt = 0; attempt < 300; attempt++) {
        final List<PdNode> matching = new ArrayList<>();
        for (final PdNode node : nodes) {
          if (!node.isClosed()) {
            try {
              final JsonNode health = node.health();
              if (role.equals(health.path("role").asText())
                  && !health.path("leaderId").asText().isBlank()) {
                matching.add(node);
              }
            } catch (final Exception ignored) {
              // A surviving node may briefly reject HTTP while the new Leader view converges.
            }
          }
        }
        if (matching.size() >= count) {
          return matching;
        }
        Thread.sleep(50);
      }
      throw new AssertionError("placement-driver cluster did not expose a " + role);
    }

    @Override
    public void close() {
      for (int index = nodes.size() - 1; index >= 0; index--) {
        nodes.get(index).close();
      }
    }
  }

  private static final class PdNode implements AutoCloseable {
    private final String nodeId;
    private final ConfigurableApplicationContext context;
    private final URI baseUri;
    private final HttpClient client = HttpClient.newHttpClient();
    private boolean closed;

    private PdNode(
        final String nodeId, final ConfigurableApplicationContext context, final URI baseUri) {
      this.nodeId = nodeId;
      this.context = context;
      this.baseUri = baseUri;
    }

    static PdNode start(
        final int index, final int[] grpcPorts, final int[] raftPorts, final Path dataDir) {
      final String nodeId = "meta-" + index;
      final List<String> peers = new ArrayList<>();
      for (int peer = 0; peer < grpcPorts.length; peer++) {
        if (peer != index) {
          peers.add("meta-" + peer + "@127.0.0.1:" + grpcPorts[peer] + ":" + raftPorts[peer]);
        }
      }
      final SpringApplicationBuilder builder =
          new SpringApplicationBuilder(PlacementDriverApplication.class);
      // Parallel contexts share the JVM's Logback context; keep its existing configuration.
      builder
          .application()
          .setListeners(
              builder.application().getListeners().stream()
                  .filter(listener -> !(listener instanceof LoggingApplicationListener))
                  .toList());
      final ConfigurableApplicationContext context =
          builder.run(
              "--server.port=0",
              "--taskite.pd.node-id=" + nodeId,
              "--taskite.pd.network.host=127.0.0.1",
              "--taskite.pd.network.grpc-port=" + grpcPorts[index],
              "--taskite.pd.network.raft-port=" + raftPorts[index],
              "--taskite.pd.consensus.election-timeout=300ms",
              "--taskite.pd.consensus.heartbeat-interval=100ms",
              "--taskite.pd.storage.data-dir=" + dataDir,
              "--taskite.pd.consensus.bootstrap-peers=" + String.join(",", peers));
      final int httpPort = context.getEnvironment().getProperty("local.server.port", Integer.class);
      return new PdNode(nodeId, context, URI.create("http://127.0.0.1:" + httpPort));
    }

    String nodeId() {
      return nodeId;
    }

    boolean isClosed() {
      return closed;
    }

    JsonNode health() throws Exception {
      return MAPPER.readTree(request("GET", "/api/v1/pd/health", "").body());
    }

    HttpResponse<String> request(final String method, final String path, final String body)
        throws Exception {
      final HttpRequest.Builder request =
          HttpRequest.newBuilder(baseUri.resolve(path)).timeout(Duration.ofSeconds(10));
      if ("GET".equals(method)) {
        request.GET();
      } else {
        request
            .header("Content-Type", "application/json")
            .method(method, HttpRequest.BodyPublishers.ofString(body));
      }
      return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    HttpResponse<String> awaitRequest(final String method, final String path, final String body)
        throws Exception {
      HttpResponse<String> response = null;
      for (int attempt = 0; attempt < 100; attempt++) {
        response = request(method, path, body);
        if (response.statusCode() != 503
            || !response.body().contains("placement-driver leader is not available")) {
          return response;
        }
        Thread.sleep(50);
      }
      return response;
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        context.close();
      }
    }
  }

  private static int freePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
