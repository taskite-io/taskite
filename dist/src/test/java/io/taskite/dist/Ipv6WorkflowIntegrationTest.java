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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.taskite.broker.Broker;
import io.taskite.broker.BrokerConfig;
import io.taskite.gateway.Gateway;
import io.taskite.gateway.GatewayConfig;
import io.taskite.gateway.api.grpc.GatewayInterceptorUtil;
import io.taskite.gateway.protocol.CreateJobRequest;
import io.taskite.gateway.protocol.CreateJobSubscriptionRequest;
import io.taskite.gateway.protocol.GatewayGrpc;
import io.taskite.placementdriver.PlacementDriverApplication;
import io.taskite.placementdriver.bootstrap.PlacementDriverSpringLifecycle;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;

/** Exercises real IPv6 HTTP, PD discovery, subscription distribution and job routing. */
final class Ipv6WorkflowIntegrationTest {
  @Test
  @Timeout(120)
  void createsSubscriptionAndJobThroughIpv6ControlAndDataPlanes(@TempDir final Path directory)
      throws Exception {
    try (var pd =
        new SpringApplicationBuilder(PlacementDriverApplication.class)
            .run(
                "--server.address=::1",
                "--server.port=0",
                "--taskite.pd.node-id=ipv6-pd",
                "--taskite.pd.network.host=::1",
                "--taskite.pd.network.grpc-port=0",
                "--taskite.pd.network.raft-port=" + port(),
                "--taskite.pd.consensus.election-timeout=300ms",
                "--taskite.pd.consensus.heartbeat-interval=100ms",
                "--taskite.pd.storage.data-dir=" + directory.resolve("pd"),
                "--taskite.pd.consensus.bootstrap-peers=",
                "--spring.main.banner-mode=off")) {
      final var http = HttpClient.newHttpClient();
      final var base =
          "http://[::1]:" + pd.getEnvironment().getRequiredProperty("local.server.port");
      await(
          () -> {
            try {
              final var response =
                  http.send(
                      HttpRequest.newBuilder(URI.create(base + "/api/v1/pd/health")).GET().build(),
                      HttpResponse.BodyHandlers.ofString());
              return "LEADER"
                  .equals(new ObjectMapper().readTree(response.body()).path("role").asText());
            } catch (final Exception error) {
              return false;
            }
          });
      final var created =
          http.send(
              HttpRequest.newBuilder(URI.create(base + "/api/v1/broker/clusters"))
                  .header("Content-Type", "application/json")
                  .POST(
                      HttpRequest.BodyPublishers.ofString(
                          "{\"clusterId\":\"ipv6\",\"clusterSize\":1,\"partitionSize\":1,\"replicationFactor\":1}"))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertTrue(created.statusCode() == 200 || created.statusCode() == 201, created.body());

      final var pdEndpoint = "[::1]:" + pd.getBean(PlacementDriverSpringLifecycle.class).grpcPort();
      final var broker =
          new Broker(
              new BrokerConfig(
                  new BrokerConfig.Identity("ipv6", "b0", "test"),
                  new BrokerConfig.Node(
                      new BrokerConfig.Network("::1", port(), port()), directory.resolve("broker")),
                  new BrokerConfig.Registration(
                      pdEndpoint,
                      Duration.ofMillis(200),
                      Duration.ofSeconds(30),
                      Duration.ofMillis(200)),
                  new BrokerConfig.Engine(
                      new BrokerConfig.Snapshot(Duration.ofMinutes(15), 128L * 1024 * 1024),
                      new BrokerConfig.Consensus(Duration.ofMillis(500)),
                      BrokerConfig.defaultProcessing())));
      final var gateway =
          new Gateway(
              new GatewayConfig(
                  new GatewayConfig.Network("::1", 0, Duration.ofSeconds(30)),
                  new GatewayConfig.PlacementDriver(
                      pdEndpoint, Duration.ofSeconds(10), Duration.ofSeconds(1)),
                  new GatewayConfig.Broker(Duration.ofSeconds(10), 3),
                  new GatewayConfig.Threads(2),
                  new GatewayConfig.MultiTenancy(false, List.of())));
      try {
        broker.start().get(30, TimeUnit.SECONDS);
        await(broker::ready);
        gateway.start().get(30, TimeUnit.SECONDS);
        final var channel =
            NettyChannelBuilder.forAddress("::1", gateway.port()).usePlaintext().build();
        try {
          final var metadata = new Metadata();
          metadata.put(GatewayInterceptorUtil.CLUSTER_ID_METADATA_KEY, "ipv6");
          final var stub =
              GatewayGrpc.newBlockingStub(channel)
                  .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
                  .withDeadlineAfter(15, TimeUnit.SECONDS);
          final long key =
              stub.createJobSubscription(
                      CreateJobSubscriptionRequest.newBuilder()
                          .setName("ipv6")
                          .setJobType("ipv6-job")
                          .setVariables("{\"enabled\":true}")
                          .build())
                  .getKey();
          assertTrue(key > 0);
          await(() -> broker.jobSubscription(1, key).isPresent());
          final var job =
              stub.createJob(CreateJobRequest.newBuilder().setJobType("ipv6-job").build());
          assertEquals(key, job.getSubscriptionKey());
          assertEquals("ipv6-job", job.getJobType());
          assertTrue(job.getJobKey() > 0);
        } finally {
          channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
      } finally {
        gateway.stop().get(20, TimeUnit.SECONDS);
        broker.stop().get(20, TimeUnit.SECONDS);
      }
    }
  }

  private static int port() throws Exception {
    try (var socket = new ServerSocket(0, 50, InetAddress.getByName("::1"))) {
      return socket.getLocalPort();
    }
  }

  private static void await(final BooleanSupplier condition) throws Exception {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(25);
    assertTrue(condition.getAsBoolean(), "IPv6 task scheduling did not converge within 30s");
  }
}
