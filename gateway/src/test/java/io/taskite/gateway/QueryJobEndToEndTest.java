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
package io.taskite.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.taskite.broker.Broker;
import io.taskite.broker.BrokerConfig;
import io.taskite.broker.BrokerSpec;
import io.taskite.gateway.api.grpc.GatewayInterceptorUtil;
import io.taskite.gateway.protocol.CreateJobRequest;
import io.taskite.gateway.protocol.CreateJobSubscriptionRequest;
import io.taskite.gateway.protocol.DeleteJobSubscriptionRequest;
import io.taskite.gateway.protocol.GatewayGrpc;
import io.taskite.gateway.protocol.QueryJobRequest;
import io.taskite.placementdriver.PlacementDriver;
import io.taskite.placementdriver.PlacementDriverConfiguration;
import io.taskite.placementdriver.runtime.transport.InternalPlacementDriverCommandGrpc;
import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.encoding.ExecuteCommandRequest;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.ClusterTopology;
import io.taskite.topology.Partition;
import io.taskite.topology.PartitionStatus;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class QueryJobEndToEndTest {
  private static final int TOPOLOGY_RAFT_PORT = 30_611;

  @Test
  void shouldIsolateJobsAndSubscriptionsAcrossBrokerClusters(@TempDir final Path tempDir)
      throws Exception {
    final String clusterA = "cluster-a-" + UUID.randomUUID();
    final String clusterB = "cluster-b-" + UUID.randomUUID();
    final int pdHttpPort = freePort();
    final int pdGrpcPort = freePort();
    int pdRaftPort;
    do {
      pdRaftPort = freePort();
    } while (pdRaftPort == pdGrpcPort);
    PlacementDriver placementDriver = null;
    Broker brokerA = null;
    Broker brokerB = null;
    Gateway gateway = null;
    ManagedChannel channel = null;
    try {
      placementDriver =
          new PlacementDriver(
              new PlacementDriverConfiguration(
                  "meta-0",
                  new PlacementDriverConfiguration.Network("127.0.0.1", pdGrpcPort, pdRaftPort),
                  new PlacementDriverConfiguration.Consensus(
                      Duration.ofMillis(150), Duration.ofMillis(50)),
                  new PlacementDriverConfiguration.Storage(tempDir.resolve("pd"), null),
                  PlacementDriverConfiguration.DEFAULT_CHANGE_HISTORY_LIMIT,
                  null,
                  null,
                  null),
              pdHttpPort);
      placementDriver.start().join();
      createCluster(pdGrpcPort, clusterA, 1);
      createCluster(pdGrpcPort, clusterB, 1);

      brokerA =
          new Broker(
              brokerConfig(
                  clusterA,
                  "broker-0",
                  "127.0.0.1",
                  freePort(),
                  tempDir.resolve("brokers"),
                  "127.0.0.1:" + pdGrpcPort));
      brokerB =
          new Broker(
              brokerConfig(
                  clusterB,
                  "broker-0",
                  "127.0.0.1",
                  freePort(),
                  tempDir.resolve("brokers"),
                  "127.0.0.1:" + pdGrpcPort));
      brokerA.start().join();
      brokerB.start().join();
      assertTrue(
          brokerA.awaitPartitionLeadership(Duration.ofSeconds(10)),
          "cluster A broker did not elect a partition leader in time");
      assertTrue(
          brokerB.awaitPartitionLeadership(Duration.ofSeconds(10)),
          "cluster B broker did not elect a partition leader in time");

      gateway =
          new Gateway(
              new GatewayConfig(
                  new GatewayConfig.Network("127.0.0.1", 0, Duration.ofSeconds(30)),
                  new GatewayConfig.PlacementDriver(
                      "127.0.0.1:" + pdGrpcPort, Duration.ofSeconds(10), Duration.ofSeconds(1)),
                  new GatewayConfig.Broker(Duration.ofSeconds(15), 0),
                  new GatewayConfig.Threads(GatewayConfig.DEFAULT_MANAGEMENT_THREADS),
                  new GatewayConfig.MultiTenancy(false, List.of())));
      gateway.start().join();
      channel =
          ManagedChannelBuilder.forAddress("127.0.0.1", gateway.port()).usePlaintext().build();
      final var clientA = client(channel, clusterA);
      final var clientB = client(channel, clusterB);
      final String unknownCluster = "cluster-c-" + UUID.randomUUID();
      final var unknownClient = client(channel, unknownCluster);

      final var resourceA =
          clientA.createJobSubscription(
              CreateJobSubscriptionRequest.newBuilder()
                  .setName("shared")
                  .setJobType("charge")
                  .build());
      final StatusRuntimeException unknownBeforeAccessingB =
          assertThrows(
              StatusRuntimeException.class,
              () -> unknownClient.queryJob(QueryJobRequest.getDefaultInstance()));
      assertEquals(Status.Code.NOT_FOUND, unknownBeforeAccessingB.getStatus().getCode());
      assertEquals(
          unknownClusterMessage(unknownCluster, List.of(clusterA)),
          unknownBeforeAccessingB.getStatus().getDescription());
      assertNoClusterTrailers(unknownBeforeAccessingB);

      final var resourceB =
          clientB.createJobSubscription(
              CreateJobSubscriptionRequest.newBuilder()
                  .setName("shared")
                  .setJobType("charge")
                  .build());
      final StatusRuntimeException unknownAfterAccessingB =
          assertThrows(
              StatusRuntimeException.class,
              () -> unknownClient.queryJob(QueryJobRequest.getDefaultInstance()));
      assertEquals(Status.Code.NOT_FOUND, unknownAfterAccessingB.getStatus().getCode());
      assertEquals(
          unknownClusterMessage(unknownCluster, List.of(clusterA, clusterB)),
          unknownAfterAccessingB.getStatus().getDescription());
      assertNoClusterTrailers(unknownAfterAccessingB);

      assertNotEquals(resourceA.getKey(), resourceB.getKey());
      assertEquals(clusterA, awaitSubscription(brokerA, resourceA.getKey()).getClusterId());
      assertEquals(clusterB, awaitSubscription(brokerB, resourceB.getKey()).getClusterId());
      assertTrue(brokerA.jobSubscription(1, resourceB.getKey()).isEmpty());
      assertTrue(brokerB.jobSubscription(1, resourceA.getKey()).isEmpty());

      final long instanceA =
          clientA.createJob(CreateJobRequest.newBuilder().setJobType("charge").build()).getJobKey();
      final long instanceB =
          clientB.createJob(CreateJobRequest.newBuilder().setJobType("charge").build()).getJobKey();
      assertTrue(instanceA > 0 && instanceB > 0);
      assertNotEquals(instanceA, instanceB);
      assertEquals(1, Protocol.decodeClusterKey(instanceA));
      assertEquals(2, Protocol.decodeClusterKey(instanceB));
      assertEquals(1, Protocol.decodePartitionId(instanceA));
      assertEquals(1, Protocol.decodePartitionId(instanceB));
      assertEquals(
          "charge",
          clientA.queryJob(QueryJobRequest.newBuilder().setJobKey(instanceA).build()).getType());
      assertEquals(
          "charge",
          clientB.queryJob(QueryJobRequest.newBuilder().setJobKey(instanceB).build()).getType());
      assertEquals(
          Status.Code.INVALID_ARGUMENT,
          assertThrows(
                  StatusRuntimeException.class,
                  () -> clientB.queryJob(QueryJobRequest.newBuilder().setJobKey(instanceA).build()))
              .getStatus()
              .getCode());
      assertEquals(
          Status.Code.INVALID_ARGUMENT,
          assertThrows(
                  StatusRuntimeException.class,
                  () -> clientA.queryJob(QueryJobRequest.newBuilder().setJobKey(instanceB).build()))
              .getStatus()
              .getCode());

    } finally {
      if (channel != null) {
        closeGracefully(channel);
      }
      if (gateway != null) {
        gateway.stop().join();
      }
      if (brokerB != null) {
        brokerB.stop().join();
      }
      if (brokerA != null) {
        brokerA.stop().join();
      }
      if (placementDriver != null) {
        placementDriver.stop().join();
      }
    }
  }

  @Test
  void shouldCreateDeduplicateAndDeleteSubscriptionThroughPlacementDriver(
      @TempDir final Path tempDir) throws Exception {
    final String clusterId = "gateway-query-e2e-" + UUID.randomUUID();
    final int pdHttpPort = freePort();
    final int pdGrpcPort = freePort();
    int pdRaftPort;
    do {
      pdRaftPort = freePort();
    } while (pdRaftPort == pdGrpcPort);
    final var brokerSpec = new BrokerSpec("broker-0", "127.0.0.1", freePort());
    PlacementDriver placementDriver = null;
    Broker broker = null;
    Gateway gateway = null;
    ManagedChannel channel = null;
    try {
      placementDriver =
          new PlacementDriver(
              new PlacementDriverConfiguration(
                  "meta-0",
                  new PlacementDriverConfiguration.Network("127.0.0.1", pdGrpcPort, pdRaftPort),
                  new PlacementDriverConfiguration.Consensus(
                      Duration.ofMillis(150), Duration.ofMillis(50)),
                  new PlacementDriverConfiguration.Storage(tempDir.resolve("pd"), null),
                  PlacementDriverConfiguration.DEFAULT_CHANGE_HISTORY_LIMIT,
                  null,
                  null,
                  null),
              pdHttpPort);
      placementDriver.start().join();
      createCluster(pdGrpcPort, clusterId, 1);

      broker =
          new Broker(
              brokerConfig(
                  clusterId,
                  brokerSpec.brokerId(),
                  brokerSpec.host(),
                  brokerSpec.port(),
                  tempDir.resolve("broker"),
                  "127.0.0.1:" + pdGrpcPort));
      broker.start().join();
      assertTrue(
          broker.awaitPartitionLeadership(Duration.ofSeconds(10)),
          "broker did not elect a partition leader in time");

      gateway =
          new Gateway(
              new GatewayConfig(
                  new GatewayConfig.Network("127.0.0.1", 0, Duration.ofSeconds(30)),
                  new GatewayConfig.PlacementDriver(
                      "127.0.0.1:" + pdGrpcPort, Duration.ofSeconds(10), Duration.ofSeconds(1)),
                  new GatewayConfig.Broker(Duration.ofSeconds(15), 0),
                  new GatewayConfig.Threads(GatewayConfig.DEFAULT_MANAGEMENT_THREADS),
                  new GatewayConfig.MultiTenancy(false, List.of())));
      gateway.start().join();

      channel =
          ManagedChannelBuilder.forAddress("127.0.0.1", gateway.port()).usePlaintext().build();
      final var client = client(channel, clusterId);
      final var request =
          CreateJobSubscriptionRequest.newBuilder()
              .setName("payment")
              .setJobType("charge")
              .setVariables("{\"amount\":42}")
              .setRetries(5)
              .setPriority(70)
              .build();
      final var response = client.createJobSubscription(request);
      assertTrue(response.getKey() > 0);
      final JobSubscriptionRecord subscription = awaitSubscription(broker, response.getKey());
      assertEquals("payment", subscription.getName());
      assertEquals("charge", subscription.getJobType());
      assertEquals("{\"amount\":42}", subscription.getVariables());
      assertEquals(5, subscription.getRetries());
      assertEquals(70, subscription.getPriority());

      final var duplicate =
          client.createJobSubscription(request.toBuilder().setName("another-name").build());
      assertEquals(response.getKey(), duplicate.getKey());
      final var createdJob =
          client.createJob(CreateJobRequest.newBuilder().setJobType("charge").build());
      final long jobKey = createdJob.getJobKey();
      assertEquals(response.getKey(), createdJob.getSubscriptionKey());
      assertEquals(
          response.getKey(),
          client
              .queryJob(QueryJobRequest.newBuilder().setJobKey(jobKey).build())
              .getSubscriptionKey());
      assertEquals(
          "{\"amount\":42}",
          client.queryJob(QueryJobRequest.newBuilder().setJobKey(jobKey).build()).getVariables());
      final var deletion =
          client.deleteJobSubscription(
              DeleteJobSubscriptionRequest.newBuilder().setKey(response.getKey()).build());
      assertEquals(response.getKey(), deletion.getKey());
      awaitSubscriptionDeleted(broker, response.getKey());
      final StatusRuntimeException deletedJob =
          assertThrows(
              StatusRuntimeException.class,
              () -> client.queryJob(QueryJobRequest.newBuilder().setJobKey(jobKey).build()));
      assertEquals(Status.Code.NOT_FOUND, deletedJob.getStatus().getCode());

      final var duplicateDelete =
          client.deleteJobSubscription(
              DeleteJobSubscriptionRequest.newBuilder().setKey(response.getKey()).build());
      assertEquals(deletion, duplicateDelete);
      final StatusRuntimeException invalidSubscription =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  client.createJobSubscription(
                      CreateJobSubscriptionRequest.newBuilder()
                          .setName("invalid")
                          .setJobType("")
                          .build()));
      assertEquals(Status.Code.INVALID_ARGUMENT, invalidSubscription.getStatus().getCode());
    } finally {
      if (channel != null) {
        closeGracefully(channel);
      }
      if (gateway != null) {
        gateway.stop().join();
      }
      if (broker != null) {
        broker.stop().join();
      }
      if (placementDriver != null) {
        placementDriver.stop().join();
      }
    }
  }

  private static void closeGracefully(final ManagedChannel channel) throws InterruptedException {
    channel.shutdown();
    if (!channel.awaitTermination(1, TimeUnit.SECONDS)) {
      channel.shutdownNow();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private static GatewayGrpc.GatewayBlockingStub client(
      final ManagedChannel channel, final String clusterId) {
    final Metadata headers = new Metadata();
    headers.put(GatewayInterceptorUtil.CLUSTER_ID_METADATA_KEY, clusterId);
    return GatewayGrpc.newBlockingStub(
        ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(headers)));
  }

  private static String unknownClusterMessage(
      final String requestedClusterId, final List<String> knownClusterIds) {
    final String clusters =
        knownClusterIds.stream()
            .sorted()
            .map(clusterId -> "\"" + clusterId + "\"")
            .collect(Collectors.joining(", ", "[", "]"));
    return "UNKNOWN_CLUSTER: requested cluster \""
        + requestedClusterId
        + "\" is not known by this gateway, know clusters: "
        + clusters;
  }

  private static void assertNoClusterTrailers(final StatusRuntimeException error) {
    final Metadata trailers = error.getTrailers();
    if (trailers != null) {
      assertFalse(trailers.keys().contains("x-requested-cluster-id"));
      assertFalse(trailers.keys().contains("x-known-clusters"));
    }
  }

  private static void closeGracefully(final Server server) throws InterruptedException {
    server.shutdown();
    if (!server.awaitTermination(1, TimeUnit.SECONDS)) {
      server.shutdownNow();
      server.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private static int freePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"))) {
      return socket.getLocalPort();
    }
  }

  private static BrokerConfig brokerConfig(
      final String clusterId,
      final String brokerId,
      final String host,
      final int apiPort,
      final Path dataDirectory,
      final String placementDrivers)
      throws Exception {
    int raftPort;
    do {
      raftPort = freePort();
    } while (raftPort == apiPort);
    return new BrokerConfig(
        new BrokerConfig.Identity(clusterId, brokerId, "test"),
        new BrokerConfig.Node(new BrokerConfig.Network(host, apiPort, raftPort), dataDirectory),
        new BrokerConfig.Registration(
            placementDrivers,
            Duration.ofMillis(200),
            BrokerConfig.DEFAULT_RENEWAL_TIMEOUT,
            Duration.ofMillis(200)),
        new BrokerConfig.Engine(
            new BrokerConfig.Snapshot(
                BrokerConfig.DEFAULT_SNAPSHOT_INTERVAL,
                BrokerConfig.DEFAULT_SNAPSHOT_LOG_SIZE_THRESHOLD_BYTES),
            new BrokerConfig.Consensus(BrokerConfig.DEFAULT_ELECTION_TIMEOUT),
            BrokerConfig.defaultProcessing()));
  }

  private static void createCluster(
      final int pdGrpcPort, final String clusterId, final int partitionSize) throws Exception {
    final ClusterConfigRecord value =
        new ClusterConfigRecord()
            .setClusterId(clusterId)
            .setClusterSize(1)
            .setPartitionSize(partitionSize)
            .setReplicationFactor(1)
            .setRosterManaged(true);
    final UnsafeBuffer valueBuffer = new UnsafeBuffer(new byte[value.getLength()]);
    value.write(valueBuffer, 0);
    final ExecuteCommandRequest command =
        new ExecuteCommandRequest()
            .setPartitionId(1)
            .setValueType(ValueType.TOPOLOGY)
            .setIntent(TopologyIntent.CREATE_CLUSTER)
            .setValue(valueBuffer, 0, valueBuffer.capacity());
    final UnsafeBuffer request = new UnsafeBuffer(new byte[command.getLength()]);
    command.write(request, 0);
    final ManagedChannel channel =
        ManagedChannelBuilder.forAddress("127.0.0.1", pdGrpcPort).usePlaintext().build();
    try {
      InternalPlacementDriverCommandGrpc.executeCommand(
              channel,
              Arrays.copyOf(request.byteArray(), command.getLength()),
              Duration.ofSeconds(5))
          .join();
    } finally {
      closeGracefully(channel);
    }
  }

  private static JobSubscriptionRecord awaitSubscription(
      final Broker broker, final long distributionKey) throws InterruptedException {
    final long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (System.nanoTime() < deadline) {
      final var resource = broker.jobSubscription(1, distributionKey);
      if (resource.isPresent()) {
        return resource.get();
      }
      Thread.sleep(50);
    }
    throw new AssertionError("subscription was not distributed to broker: " + distributionKey);
  }

  private static void awaitSubscriptionDeleted(final Broker broker, final long distributionKey)
      throws InterruptedException {
    final long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (System.nanoTime() < deadline) {
      if (broker.jobSubscription(1, distributionKey).isEmpty()) {
        return;
      }
      Thread.sleep(50);
    }
    throw new AssertionError("subscription was not deleted from broker: " + distributionKey);
  }

  private static ClusterTopology topology(final String clusterId, final BrokerSpec brokerSpec) {
    return topology(clusterId, brokerSpec, 1);
  }

  private static ClusterTopology topology(
      final String clusterId, final BrokerSpec brokerSpec, final int partitionSize) {
    final var partitions = new ArrayList<Partition>();
    for (int partitionId = 1; partitionId <= partitionSize; partitionId++) {
      partitions.add(
          new Partition(
              partitionId,
              new Partition.Leadership(brokerSpec.brokerId(), 1),
              new Partition.Membership(List.of(brokerSpec.brokerId()), List.of(), Map.of(), 0),
              PartitionStatus.ACTIVE));
    }
    return new ClusterTopology(
        clusterId,
        1,
        partitionSize,
        1,
        partitions,
        Map.of(
            brokerSpec.brokerId(),
            new BrokerEndpoint(
                brokerSpec.brokerId(), brokerSpec.host(), brokerSpec.port(), TOPOLOGY_RAFT_PORT)),
        Instant.now());
  }
}
