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
package io.taskite.broker;

import static io.taskite.broker.BrokerTopologyTestRecords.broker;
import static io.taskite.placementdriver.protocol.Partition.newBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.taskite.broker.bootstrap.BrokerBootstrapTopology;
import io.taskite.placementdriver.protocol.BrokerPartitionState;
import io.taskite.placementdriver.protocol.BrokerTopology;
import io.taskite.placementdriver.protocol.DeregisterBrokerRequest;
import io.taskite.placementdriver.protocol.DeregisterBrokerResponse;
import io.taskite.placementdriver.protocol.GetBrokerTopologyRequest;
import io.taskite.placementdriver.protocol.GetClusterConfigRequest;
import io.taskite.placementdriver.protocol.GetClusterConfigResponse;
import io.taskite.placementdriver.protocol.Partition.Builder;
import io.taskite.placementdriver.protocol.PlacementDriverGrpc;
import io.taskite.placementdriver.protocol.PlacementDriverMember;
import io.taskite.placementdriver.protocol.PlacementDriverTopology;
import io.taskite.placementdriver.protocol.RegisterBrokerRequest;
import io.taskite.placementdriver.protocol.RegisterBrokerResponse;
import io.taskite.placementdriver.protocol.RenewBrokerRequest;
import io.taskite.placementdriver.protocol.RenewBrokerResponse;
import io.taskite.placementdriver.protocol.WatchPlacementDriverTopologyRequest;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.ClusterTopology;
import io.taskite.topology.Partition;
import io.taskite.topology.Partition.Leadership;
import io.taskite.topology.Partition.Membership;
import io.taskite.topology.PartitionStatus;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class BrokerBootstrapTopologyTest {
  @TempDir Path tempDir;

  @Test
  void firstBrokerWithPlacementDriverBootstrapsPartitionsAsVoter() {
    final BrokerConfig config =
        new BrokerConfig(
            new BrokerConfig.Identity("demo", "broker-1", "test"),
            new BrokerConfig.Node(
                new BrokerConfig.Network("127.0.0.1", 26502, 26602),
                Path.of("target/test-broker-bootstrap")),
            new BrokerConfig.Registration(
                "127.0.0.1:29001",
                Duration.ofSeconds(1),
                Duration.ofSeconds(30),
                Duration.ofSeconds(1)),
            new BrokerConfig.Engine(
                new BrokerConfig.Snapshot(
                    BrokerConfig.DEFAULT_SNAPSHOT_INTERVAL,
                    BrokerConfig.DEFAULT_SNAPSHOT_LOG_SIZE_THRESHOLD_BYTES),
                new BrokerConfig.Consensus(BrokerConfig.DEFAULT_ELECTION_TIMEOUT),
                BrokerConfig.defaultProcessing()));

    final ClusterTopology topology =
        Broker.bootstrapTopology(config, new BrokerClusterConfig("demo", 1, 3, 5, 3));

    assertEquals(Duration.ofSeconds(30), config.registration().renewalTimeout());
    assertEquals(Duration.ofSeconds(5), config.consensus().electionTimeout());
    assertEquals(5, topology.partitions().size());
    for (final Partition partition : topology.partitions()) {
      assertEquals("broker-1", partition.leaderId());
      assertEquals(List.of("broker-1"), partition.voters());
      assertTrue(partition.learners().isEmpty());
    }
  }

  @Test
  void newBrokerPreservesPersistedVotersWithoutInventingAssignment() {
    final BrokerConfig config =
        config("broker-1", Path.of("target/test-broker-persisted-bootstrap"), 26502);
    final ClusterTopology persisted =
        new ClusterTopology(
            "demo",
            7,
            1,
            3,
            List.of(
                new Partition(
                    1,
                    new Leadership("broker-0", 4),
                    new Membership(List.of("broker-0"), List.of(), Map.of(), 0),
                    PartitionStatus.ACTIVE)),
            Map.of("broker-0", new BrokerEndpoint("broker-0", "127.0.0.1", 26501, 26601)),
            Instant.EPOCH);

    final ClusterTopology bootstrap =
        BrokerBootstrapTopology.bootstrapTopology(
            config, new BrokerClusterConfig("demo", 1, 3, 1, 3), persisted);

    assertEquals(List.of("broker-0"), bootstrap.partitions().getFirst().voters());
    assertTrue(bootstrap.partitions().getFirst().learners().isEmpty());
    assertFalse(bootstrap.partitions().getFirst().priorities().containsKey("broker-1"));
  }

  @Test
  void registrationAloneDoesNotAuthorizeLearnerBootstrap() {
    final BrokerConfig config =
        config("broker-1", Path.of("target/test-broker-unassigned-bootstrap"), 26502);
    final ClusterTopology persisted =
        new ClusterTopology(
            "demo",
            7,
            1,
            1,
            List.of(
                new Partition(
                    1,
                    new Leadership("broker-0", 4),
                    new Membership(List.of("broker-0"), List.of(), Map.of(), 0),
                    PartitionStatus.ACTIVE)),
            Map.of(
                "broker-0", new BrokerEndpoint("broker-0", "127.0.0.1", 26501, 26601),
                "broker-1", new BrokerEndpoint("broker-1", "127.0.0.1", 26502, 26602)),
            Instant.EPOCH);

    final ClusterTopology bootstrap =
        BrokerBootstrapTopology.bootstrapTopology(
            config, new BrokerClusterConfig("demo", 1, 2, 1, 1), persisted);

    assertTrue(bootstrap.partitions().getFirst().learners().isEmpty());
  }

  @Test
  void missingPartitionDoesNotCreateLearnerOnlyMembership() {
    final BrokerConfig config = config("broker-1", tempDir, 26502);
    final ClusterTopology persisted =
        new ClusterTopology(
            "demo",
            7,
            2,
            3,
            List.of(
                new Partition(
                    1,
                    new Leadership("broker-0", 4),
                    new Membership(List.of(), List.of(), Map.of(), 0),
                    PartitionStatus.ACTIVE)),
            Map.of("broker-0", new BrokerEndpoint("broker-0", "127.0.0.1", 26501, 26601)),
            Instant.EPOCH);

    final ClusterTopology bootstrap =
        BrokerBootstrapTopology.bootstrapTopology(
            config, new BrokerClusterConfig("demo", 1, 3, 2, 3), persisted);

    assertEquals(1, bootstrap.partitions().size());
    assertEquals(List.of("broker-0"), bootstrap.partitions().getFirst().voters());
    assertTrue(bootstrap.partitions().getFirst().learners().isEmpty());
  }

  @ParameterizedTest
  @EnumSource(IncompleteTopology.class)
  void brokerWaitsForCompleteTopologyBeforeStartingPartitions(final IncompleteTopology incomplete)
      throws Exception {
    final IncompleteTopologyPlacementDriver placementDriver =
        new IncompleteTopologyPlacementDriver(incomplete);
    final Server server = start(placementDriver);
    final BrokerConfig config =
        config("broker-1", tempDir.resolve(incomplete.name()), 0, endpoint(server), 100, 30_000);
    final Broker broker = new Broker(config);
    try {
      final var startup = broker.start();
      assertTrue(placementDriver.incompleteRequested.await(3, TimeUnit.SECONDS));
      assertTrue(placementDriver.initializationRenewed.await(3, TimeUnit.SECONDS));
      assertFalse(startup.isDone());
      assertFalse(isListening(config.network().raftPort()));
      placementDriver.ready = true;

      startup.get(10, TimeUnit.SECONDS);
      assertTrue(placementDriver.awaitPartitionRenewal());
      final BrokerPartitionState partition = placementDriver.renewedPartitions().getFirst();
      assertEquals("LEARNER", partition.getRole());
      assertEquals(0, partition.getPriority());
      assertFalse(partition.getReadyForPromotion());
    } finally {
      broker.stop().join();
      close(server);
    }
  }

  @Test
  void registeredBrokerStartsLearnerOnlyAfterPlacementDriverAssignment() throws Exception {
    final TopologyPlacementDriver placementDriver = TopologyPlacementDriver.knownUnassignedBroker();
    final Server server = start(placementDriver);
    final Broker broker =
        new Broker(
            config(
                "broker-1",
                tempDir.resolve("known-unassigned-broker"),
                0,
                endpoint(server),
                1000,
                30_000));
    try {
      broker.start().join();
      assertTrue(placementDriver.awaitRegistration());
      assertFalse(placementDriver.awaitPartitionRenewal());
      assertTrue(placementDriver.renewedPartitions().isEmpty());
      placementDriver.authorizeLearner();
      assertTrue(placementDriver.awaitPartitionRenewal());
      assertEquals("LEARNER", placementDriver.renewedPartitions().getFirst().getRole());
    } finally {
      broker.stop().join();
      close(server);
    }
  }

  @Test
  void joiningBrokerReportsLearnerFromAuthorizedPlacementDriverTopology() throws Exception {
    final TopologyPlacementDriver placementDriver =
        TopologyPlacementDriver.joiningBrokerWithStaleTopology();
    final Server server = start(placementDriver);
    final Broker broker =
        new Broker(
            config(
                "broker-3",
                tempDir.resolve("joining-broker-stale-topology"),
                0,
                endpoint(server),
                1000,
                30_000));
    try {
      broker.start().join();
      assertTrue(placementDriver.awaitPartitionRenewal());
      final BrokerPartitionState partition = placementDriver.renewedPartitions().getFirst();
      assertEquals("LEARNER", partition.getRole());
      assertEquals("CATCHING_UP", partition.getLifecycleState());
      assertFalse(partition.getReadyForPromotion());
      assertEquals(0, partition.getPriority());
    } finally {
      broker.stop().join();
      close(server);
    }
  }

  @Test
  void brokerFailsWhenPlacementDriverDoesNotKnowCluster() throws Exception {
    final Server server = start(new UnknownClusterPlacementDriver());
    final Broker broker =
        new Broker(
            config(
                "broker-0", tempDir.resolve("unknown-cluster"), 0, endpoint(server), 1000, 30_000));
    try {
      assertThrows(Exception.class, () -> broker.start().join());
    } finally {
      broker.stop().join();
      close(server);
    }
  }

  @Test
  void brokerFailsWhenBrokerIdIsAlreadyActive() throws Exception {
    final DuplicateBrokerPlacementDriver placementDriver = new DuplicateBrokerPlacementDriver();
    final Server server = start(placementDriver);
    final Broker broker =
        new Broker(
            config(
                "broker-0",
                tempDir.resolve("duplicate-broker-id"),
                0,
                endpoint(server),
                1000,
                30_000));
    try {
      final Exception error = assertThrows(Exception.class, () -> broker.start().join());
      assertTrue(hasMessage(error, "already active in cluster 'demo'"));
      assertEquals(1, placementDriver.registrationAttempts());
      assertEquals(0, placementDriver.clusterConfigRequests());
    } finally {
      broker.stop().join();
      close(server);
    }
  }

  @Test
  void brokerRegistersAndRefreshesPlacementDriverTopologyAfterStart() throws Exception {
    final TopologyPlacementDriver placementDriver = new TopologyPlacementDriver();
    final Server server = start(placementDriver);
    final Broker broker =
        new Broker(
            config(
                "broker-0",
                tempDir.resolve("broker-with-topology"),
                0,
                endpoint(server),
                1000,
                45_000));
    try {
      broker.start().join();
      assertTrue(placementDriver.awaitRegistration());
      assertEquals(45_000, placementDriver.registration().getRenewalTimeoutMillis());
      assertEquals(broker.raftPort(), placementDriver.registration().getRaftPort());
      assertTrue(placementDriver.awaitTopologyRequest());
      assertTrue(placementDriver.awaitPartitionRenewal());
      assertEquals(1, placementDriver.registrationCount());
      assertTrue(broker.awaitPartitionLeadership(Duration.ofSeconds(5)));
    } finally {
      broker.stop().join();
      close(server);
    }
  }

  @Test
  void brokerCoalescesPartitionReportsWhileRenewalIsInFlight() throws Exception {
    final DelayedRenewalPlacementDriver placementDriver = new DelayedRenewalPlacementDriver();
    final Server server = start(placementDriver);
    final Broker broker =
        new Broker(
            config(
                "broker-0",
                tempDir.resolve("coalesced-partition-reports"),
                0,
                endpoint(server),
                60_000,
                120_000));
    try {
      broker.start().join();
      assertTrue(placementDriver.awaitRenewal());

      placementDriver.publishTopologyBurst(100);
      Thread.sleep(300);

      assertEquals(1, placementDriver.renewalCount());
      assertEquals(1, placementDriver.maxConcurrentRenewals());

      placementDriver.completeNextRenewal();
      await(() -> placementDriver.renewalCount() == 2, Duration.ofSeconds(3));
      assertEquals(1, placementDriver.maxConcurrentRenewals());
    } finally {
      placementDriver.releaseRenewals();
      broker.stop().join();
      close(server);
    }
  }

  @Test
  void brokerRegistersThroughLeaderHintFromFollower() throws Exception {
    final LeaderPlacementDriver leader = new LeaderPlacementDriver();
    final Server leaderServer = start(leader);
    final FollowerPlacementDriver follower =
        new FollowerPlacementDriver(() -> endpoint(leaderServer));
    final Server followerServer = start(follower);
    final Broker broker =
        new Broker(
            config(
                "broker-0",
                tempDir.resolve("broker-follower-pd"),
                0,
                endpoint(followerServer),
                1000,
                30_000));
    try {
      broker.start().join();
      assertTrue(leader.awaitClusterConfigRequest());
      assertTrue(leader.awaitBrokerRegistration());
      assertNotNull(leader.registration());
      assertEquals("broker-0", leader.registration().getBrokerId());
    } finally {
      broker.stop().join();
      close(followerServer);
      close(leaderServer);
    }
  }

  @Test
  void brokerRetriesClusterConfigWhenPlacementDriverIsTemporarilyUnavailable() throws Exception {
    final TransientUnavailablePlacementDriver placementDriver =
        new TransientUnavailablePlacementDriver();
    final Server server = start(placementDriver);
    final Broker broker =
        new Broker(
            config(
                "broker-1",
                tempDir.resolve("broker-pd-unavailable"),
                0,
                endpoint(server),
                1000,
                30_000));
    try {
      broker.start().join();
      assertTrue(placementDriver.awaitClusterConfigRetried());
      assertTrue(broker.port() > 0);
      assertTrue(isListening(broker.raftPort()));
    } finally {
      broker.stop().join();
      close(server);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "BROKER_REMOVAL_IN_PROGRESS",
        "BROKER_REMOVAL_IN_PROGRESS: Expected to register broker 'broker-0' in cluster 'demo', but a pending topology or member change is removing it",
        "BROKER_REMOVAL_FENCED",
        "BROKER_REMOVAL_FENCED: Expected to register broker 'broker-0' in cluster 'demo' with incarnation 'previous', but this incarnation is excluded from learner membership after removal"
      })
  void startupRetriesOnlyTemporaryRemoval(final String reason) throws Exception {
    final var attempts = new AtomicInteger();
    final TopologyPlacementDriver placementDriver =
        new TopologyPlacementDriver() {
          @Override
          public void registerBroker(
              final RegisterBrokerRequest request,
              final StreamObserver<RegisterBrokerResponse> responseObserver) {
            if (attempts.incrementAndGet() == 1) {
              responseObserver.onError(
                  Status.FAILED_PRECONDITION.withDescription(reason).asRuntimeException());
            } else {
              super.registerBroker(request, responseObserver);
            }
          }
        };
    final var server = start(placementDriver);
    final var broker =
        new Broker(
            config("broker-0", tempDir.resolve("removal"), 0, endpoint(server), 1000, 30_000));
    try {
      if (reason.equals("BROKER_REMOVAL_IN_PROGRESS")
          || reason.startsWith("BROKER_REMOVAL_IN_PROGRESS:")) {
        broker.start().join();
        assertEquals(2, attempts.get());
        assertTrue(placementDriver.awaitRegistration());
      } else {
        final var failure = assertThrows(RuntimeException.class, () -> broker.start().join());
        assertTrue(failure.toString().contains("BROKER_REMOVAL_FENCED"));
        assertFalse(failure.toString().contains("within 300000 ms"));
        assertEquals(1, attempts.get());
      }
    } finally {
      broker.stop().join();
      close(server);
    }
  }

  @Test
  void removedBrokerStopsBusinessAndRaftServices() throws Exception {
    final RemovalPlacementDriver placementDriver = new RemovalPlacementDriver();
    final Server server = start(placementDriver);
    final Broker broker =
        new Broker(
            config(
                "broker-1", tempDir.resolve("removed-broker"), 0, endpoint(server), 100, 30_000));
    try {
      broker.start().join();
      assertTrue(placementDriver.awaitRegistration());
      final int apiPort = broker.port();
      final int raftPort = broker.raftPort();
      placementDriver.removeBroker();
      await(() -> !isListening(apiPort) && !isListening(raftPort), Duration.ofSeconds(10));
      assertEquals(0, placementDriver.deregistrationCount());
    } finally {
      broker.stop().join();
      close(server);
    }
  }

  @Test
  void brokerStaysRunningWhenTopologyOmissionIsNotACommittedRemoval() throws Exception {
    final RemovalPlacementDriver placementDriver = new RemovalPlacementDriver();
    final Server server = start(placementDriver);
    final Broker broker =
        new Broker(
            config(
                "broker-1",
                tempDir.resolve("temporarily-absent-broker"),
                0,
                endpoint(server),
                100,
                30_000));
    try {
      broker.start().join();
      assertTrue(placementDriver.awaitRegistration());
      placementDriver.hideBrokerWithoutRemoval();
      Thread.sleep(500);
      assertTrue(isListening(broker.port()));
    } finally {
      broker.stop().join();
      close(server);
    }
  }

  private static BrokerConfig config(final String brokerId, final Path dataDir, final int port) {
    return config(brokerId, dataDir, port, "127.0.0.1:29001", 1000, 30_000);
  }

  private static BrokerConfig config(
      final String brokerId,
      final Path dataDir,
      final int port,
      final String placementDrivers,
      final long renewInterval,
      final long renewalTimeout) {
    final int apiPort = port > 0 ? port : freePort();
    int raftPort;
    do {
      raftPort = freePort();
    } while (raftPort == apiPort);
    return new BrokerConfig(
        new BrokerConfig.Identity("demo", brokerId, "test"),
        new BrokerConfig.Node(new BrokerConfig.Network("127.0.0.1", apiPort, raftPort), dataDir),
        new BrokerConfig.Registration(
            placementDrivers,
            Duration.ofMillis(renewInterval),
            Duration.ofMillis(renewalTimeout),
            Duration.ofSeconds(1)),
        new BrokerConfig.Engine(
            new BrokerConfig.Snapshot(
                BrokerConfig.DEFAULT_SNAPSHOT_INTERVAL,
                BrokerConfig.DEFAULT_SNAPSHOT_LOG_SIZE_THRESHOLD_BYTES),
            new BrokerConfig.Consensus(BrokerConfig.DEFAULT_ELECTION_TIMEOUT),
            BrokerConfig.defaultProcessing()));
  }

  private static int freePort() {
    try (final ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (final IOException error) {
      throw new IllegalStateException("failed to allocate test port", error);
    }
  }

  private static Server start(final PlacementDriverGrpc.PlacementDriverImplBase service)
      throws IOException {
    return NettyServerBuilder.forPort(0).addService(service).build().start();
  }

  private static String endpoint(final Server server) {
    return "127.0.0.1:" + server.getPort();
  }

  private static void close(final Server server) throws InterruptedException {
    server.shutdown();
    if (!server.awaitTermination(1, TimeUnit.SECONDS)) {
      server.shutdownNow();
      server.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private static void await(final BooleanSupplier condition, final Duration timeout)
      throws InterruptedException {
    final long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(50);
    }
    assertTrue(condition.getAsBoolean(), "condition did not converge within " + timeout);
  }

  private static boolean isListening(final int port) {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("127.0.0.1", port), 100);
      return true;
    } catch (final IOException ignored) {
      return false;
    }
  }

  private static boolean hasMessage(final Throwable error, final String expected) {
    Throwable current = error;
    while (current != null) {
      if (current.getMessage() != null && current.getMessage().contains(expected)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private abstract static class TestPlacementDriver
      extends PlacementDriverGrpc.PlacementDriverImplBase {
    @Override
    public void registerBroker(
        final RegisterBrokerRequest request,
        final StreamObserver<RegisterBrokerResponse> responseObserver) {
      responseObserver.onNext(
          RegisterBrokerResponse.newBuilder()
              .setClusterId(request.getClusterId())
              .setBrokerId(request.getBrokerId())
              .build());
      responseObserver.onCompleted();
    }

    @Override
    public void renewBroker(
        final RenewBrokerRequest request,
        final StreamObserver<RenewBrokerResponse> responseObserver) {
      responseObserver.onNext(
          RenewBrokerResponse.newBuilder()
              .setClusterId(request.getClusterId())
              .setBrokerId(request.getBrokerId())
              .build());
      responseObserver.onCompleted();
    }

    @Override
    public void deregisterBroker(
        final DeregisterBrokerRequest request,
        final StreamObserver<DeregisterBrokerResponse> responseObserver) {
      responseObserver.onNext(
          DeregisterBrokerResponse.newBuilder()
              .setClusterId(request.getClusterId())
              .setBrokerId(request.getBrokerId())
              .build());
      responseObserver.onCompleted();
    }
  }

  private static class TopologyPlacementDriver extends TestPlacementDriver {
    private final AtomicReference<RegisterBrokerRequest> registration = new AtomicReference<>();
    private final AtomicReference<List<BrokerPartitionState>> renewedPartitions =
        new AtomicReference<>(List.of());
    private final CountDownLatch brokerRegistered = new CountDownLatch(1);
    private final CountDownLatch partitionRenewed = new CountDownLatch(1);
    private final CountDownLatch topologyRequested = new CountDownLatch(1);
    private final AtomicInteger registrationCount = new AtomicInteger();
    private final String localBrokerId;
    private final int clusterSize;
    private final boolean assignLocalBroker;
    private final boolean exposeLocalBeforeRegistration;

    private TopologyPlacementDriver() {
      this("broker-0", 1, true, true);
    }

    private TopologyPlacementDriver(
        final String localBrokerId,
        final int clusterSize,
        final boolean assignLocalBroker,
        final boolean exposeLocalBeforeRegistration) {
      this.localBrokerId = localBrokerId;
      this.clusterSize = clusterSize;
      this.assignLocalBroker = assignLocalBroker;
      this.exposeLocalBeforeRegistration = exposeLocalBeforeRegistration;
    }

    private static TopologyPlacementDriver knownUnassignedBroker() {
      final var driver = new TopologyPlacementDriver("broker-1", 2, false, true);
      driver.authorizedLearner = false;
      return driver;
    }

    private static TopologyPlacementDriver joiningBrokerWithStaleTopology() {
      return new TopologyPlacementDriver("broker-3", 3, false, false);
    }

    protected boolean awaitRegistration() throws InterruptedException {
      return brokerRegistered.await(3, TimeUnit.SECONDS);
    }

    protected boolean awaitPartitionRenewal() throws InterruptedException {
      return partitionRenewed.await(3, TimeUnit.SECONDS);
    }

    private boolean awaitTopologyRequest() throws InterruptedException {
      return topologyRequested.await(3, TimeUnit.SECONDS);
    }

    protected RegisterBrokerRequest registration() {
      return registration.get();
    }

    protected List<BrokerPartitionState> renewedPartitions() {
      return renewedPartitions.get();
    }

    private int registrationCount() {
      return registrationCount.get();
    }

    @Override
    public void registerBroker(
        final RegisterBrokerRequest request,
        final StreamObserver<RegisterBrokerResponse> responseObserver) {
      registrationCount.incrementAndGet();
      registration.set(request);
      brokerRegistered.countDown();
      super.registerBroker(request, responseObserver);
    }

    @Override
    public void renewBroker(
        final RenewBrokerRequest request,
        final StreamObserver<RenewBrokerResponse> responseObserver) {
      renewedPartitions.set(request.getPartitionsList());
      if (!request.getPartitionsList().isEmpty()) {
        partitionRenewed.countDown();
      }
      super.renewBroker(request, responseObserver);
    }

    @Override
    public void getClusterConfig(
        final GetClusterConfigRequest request,
        final StreamObserver<GetClusterConfigResponse> responseObserver) {
      responseObserver.onNext(configResponse(clusterSize, 1, 1));
      responseObserver.onCompleted();
    }

    @Override
    public void getBrokerTopology(
        final GetBrokerTopologyRequest request,
        final StreamObserver<BrokerTopology> responseObserver) {
      topologyRequested.countDown();
      responseObserver.onNext(topology());
      responseObserver.onCompleted();
    }

    @Override
    public void watchBrokerTopology(
        final GetBrokerTopologyRequest request,
        final StreamObserver<BrokerTopology> responseObserver) {
      watcher.set(responseObserver);
      responseObserver.onNext(topology());
    }

    private volatile boolean authorizedLearner = true;
    private final AtomicReference<StreamObserver<BrokerTopology>> watcher = new AtomicReference<>();

    private void authorizeLearner() {
      authorizedLearner = true;
      watcher.get().onNext(topology());
    }

    private BrokerTopology topology() {
      final RegisterBrokerRequest current = registration.get();
      final int port = current == null ? 0 : current.getPort();
      final int raftPort = current == null ? 0 : current.getRaftPort();
      final BrokerTopology.Builder topology =
          baseTopology(clusterSize, 1, 1)
              .addBrokers(broker("broker-0", 26501, 30611))
              .addPartitions(
                  partition(
                      "broker-0",
                      assignLocalBroker ? List.of("broker-0") : List.of(),
                      !assignLocalBroker && authorizedLearner && current != null
                          ? List.of(localBrokerId)
                          : List.of()));
      if (!assignLocalBroker && (exposeLocalBeforeRegistration || current != null)) {
        topology.addBrokers(broker(localBrokerId, port, raftPort));
      }
      if (assignLocalBroker && !"broker-0".equals(localBrokerId)) {
        topology.addBrokers(broker(localBrokerId, port, raftPort));
      }
      return topology.build();
    }
  }

  private enum IncompleteTopology {
    EMPTY,
    LEARNERS_ONLY,
    MISSING_VOTER_ENDPOINT,
    MISSING_ONE_OF_TWO_VOTER_ENDPOINTS
  }

  private static final class IncompleteTopologyPlacementDriver extends TopologyPlacementDriver {
    private final IncompleteTopology incomplete;
    private final CountDownLatch incompleteRequested = new CountDownLatch(1);
    private final CountDownLatch initializationRenewed = new CountDownLatch(1);
    private volatile boolean ready;

    private IncompleteTopologyPlacementDriver(final IncompleteTopology incomplete) {
      super("broker-1", 3, false, true);
      this.incomplete = incomplete;
    }

    @Override
    public void renewBroker(
        final RenewBrokerRequest request,
        final StreamObserver<RenewBrokerResponse> responseObserver) {
      if (request.getPreservePartitionObservations() && request.getPartitionsCount() == 0) {
        initializationRenewed.countDown();
      }
      super.renewBroker(request, responseObserver);
    }

    @Override
    public void getBrokerTopology(
        final GetBrokerTopologyRequest request,
        final StreamObserver<BrokerTopology> responseObserver) {
      if (ready) {
        super.getBrokerTopology(request, responseObserver);
        return;
      }
      final BrokerTopology.Builder topology =
          baseTopology(3, 1, 1)
              .addBrokers(
                  broker("broker-1", registration().getPort(), registration().getRaftPort()));
      switch (incomplete) {
        case EMPTY -> {}
        case LEARNERS_ONLY -> topology.addPartitions(partition("", List.of(), List.of("broker-1")));
        case MISSING_VOTER_ENDPOINT ->
            topology.addPartitions(partition("", List.of("broker-0"), List.of("broker-1")));
        case MISSING_ONE_OF_TWO_VOTER_ENDPOINTS ->
            topology
                .addBrokers(broker("broker-0", 26501, 30611))
                .addPartitions(partition("", List.of("broker-0", "broker-2"), List.of("broker-1")));
      }
      responseObserver.onNext(topology.build());
      responseObserver.onCompleted();
      incompleteRequested.countDown();
    }
  }

  private static final class UnknownClusterPlacementDriver extends TestPlacementDriver {
    @Override
    public void getClusterConfig(
        final GetClusterConfigRequest request,
        final StreamObserver<GetClusterConfigResponse> responseObserver) {
      responseObserver.onError(
          Status.NOT_FOUND
              .withDescription("CLUSTER_NOT_REGISTERED: " + request.getClusterId())
              .asRuntimeException());
    }
  }

  private static final class DuplicateBrokerPlacementDriver extends TestPlacementDriver {
    private final AtomicInteger registrationAttempts = new AtomicInteger();
    private final AtomicInteger clusterConfigRequests = new AtomicInteger();

    @Override
    public void registerBroker(
        final RegisterBrokerRequest request,
        final StreamObserver<RegisterBrokerResponse> responseObserver) {
      registrationAttempts.incrementAndGet();
      responseObserver.onError(
          Status.ALREADY_EXISTS
              .withDescription(
                  "BROKER_ID_ALREADY_REGISTERED: clusterId="
                      + request.getClusterId()
                      + ", brokerId="
                      + request.getBrokerId()
                      + ", status=ONLINE")
              .asRuntimeException());
    }

    @Override
    public void getClusterConfig(
        final GetClusterConfigRequest request,
        final StreamObserver<GetClusterConfigResponse> responseObserver) {
      clusterConfigRequests.incrementAndGet();
      responseObserver.onNext(configResponse(1, 1, 1));
      responseObserver.onCompleted();
    }

    private int registrationAttempts() {
      return registrationAttempts.get();
    }

    private int clusterConfigRequests() {
      return clusterConfigRequests.get();
    }
  }

  private static final class LeaderPlacementDriver extends TopologyPlacementDriver {
    private final CountDownLatch clusterConfigRequested = new CountDownLatch(1);

    @Override
    public void getClusterConfig(
        final GetClusterConfigRequest request,
        final StreamObserver<GetClusterConfigResponse> responseObserver) {
      clusterConfigRequested.countDown();
      super.getClusterConfig(request, responseObserver);
    }

    private boolean awaitClusterConfigRequest() throws InterruptedException {
      return clusterConfigRequested.await(3, TimeUnit.SECONDS);
    }

    private boolean awaitBrokerRegistration() throws InterruptedException {
      return awaitRegistration();
    }
  }

  private static final class FollowerPlacementDriver extends TestPlacementDriver {
    private final Supplier<String> leaderEndpoint;

    private FollowerPlacementDriver(final Supplier<String> leaderEndpoint) {
      this.leaderEndpoint = leaderEndpoint;
    }

    @Override
    public void getClusterConfig(
        final GetClusterConfigRequest request,
        final StreamObserver<GetClusterConfigResponse> responseObserver) {
      notLeader(responseObserver);
    }

    @Override
    public void registerBroker(
        final RegisterBrokerRequest request,
        final StreamObserver<RegisterBrokerResponse> responseObserver) {
      notLeader(responseObserver);
    }

    @Override
    public void watchPlacementDriverTopology(
        final WatchPlacementDriverTopologyRequest request,
        final StreamObserver<PlacementDriverTopology> responseObserver) {
      responseObserver.onNext(
          PlacementDriverTopology.newBuilder()
              .setLocalMemberId("pd-follower")
              .setLeaderId("pd-leader")
              .setLeaderEndpoint(leaderEndpoint.get())
              .addMembers(
                  PlacementDriverMember.newBuilder()
                      .setMemberId("pd-leader")
                      .setEndpoint(leaderEndpoint.get())
                      .setRole("LEADER"))
              .build());
    }

    private void notLeader(final StreamObserver<?> responseObserver) {
      responseObserver.onError(
          Status.FAILED_PRECONDITION
              .withDescription("NOT_LEADER:" + leaderEndpoint.get())
              .asRuntimeException());
    }
  }

  private static final class TransientUnavailablePlacementDriver extends TestPlacementDriver {
    private final AtomicInteger attempts = new AtomicInteger();
    private final CountDownLatch retried = new CountDownLatch(1);

    @Override
    public void getClusterConfig(
        final GetClusterConfigRequest request,
        final StreamObserver<GetClusterConfigResponse> responseObserver) {
      final int attempt = attempts.incrementAndGet();
      if (attempt == 1) {
        responseObserver.onError(Status.DEADLINE_EXCEEDED.asRuntimeException());
        return;
      }
      if (attempt == 2) {
        responseObserver.onError(
            Status.FAILED_PRECONDITION.withDescription("NOT_LEADER:").asRuntimeException());
        return;
      }
      retried.countDown();
      responseObserver.onNext(configResponse(3, 5, 3));
      responseObserver.onCompleted();
    }

    @Override
    public void getBrokerTopology(
        final GetBrokerTopologyRequest request,
        final StreamObserver<BrokerTopology> responseObserver) {
      responseObserver.onNext(
          baseTopology(3, 5, 3)
              .addBrokers(broker("broker-1", 26502, 30622))
              .addAllPartitions(
                  IntStream.rangeClosed(1, 5)
                      .mapToObj(
                          id ->
                              newBuilder()
                                  .setPartitionId(id)
                                  .addReplicas("broker-1")
                                  .setStatus("ACTIVE")
                                  .putPriorities("broker-1", 100)
                                  .build())
                      .toList())
              .build());
      responseObserver.onCompleted();
    }

    private boolean awaitClusterConfigRetried() throws InterruptedException {
      return retried.await(5, TimeUnit.SECONDS);
    }
  }

  private static final class DelayedRenewalPlacementDriver extends TestPlacementDriver {
    private final AtomicReference<RegisterBrokerRequest> registration = new AtomicReference<>();
    private final AtomicReference<StreamObserver<BrokerTopology>> watcher = new AtomicReference<>();
    private final Queue<StreamObserver<RenewBrokerResponse>> pendingRenewals =
        new ConcurrentLinkedQueue<>();
    private final AtomicInteger renewalCount = new AtomicInteger();
    private final AtomicInteger activeRenewals = new AtomicInteger();
    private final AtomicInteger maxConcurrentRenewals = new AtomicInteger();
    private final CountDownLatch firstRenewal = new CountDownLatch(1);
    private boolean holdRenewals = true;

    @Override
    public void registerBroker(
        final RegisterBrokerRequest request,
        final StreamObserver<RegisterBrokerResponse> responseObserver) {
      registration.set(request);
      super.registerBroker(request, responseObserver);
    }

    @Override
    public void getClusterConfig(
        final GetClusterConfigRequest request,
        final StreamObserver<GetClusterConfigResponse> responseObserver) {
      responseObserver.onNext(configResponse(1, 1, 1));
      responseObserver.onCompleted();
    }

    @Override
    public void getBrokerTopology(
        final GetBrokerTopologyRequest request,
        final StreamObserver<BrokerTopology> responseObserver) {
      responseObserver.onNext(topology());
      responseObserver.onCompleted();
    }

    @Override
    public void watchBrokerTopology(
        final GetBrokerTopologyRequest request,
        final StreamObserver<BrokerTopology> responseObserver) {
      watcher.set(responseObserver);
      responseObserver.onNext(topology());
    }

    @Override
    public synchronized void renewBroker(
        final RenewBrokerRequest request,
        final StreamObserver<RenewBrokerResponse> responseObserver) {
      renewalCount.incrementAndGet();
      final int active = activeRenewals.incrementAndGet();
      maxConcurrentRenewals.accumulateAndGet(active, Math::max);
      firstRenewal.countDown();
      if (holdRenewals) {
        pendingRenewals.add(responseObserver);
      } else {
        completeRenewal(responseObserver);
      }
    }

    private boolean awaitRenewal() throws InterruptedException {
      return firstRenewal.await(3, TimeUnit.SECONDS);
    }

    private void publishTopologyBurst(final int count) {
      final StreamObserver<BrokerTopology> observer = watcher.get();
      assertNotNull(observer);
      for (int i = 0; i < count; i++) {
        observer.onNext(topology());
      }
    }

    private synchronized void completeNextRenewal() {
      final StreamObserver<RenewBrokerResponse> observer = pendingRenewals.poll();
      assertNotNull(observer);
      completeRenewal(observer);
    }

    private synchronized void releaseRenewals() {
      holdRenewals = false;
      StreamObserver<RenewBrokerResponse> observer;
      while ((observer = pendingRenewals.poll()) != null) {
        completeRenewal(observer);
      }
    }

    private void completeRenewal(final StreamObserver<RenewBrokerResponse> observer) {
      activeRenewals.decrementAndGet();
      observer.onNext(
          RenewBrokerResponse.newBuilder().setClusterId("demo").setBrokerId("broker-0").build());
      observer.onCompleted();
    }

    private int renewalCount() {
      return renewalCount.get();
    }

    private int maxConcurrentRenewals() {
      return maxConcurrentRenewals.get();
    }

    private BrokerTopology topology() {
      final RegisterBrokerRequest current = registration.get();
      final int port = current == null ? 26501 : current.getPort();
      final int raftPort = current == null ? 26502 : current.getRaftPort();
      return baseTopology(1, 1, 1)
          .addBrokers(broker("broker-0", port, raftPort))
          .addPartitions(partition("broker-0", List.of(), List.of()))
          .build();
    }
  }

  private static final class RemovalPlacementDriver extends TestPlacementDriver {
    private final CountDownLatch registered = new CountDownLatch(1);
    private final AtomicReference<RegisterBrokerRequest> registration = new AtomicReference<>();
    private final AtomicReference<StreamObserver<BrokerTopology>> watcher = new AtomicReference<>();
    private final AtomicInteger deregistrationCount = new AtomicInteger();
    private volatile boolean brokerVisible = true;
    private volatile boolean removed;

    @Override
    public void registerBroker(
        final RegisterBrokerRequest request,
        final StreamObserver<RegisterBrokerResponse> responseObserver) {
      registration.set(request);
      registered.countDown();
      super.registerBroker(request, responseObserver);
    }

    @Override
    public void deregisterBroker(
        final DeregisterBrokerRequest request,
        final StreamObserver<DeregisterBrokerResponse> responseObserver) {
      deregistrationCount.incrementAndGet();
      super.deregisterBroker(request, responseObserver);
    }

    @Override
    public void getClusterConfig(
        final GetClusterConfigRequest request,
        final StreamObserver<GetClusterConfigResponse> responseObserver) {
      final GetClusterConfigResponse.Builder response = configResponse(2, 1, 1).toBuilder();
      if (removed) {
        response.addRemovedBrokers("broker-1");
      }
      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    }

    @Override
    public void getBrokerTopology(
        final GetBrokerTopologyRequest request,
        final StreamObserver<BrokerTopology> responseObserver) {
      responseObserver.onNext(topology());
      responseObserver.onCompleted();
    }

    @Override
    public void watchBrokerTopology(
        final GetBrokerTopologyRequest request,
        final StreamObserver<BrokerTopology> responseObserver) {
      watcher.set(responseObserver);
      responseObserver.onNext(topology());
    }

    private void removeBroker() {
      removed = true;
      brokerVisible = false;
      publish();
    }

    private void hideBrokerWithoutRemoval() {
      brokerVisible = false;
      publish();
    }

    private void publish() {
      final StreamObserver<BrokerTopology> observer = watcher.get();
      if (observer != null) {
        observer.onNext(topology());
      }
    }

    private BrokerTopology topology() {
      final BrokerTopology.Builder topology =
          baseTopology(2, 1, 1)
              .addBrokers(broker("broker-0", 26501, 30611))
              .addPartitions(partition("broker-0", List.of(), List.of()));
      final RegisterBrokerRequest current = registration.get();
      if (brokerVisible && current != null) {
        topology.addBrokers(broker("broker-1", current.getPort(), current.getRaftPort()));
      }
      return topology.build();
    }

    private boolean awaitRegistration() throws InterruptedException {
      return registered.await(3, TimeUnit.SECONDS);
    }

    private int deregistrationCount() {
      return deregistrationCount.get();
    }
  }

  private static GetClusterConfigResponse configResponse(
      final int clusterSize, final int partitionSize, final int replicationFactor) {
    return GetClusterConfigResponse.newBuilder()
        .setClusterId("demo")
        .setClusterKey(1)
        .setClusterSize(clusterSize)
        .setPartitionSize(partitionSize)
        .setReplicationFactor(replicationFactor)
        .build();
  }

  private static BrokerTopology.Builder baseTopology(
      final int clusterSize, final int partitionSize, final int replicationFactor) {
    return BrokerTopology.newBuilder()
        .setClusterId("demo")
        .setVersion(1)
        .setClusterSize(clusterSize)
        .setPartitionSize(partitionSize)
        .setReplicationFactor(replicationFactor)
        .setUpdatedAtEpochMillis(System.currentTimeMillis());
  }

  private static Builder partition(
      final String leaderId, final List<String> replicas, final List<String> learners) {
    return newBuilder()
        .setPartitionId(1)
        .setLeaderId(leaderId)
        .addAllReplicas(replicas)
        .addAllLearners(learners)
        .setTerm(1)
        .setStatus("ACTIVE");
  }
}
