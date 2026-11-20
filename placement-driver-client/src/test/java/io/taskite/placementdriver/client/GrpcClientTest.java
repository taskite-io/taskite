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
package io.taskite.placementdriver.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.BindableService;
import io.grpc.Context;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.taskite.placementdriver.client.request.PlacementDriverRequests;
import io.taskite.placementdriver.protocol.Broker;
import io.taskite.placementdriver.protocol.BrokerTopology;
import io.taskite.placementdriver.protocol.CreateTimerSubscriptionRequest;
import io.taskite.placementdriver.protocol.CreateTimerSubscriptionResponse;
import io.taskite.placementdriver.protocol.DeregisterBrokerRequest;
import io.taskite.placementdriver.protocol.DeregisterBrokerResponse;
import io.taskite.placementdriver.protocol.GetBrokerTopologyRequest;
import io.taskite.placementdriver.protocol.GetClusterConfigRequest;
import io.taskite.placementdriver.protocol.GetClusterConfigResponse;
import io.taskite.placementdriver.protocol.Partition;
import io.taskite.placementdriver.protocol.PlacementDriverGrpc;
import io.taskite.placementdriver.protocol.PlacementDriverMember;
import io.taskite.placementdriver.protocol.PlacementDriverTopology;
import io.taskite.placementdriver.protocol.RegisterBrokerRequest;
import io.taskite.placementdriver.protocol.RegisterBrokerResponse;
import io.taskite.placementdriver.protocol.RenewBrokerRequest;
import io.taskite.placementdriver.protocol.RenewBrokerResponse;
import io.taskite.placementdriver.protocol.WatchPlacementDriverTopologyRequest;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.ClusterTopology;
import io.taskite.topology.MemberRole;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

final class GrpcClientTest {
  @Test
  void timerCreationDoesNotRetryAnUncertainTransportResult() throws Exception {
    for (final Status status :
        List.of(Status.UNAVAILABLE, Status.UNKNOWN, Status.DEADLINE_EXCEEDED, Status.CANCELLED)) {
      final var calls = new AtomicInteger();
      final var otherCalls = new AtomicInteger();
      final Server first =
          start(
              new PlacementDriverGrpc.PlacementDriverImplBase() {
                @Override
                public void createTimerSubscription(
                    final CreateTimerSubscriptionRequest request,
                    final StreamObserver<CreateTimerSubscriptionResponse> observer) {
                  calls.incrementAndGet();
                  observer.onError(status.asRuntimeException());
                }
              });
      final Server other =
          start(
              new PlacementDriverGrpc.PlacementDriverImplBase() {
                @Override
                public void createTimerSubscription(
                    final CreateTimerSubscriptionRequest request,
                    final StreamObserver<CreateTimerSubscriptionResponse> observer) {
                  otherCalls.incrementAndGet();
                  observer.onNext(
                      CreateTimerSubscriptionResponse.newBuilder()
                          .setTimerSubscriptionKey(42)
                          .build());
                  observer.onCompleted();
                }
              });
      try (var client = GrpcClient.connect(endpoint(first) + "," + endpoint(other))) {
        final var error =
            assertThrows(
                ExecutionException.class,
                () ->
                    client
                        .createTimerSubscription(
                            CreateTimerSubscriptionRequest.newBuilder().build())
                        .get(3, TimeUnit.SECONDS));
        assertEquals(status.getCode(), Status.fromThrowable(error).getCode());
        assertEquals(1, calls.get());
        assertEquals(0, otherCalls.get());
      } finally {
        first.shutdownNow();
        other.shutdownNow();
      }
    }
  }

  @Test
  void timerCreationFollowsExplicitNotLeaderRejection() throws Exception {
    final var calls = new AtomicInteger();
    final Server leader =
        start(
            new PlacementDriverGrpc.PlacementDriverImplBase() {
              @Override
              public void createTimerSubscription(
                  final CreateTimerSubscriptionRequest request,
                  final StreamObserver<CreateTimerSubscriptionResponse> observer) {
                calls.incrementAndGet();
                observer.onNext(
                    CreateTimerSubscriptionResponse.newBuilder()
                        .setTimerSubscriptionKey(42)
                        .build());
                observer.onCompleted();
              }
            });
    final Server follower =
        start(
            new PlacementDriverGrpc.PlacementDriverImplBase() {
              @Override
              public void createTimerSubscription(
                  final CreateTimerSubscriptionRequest request,
                  final StreamObserver<CreateTimerSubscriptionResponse> observer) {
                observer.onError(
                    Status.FAILED_PRECONDITION
                        .withDescription("NOT_LEADER:" + endpoint(leader))
                        .asRuntimeException());
              }
            });
    try (var client = GrpcClient.connect(endpoint(follower))) {
      assertEquals(
          42,
          client
              .createTimerSubscription(CreateTimerSubscriptionRequest.getDefaultInstance())
              .get(3, TimeUnit.SECONDS)
              .getTimerSubscriptionKey());
      assertEquals(1, calls.get());
    } finally {
      follower.shutdownNow();
      leader.shutdownNow();
    }
  }

  @Test
  void configuredTimeoutAppliesToRegistrationRenewalAndDeregistration() throws Exception {
    final var timeout = Duration.ofMillis(500);
    final var calls = new AtomicInteger();
    final var reportedTimeout = new AtomicReference<Long>();
    final Server server =
        start(
            new PlacementDriverGrpc.PlacementDriverImplBase() {
              private void recordDeadline() {
                final var deadline = Context.current().getDeadline();
                assertNotNull(deadline);
                assertTrue(deadline.timeRemaining(TimeUnit.NANOSECONDS) <= timeout.toNanos());
                calls.incrementAndGet();
              }

              @Override
              public void registerBroker(
                  final RegisterBrokerRequest request,
                  final StreamObserver<RegisterBrokerResponse> observer) {
                recordDeadline();
                reportedTimeout.set(request.getRenewalTimeoutMillis());
              }

              @Override
              public void renewBroker(
                  final RenewBrokerRequest request,
                  final StreamObserver<RenewBrokerResponse> observer) {
                recordDeadline();
              }

              @Override
              public void deregisterBroker(
                  final DeregisterBrokerRequest request,
                  final StreamObserver<DeregisterBrokerResponse> observer) {
                recordDeadline();
              }
            });
    try (final var client = GrpcClient.connect(endpoint(server), null, timeout)) {
      final var registration =
          PlacementDriverRequests.registerBroker(
              "demo", "b0", "127.0.0.1", 26501, 26502, "test", timeout.toMillis(), List.of());
      for (final var request :
          List.of(
              client.registerBroker(registration),
              client.renewBroker(PlacementDriverRequests.renewBroker("demo", "b0", List.of())),
              client.deregisterBroker(PlacementDriverRequests.deregisterBroker("demo", "b0")))) {
        final var error =
            assertThrows(ExecutionException.class, () -> request.get(3, TimeUnit.SECONDS));
        assertEquals(Status.Code.DEADLINE_EXCEEDED, Status.fromThrowable(error).getCode());
      }
      assertEquals(3, calls.get());
      assertEquals(timeout.toMillis(), reportedTimeout.get());
    } finally {
      server.shutdownNow();
    }
  }

  @Test
  void rejectsNonPositiveOrUnrepresentableRequestTimeoutBeforeConnecting() {
    for (final var timeout :
        List.of(Duration.ZERO, Duration.ofMillis(-1), Duration.ofSeconds(Long.MAX_VALUE))) {
      assertThrows(
          IllegalArgumentException.class,
          () -> GrpcClient.connect("127.0.0.1:26501", null, timeout));
    }
  }

  @Test
  void propagatesBackpressureWithoutRetryingOrTryingAnotherEndpoint() throws Exception {
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicInteger otherAttempts = new AtomicInteger();
    final Server overloaded =
        start(
            new PlacementDriverGrpc.PlacementDriverImplBase() {
              @Override
              public void registerBroker(
                  final RegisterBrokerRequest request,
                  final StreamObserver<RegisterBrokerResponse> observer) {
                if (attempts.incrementAndGet() == 1) {
                  observer.onError(
                      Status.RESOURCE_EXHAUSTED
                          .withDescription("BACKPRESSURE: REGISTRATION queue is full")
                          .asRuntimeException());
                } else {
                  observer.onNext(
                      RegisterBrokerResponse.newBuilder()
                          .setBrokerId(request.getBrokerId())
                          .build());
                  observer.onCompleted();
                }
              }
            });
    final Server other =
        start(
            new PlacementDriverGrpc.PlacementDriverImplBase() {
              @Override
              public void registerBroker(
                  final RegisterBrokerRequest request,
                  final StreamObserver<RegisterBrokerResponse> observer) {
                otherAttempts.incrementAndGet();
                observer.onNext(RegisterBrokerResponse.getDefaultInstance());
                observer.onCompleted();
              }
            });
    try (GrpcClient client = GrpcClient.connect(endpoint(overloaded) + "," + endpoint(other))) {
      final var request =
          RegisterBrokerRequest.newBuilder().setClusterId("demo").setBrokerId("broker-1").build();
      final var error =
          assertThrows(
              ExecutionException.class,
              () -> client.registerBroker(request).get(3, TimeUnit.SECONDS));
      assertEquals(Status.Code.RESOURCE_EXHAUSTED, Status.fromThrowable(error).getCode());
      assertEquals(1, attempts.get(), "pd-client must leave backpressure retries to the caller");
      assertEquals(0, otherAttempts.get());
      assertEquals(
          "broker-1", client.registerBroker(request).get(3, TimeUnit.SECONDS).getBrokerId());
      assertEquals(2, attempts.get());
      assertEquals(0, otherAttempts.get());
    } finally {
      overloaded.shutdownNow();
      other.shutdownNow();
    }
  }

  @Test
  void usesGeneratedRpcForQueriesAndRegistration() throws Exception {
    final RecordingService service = new RecordingService();
    final Server server = start(service);
    try (GrpcClient client = GrpcClient.connect(endpoint(server))) {
      final var config = client.clusterConfig("demo").join();
      final ClusterTopology topology = client.topology("demo").join();
      final RegisterBrokerResponse registration =
          client
              .registerBroker(
                  PlacementDriverRequests.registerBroker(
                      "demo",
                      "broker-0",
                      "127.0.0.1",
                      26501,
                      26601,
                      "1.0",
                      30_000,
                      List.of(new BrokerPartitionState(1, MemberRole.LEADER, 7))))
              .join();

      assertEquals("demo", config.clusterId());
      assertEquals(3, config.partitionSize());
      assertEquals("broker-0", topology.partition(1).orElseThrow().leaderId());
      assertEquals("broker-0", registration.getBrokerId());
      assertEquals("demo", service.clusterConfigRequest.get().getClusterId());
      assertEquals(1, service.registerBrokerRequest.get().getPartitionsCount());
    } finally {
      server.shutdownNow();
    }
  }

  @Test
  void followsNotLeaderHintWithGeneratedStub() throws Exception {
    final RecordingService leader = new RecordingService();
    final Server leaderServer = start(leader);
    final Server follower =
        start(
            new PlacementDriverGrpc.PlacementDriverImplBase() {
              @Override
              public void getClusterConfig(
                  final GetClusterConfigRequest request,
                  final StreamObserver<GetClusterConfigResponse> responseObserver) {
                responseObserver.onError(
                    Status.FAILED_PRECONDITION
                        .withDescription("NOT_LEADER:" + endpoint(leaderServer))
                        .asRuntimeException());
              }
            });
    try (GrpcClient client = GrpcClient.connect(endpoint(follower))) {
      assertEquals("demo", client.clusterConfig("demo").join().clusterId());
      assertNotNull(leader.clusterConfigRequest.get());
    } finally {
      follower.shutdownNow();
      leaderServer.shutdownNow();
    }
  }

  @Test
  void followsIpv6LeaderHintAndRegistersBroker() throws Exception {
    final RecordingService leader = new RecordingService();
    final Server leaderServer =
        NettyServerBuilder.forAddress(new InetSocketAddress("::1", 0))
            .addService(leader)
            .build()
            .start();
    leader.grpcPort = leaderServer.getPort();
    final Server follower =
        NettyServerBuilder.forAddress(new InetSocketAddress("::1", 0))
            .addService(
                new PlacementDriverGrpc.PlacementDriverImplBase() {
                  @Override
                  public void getClusterConfig(
                      final GetClusterConfigRequest request,
                      final StreamObserver<GetClusterConfigResponse> observer) {
                    observer.onError(
                        Status.FAILED_PRECONDITION
                            .withDescription("NOT_LEADER:[::1]:" + leaderServer.getPort())
                            .asRuntimeException());
                  }
                })
            .build()
            .start();
    try (GrpcClient client = GrpcClient.connect("[::1]:" + follower.getPort())) {
      assertEquals("demo", client.clusterConfig("demo").get(10, TimeUnit.SECONDS).clusterId());
      final var response =
          client
              .registerBroker(
                  PlacementDriverRequests.registerBroker(
                      "demo", "broker-0", "::1", 26501, 26502, "1.0", 30_000, List.of()))
              .get(10, TimeUnit.SECONDS);
      assertEquals("broker-0", response.getBrokerId());
      assertEquals("::1", leader.registerBrokerRequest.get().getHost());
    } finally {
      follower.shutdownNow();
      leaderServer.shutdownNow();
    }
  }

  @Test
  void mapsGeneratedBrokerTopologyStream() throws Exception {
    final RecordingService service = new RecordingService();
    final Server server = start(service);
    final CountDownLatch received = new CountDownLatch(1);
    final AtomicReference<ClusterTopology> observed = new AtomicReference<>();
    try (GrpcClient client = GrpcClient.connect(endpoint(server));
        AutoCloseable ignored =
            client.watchBrokerTopology(
                "demo",
                new TopologyChangeListener() {
                  @Override
                  public void onTopologyChanged(final ClusterTopology topology) {
                    observed.set(topology);
                    received.countDown();
                  }

                  @Override
                  public void onTopologyWatchFailed(final Throwable error) {}
                })) {
      assertTrue(received.await(5, TimeUnit.SECONDS));
      assertEquals(11, observed.get().version());
      assertEquals("broker-0", observed.get().partition(1).orElseThrow().leaderId());
    } finally {
      server.shutdownNow();
    }
  }

  @Test
  void learnsLeaderFromPlacementDriverTopologyStream() throws Exception {
    final RecordingService service = new RecordingService();
    final Server server = start(service);
    try (GrpcClient client = GrpcClient.connect(endpoint(server))) {
      final long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
      while (client.placementDriverTopology() == null && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
      assertNotNull(client.placementDriverTopology());
      assertEquals("pd-0", client.placementDriverTopology().getLeaderId());
    } finally {
      server.shutdownNow();
    }
  }

  private static Server start(final BindableService service) throws Exception {
    final Server server =
        NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
            .addService(service)
            .build()
            .start();
    if (service instanceof RecordingService recording) {
      recording.grpcPort = server.getPort();
    }
    return server;
  }

  private static String endpoint(final Server server) {
    return "127.0.0.1:" + server.getPort();
  }

  private static final class RecordingService extends PlacementDriverGrpc.PlacementDriverImplBase {
    private volatile int grpcPort;
    private final AtomicReference<GetClusterConfigRequest> clusterConfigRequest =
        new AtomicReference<>();
    private final AtomicReference<RegisterBrokerRequest> registerBrokerRequest =
        new AtomicReference<>();

    @Override
    public void getClusterConfig(
        final GetClusterConfigRequest request,
        final StreamObserver<GetClusterConfigResponse> responseObserver) {
      clusterConfigRequest.set(request);
      responseObserver.onNext(
          GetClusterConfigResponse.newBuilder()
              .setClusterId(request.getClusterId())
              .setClusterKey(1)
              .setClusterSize(3)
              .setPartitionSize(3)
              .setReplicationFactor(3)
              .build());
      responseObserver.onCompleted();
    }

    @Override
    public void getBrokerTopology(
        final GetBrokerTopologyRequest request,
        final StreamObserver<BrokerTopology> responseObserver) {
      responseObserver.onNext(topology(request.getClusterId()));
      responseObserver.onCompleted();
    }

    @Override
    public void watchBrokerTopology(
        final GetBrokerTopologyRequest request,
        final StreamObserver<BrokerTopology> responseObserver) {
      responseObserver.onNext(topology(request.getClusterId()));
    }

    @Override
    public void watchPlacementDriverTopology(
        final WatchPlacementDriverTopologyRequest request,
        final StreamObserver<PlacementDriverTopology> responseObserver) {
      responseObserver.onNext(
          PlacementDriverTopology.newBuilder()
              .setLocalMemberId("pd-0")
              .setLeaderId("pd-0")
              .setLeaderEndpoint("127.0.0.1:" + grpcPort)
              .addMembers(
                  PlacementDriverMember.newBuilder()
                      .setMemberId("pd-0")
                      .setRole("LEADER")
                      .setHost("127.0.0.1")
                      .setGrpcPort(grpcPort))
              .build());
    }

    @Override
    public void registerBroker(
        final RegisterBrokerRequest request,
        final StreamObserver<RegisterBrokerResponse> responseObserver) {
      registerBrokerRequest.set(request);
      responseObserver.onNext(
          RegisterBrokerResponse.newBuilder()
              .setClusterId(request.getClusterId())
              .setBrokerId(request.getBrokerId())
              .build());
      responseObserver.onCompleted();
    }

    private static BrokerTopology topology(final String clusterId) {
      return BrokerTopology.newBuilder()
          .setClusterId(clusterId)
          .setVersion(11)
          .setPartitionSize(1)
          .setReplicationFactor(1)
          .setUpdatedAtEpochMillis(1)
          .addBrokers(
              Broker.newBuilder()
                  .setBrokerId("broker-0")
                  .setHost("127.0.0.1")
                  .setPort(26501)
                  .setRaftPort(26601))
          .addPartitions(
              Partition.newBuilder()
                  .setPartitionId(1)
                  .setLeaderId("broker-0")
                  .addReplicas("broker-0")
                  .setStatus("ACTIVE"))
          .build();
    }
  }
}
