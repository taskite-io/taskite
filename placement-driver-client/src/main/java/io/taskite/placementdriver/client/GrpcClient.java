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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.taskite.placementdriver.client.mapper.PlacementDriverResponseMapper;
import io.taskite.placementdriver.client.response.ClusterConfig;
import io.taskite.placementdriver.client.topology.BrokerTopologyWatch;
import io.taskite.placementdriver.client.topology.PlacementDriverTopologyWatch;
import io.taskite.placementdriver.protocol.BrokerTopology;
import io.taskite.placementdriver.protocol.CreateTimerSubscriptionRequest;
import io.taskite.placementdriver.protocol.CreateTimerSubscriptionResponse;
import io.taskite.placementdriver.protocol.DeleteTimerSubscriptionRequest;
import io.taskite.placementdriver.protocol.DeleteTimerSubscriptionResponse;
import io.taskite.placementdriver.protocol.DeregisterBrokerRequest;
import io.taskite.placementdriver.protocol.DeregisterBrokerResponse;
import io.taskite.placementdriver.protocol.DeregisterWorkerRequest;
import io.taskite.placementdriver.protocol.DeregisterWorkerResponse;
import io.taskite.placementdriver.protocol.DistributeCommandRequest;
import io.taskite.placementdriver.protocol.DistributeCommandResponse;
import io.taskite.placementdriver.protocol.GetBrokerTopologyRequest;
import io.taskite.placementdriver.protocol.GetClusterConfigRequest;
import io.taskite.placementdriver.protocol.GetClusterConfigResponse;
import io.taskite.placementdriver.protocol.PlacementDriverGrpc;
import io.taskite.placementdriver.protocol.PlacementDriverTopology;
import io.taskite.placementdriver.protocol.RegisterBrokerRequest;
import io.taskite.placementdriver.protocol.RegisterBrokerResponse;
import io.taskite.placementdriver.protocol.RegisterWorkerRequest;
import io.taskite.placementdriver.protocol.RegisterWorkerResponse;
import io.taskite.placementdriver.protocol.RenewBrokerRequest;
import io.taskite.placementdriver.protocol.RenewBrokerResponse;
import io.taskite.placementdriver.protocol.RenewWorkerRequest;
import io.taskite.placementdriver.protocol.RenewWorkerResponse;
import io.taskite.scheduler.ActorScheduler;
import io.taskite.scheduler.ActorSchedulingService;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.topology.ClusterTopology;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public final class GrpcClient implements Client {
  private static final long WATCH_RECONNECT_DELAY_MILLIS = 1000L;
  private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(10);

  private final List<Endpoint> endpoints;
  private final long requestTimeoutNanos;
  private final List<ManagedChannel> channels;
  private final ConcurrentHashMap<Endpoint, ManagedChannel> channelsByEndpoint =
      new ConcurrentHashMap<>();
  private final Set<BrokerTopologyWatch> brokerTopologyWatches = ConcurrentHashMap.newKeySet();
  private final ActorSchedulingService schedulingService;
  private volatile Endpoint leaderHint;
  private volatile PlacementDriverTopology placementDriverTopology;
  private PlacementDriverTopologyWatch placementDriverTopologyWatch;
  private ActorScheduler ownedScheduler;

  public GrpcClient(final Collection<Endpoint> endpoints) {
    this(endpoints, null);
  }

  public GrpcClient(
      final Collection<Endpoint> endpoints, final ActorSchedulingService schedulingService) {
    this(endpoints, schedulingService, DEFAULT_REQUEST_TIMEOUT);
  }

  public GrpcClient(
      final Collection<Endpoint> endpoints,
      final ActorSchedulingService schedulingService,
      final Duration requestTimeout) {
    if (endpoints == null || endpoints.isEmpty()) {
      throw new IllegalArgumentException("at least one placement-driver endpoint is required");
    }
    Objects.requireNonNull(requestTimeout, "requestTimeout");
    if (requestTimeout.isNegative() || requestTimeout.isZero()) {
      throw new IllegalArgumentException("requestTimeout must be positive");
    }
    try {
      requestTimeoutNanos = requestTimeout.toNanos();
    } catch (final ArithmeticException error) {
      throw new IllegalArgumentException("requestTimeout is too large", error);
    }
    this.endpoints = List.copyOf(endpoints);
    final List<ManagedChannel> configuredChannels = new ArrayList<>();
    for (final Endpoint endpoint : this.endpoints) {
      final ManagedChannel channel = newChannel(endpoint);
      configuredChannels.add(channel);
      channelsByEndpoint.put(endpoint, channel);
    }
    channels = List.copyOf(configuredChannels);
    this.schedulingService = schedulingService;
    startPlacementDriverTopologyWatch();
  }

  public static GrpcClient connect(final String endpoints) {
    return connect(endpoints, null);
  }

  public static GrpcClient connect(
      final String endpoints, final ActorSchedulingService schedulingService) {
    return connect(endpoints, schedulingService, DEFAULT_REQUEST_TIMEOUT);
  }

  public static GrpcClient connect(
      final String endpoints,
      final ActorSchedulingService schedulingService,
      final Duration requestTimeout) {
    final List<Endpoint> parsed = new ArrayList<>();
    for (final String endpoint : endpoints.split(",")) {
      if (!endpoint.isBlank()) {
        parsed.add(Endpoint.parse(endpoint));
      }
    }
    return new GrpcClient(parsed, schedulingService, requestTimeout);
  }

  @Override
  public ActorFuture<ClusterConfig> clusterConfig(final GetClusterConfigRequest request) {
    Objects.requireNonNull(request, "request");
    return this.<GetClusterConfigResponse>send(
            (stub, observer) -> stub.getClusterConfig(request, observer))
        .thenApply(PlacementDriverResponseMapper::toClusterConfig);
  }

  @Override
  public ActorFuture<ClusterTopology> topology(final GetBrokerTopologyRequest request) {
    Objects.requireNonNull(request, "request");
    return this.<BrokerTopology>send((stub, observer) -> stub.getBrokerTopology(request, observer))
        .thenApply(PlacementDriverResponseMapper::toClusterTopology);
  }

  @Override
  public AutoCloseable watchBrokerTopology(
      final GetBrokerTopologyRequest request, final TopologyChangeListener listener) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(listener, "listener");
    final BrokerTopologyWatch watch =
        new BrokerTopologyWatch(
            channels, request, listener, WATCH_RECONNECT_DELAY_MILLIS, watchSchedulingService());
    brokerTopologyWatches.add(watch);
    watch.start();
    return () -> {
      watch.close();
      brokerTopologyWatches.remove(watch);
    };
  }

  @Override
  public ActorFuture<RegisterBrokerResponse> registerBroker(final RegisterBrokerRequest request) {
    Objects.requireNonNull(request, "request");
    return send((stub, observer) -> stub.registerBroker(request, observer));
  }

  @Override
  public ActorFuture<RenewBrokerResponse> renewBroker(final RenewBrokerRequest request) {
    Objects.requireNonNull(request, "request");
    return send((stub, observer) -> stub.renewBroker(request, observer));
  }

  @Override
  public ActorFuture<DeregisterBrokerResponse> deregisterBroker(
      final DeregisterBrokerRequest request) {
    Objects.requireNonNull(request, "request");
    return send((stub, observer) -> stub.deregisterBroker(request, observer));
  }

  @Override
  public ActorFuture<RegisterWorkerResponse> registerWorker(final RegisterWorkerRequest request) {
    Objects.requireNonNull(request, "request");
    return send((stub, observer) -> stub.registerWorker(request, observer));
  }

  @Override
  public ActorFuture<RenewWorkerResponse> renewWorker(final RenewWorkerRequest request) {
    Objects.requireNonNull(request, "request");
    return send((stub, observer) -> stub.renewWorker(request, observer));
  }

  @Override
  public ActorFuture<DeregisterWorkerResponse> deregisterWorker(
      final DeregisterWorkerRequest request) {
    Objects.requireNonNull(request, "request");
    return send((stub, observer) -> stub.deregisterWorker(request, observer));
  }

  /** Sends a typed request through the PD command-distribution RPC. */
  @Override
  public ActorFuture<DistributeCommandResponse> distributeCommand(
      final DistributeCommandRequest request) {
    Objects.requireNonNull(request, "request");
    return send((stub, observer) -> stub.distributeCommand(request, observer));
  }

  @Override
  public ActorFuture<CreateTimerSubscriptionResponse> createTimerSubscription(
      final CreateTimerSubscriptionRequest request) {
    Objects.requireNonNull(request, "request");
    return send((stub, observer) -> stub.createTimerSubscription(request, observer), false);
  }

  @Override
  public ActorFuture<DeleteTimerSubscriptionResponse> deleteTimerSubscription(
      final DeleteTimerSubscriptionRequest request) {
    Objects.requireNonNull(request, "request");
    return send((stub, observer) -> stub.deleteTimerSubscription(request, observer));
  }

  @Override
  public void close() {
    for (final BrokerTopologyWatch watch : brokerTopologyWatches) {
      watch.close();
    }
    brokerTopologyWatches.clear();
    if (placementDriverTopologyWatch != null) {
      placementDriverTopologyWatch.close();
      placementDriverTopologyWatch = null;
    }
    channelsByEndpoint.values().forEach(this::closeChannel);
    channelsByEndpoint.clear();
    if (ownedScheduler != null) {
      ownedScheduler.stop();
      ownedScheduler = null;
    }
  }

  PlacementDriverTopology placementDriverTopology() {
    return placementDriverTopology;
  }

  private void startPlacementDriverTopologyWatch() {
    placementDriverTopologyWatch =
        new PlacementDriverTopologyWatch(
            channels,
            this::onPlacementDriverTopologyChanged,
            WATCH_RECONNECT_DELAY_MILLIS,
            watchSchedulingService());
    placementDriverTopologyWatch.start();
  }

  private void onPlacementDriverTopologyChanged(final PlacementDriverTopology topology) {
    placementDriverTopology = topology;
    if (topology.getLeaderEndpoint().isBlank()) {
      leaderHint = null;
      return;
    }
    try {
      leaderHint = Endpoint.parse(topology.getLeaderEndpoint());
    } catch (final IllegalArgumentException ignored) {
      // Unary NOT_LEADER hints remain the fallback for malformed watch updates.
    }
  }

  private <T> ActorFuture<T> send(final UnaryCall<T> call) {
    return send(call, true);
  }

  private <T> ActorFuture<T> send(final UnaryCall<T> call, final boolean retryUncertainResult) {
    final ActorFuture<T> result = new CompletableActorFuture<>();
    sendAt(
        candidates(),
        0,
        call,
        result,
        null,
        System.nanoTime() + requestTimeoutNanos,
        retryUncertainResult);
    return result;
  }

  private <T> void sendAt(
      final List<Endpoint> candidates,
      final int index,
      final UnaryCall<T> call,
      final ActorFuture<T> result,
      final Throwable previousError,
      final long deadlineNanos,
      final boolean retryUncertainResult) {
    if (result.isDone()) {
      return;
    }
    final long remainingNanos = deadlineNanos - System.nanoTime();
    if (remainingNanos <= 0) {
      result.completeExceptionally(Status.DEADLINE_EXCEEDED.asRuntimeException());
      return;
    }
    if (index >= candidates.size()) {
      result.completeExceptionally(
          previousError == null
              ? new IllegalStateException("no placement-driver endpoint available")
              : previousError);
      return;
    }

    final Endpoint endpoint = candidates.get(index);
    final PlacementDriverGrpc.PlacementDriverStub stub =
        PlacementDriverGrpc.newStub(channel(endpoint))
            .withWaitForReady()
            .withDeadlineAfter(remainingNanos, TimeUnit.NANOSECONDS);
    call.invoke(
        stub,
        new StreamObserver<>() {
          private T response;

          @Override
          public void onNext(final T value) {
            response = value;
          }

          @Override
          public void onError(final Throwable error) {
            if (result.isDone()) {
              return;
            }
            final Status status = Status.fromThrowable(error);
            final boolean rejectedByFollower =
                status.getCode() == Status.Code.FAILED_PRECONDITION
                    && status.getDescription() != null
                    && status.getDescription().startsWith("NOT_LEADER:");
            if (!isRetryable(error) || (!retryUncertainResult && !rejectedByFollower)) {
              result.completeExceptionally(error);
              return;
            }
            final Endpoint hintedLeader = leaderEndpoint(error);
            if (endpoint.equals(leaderHint)
                && (hintedLeader == null || hintedLeader.equals(endpoint))) {
              leaderHint = null;
            }
            if (hintedLeader != null && !candidates.subList(0, index + 1).contains(hintedLeader)) {
              leaderHint = hintedLeader;
              candidates.remove(hintedLeader);
              candidates.add(index + 1, hintedLeader);
            }
            sendAt(candidates, index + 1, call, result, error, deadlineNanos, retryUncertainResult);
          }

          @Override
          public void onCompleted() {
            if (!result.isDone()) {
              leaderHint = endpoint;
              result.complete(response);
            }
          }
        });
  }

  private List<Endpoint> candidates() {
    final List<Endpoint> candidates = new ArrayList<>();
    if (leaderHint != null) {
      candidates.add(leaderHint);
    }
    for (final Endpoint endpoint : endpoints) {
      if (!candidates.contains(endpoint)) {
        candidates.add(endpoint);
      }
    }
    return candidates;
  }

  private static boolean isRetryable(final Throwable error) {
    final Status status = Status.fromThrowable(error);
    if (status.getCode() == Status.Code.FAILED_PRECONDITION) {
      return status.getDescription() != null && status.getDescription().startsWith("NOT_LEADER:");
    }
    return switch (status.getCode()) {
      case CANCELLED, UNKNOWN, DEADLINE_EXCEEDED, UNAVAILABLE -> true;
      default -> false;
    };
  }

  private static Endpoint leaderEndpoint(final Throwable error) {
    final Status status = Status.fromThrowable(error);
    if (status.getCode() != Status.Code.FAILED_PRECONDITION) {
      return null;
    }
    final String description = status.getDescription();
    if (description == null || !description.startsWith("NOT_LEADER:")) {
      return null;
    }
    final String value = description.substring("NOT_LEADER:".length()).trim();
    if (value.isEmpty()) {
      return null;
    }
    try {
      return Endpoint.parse(value);
    } catch (final IllegalArgumentException ignored) {
      return null;
    }
  }

  private synchronized ActorSchedulingService watchSchedulingService() {
    if (schedulingService != null) {
      return schedulingService;
    }
    if (ownedScheduler == null) {
      ownedScheduler =
          ActorScheduler.newActorScheduler()
              .setCpuBoundActorThreadCount(1)
              .setIoBoundActorThreadCount(1)
              .build();
      ownedScheduler.start();
    }
    return ownedScheduler;
  }

  private ManagedChannel channel(final Endpoint endpoint) {
    return channelsByEndpoint.computeIfAbsent(endpoint, this::newChannel);
  }

  private ManagedChannel newChannel(final Endpoint endpoint) {
    return ManagedChannelBuilder.forAddress(endpoint.host(), endpoint.port())
        .usePlaintext()
        .build();
  }

  private void closeChannel(final ManagedChannel channel) {
    channel.shutdown();
    try {
      if (!channel.awaitTermination(1, TimeUnit.SECONDS)) {
        channel.shutdownNow();
      }
    } catch (final InterruptedException error) {
      Thread.currentThread().interrupt();
      channel.shutdownNow();
    }
  }

  @FunctionalInterface
  private interface UnaryCall<T> {
    void invoke(PlacementDriverGrpc.PlacementDriverStub stub, StreamObserver<T> responseObserver);
  }
}
