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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Status;
import io.taskite.gateway.common.ClusterTopologyUnavailableException;
import io.taskite.gateway.common.UnknownClusterException;
import io.taskite.gateway.routing.GatewayTopologyCache;
import io.taskite.gateway.routing.GatewayTopologyManager;
import io.taskite.placementdriver.client.Client;
import io.taskite.placementdriver.client.TopologyChangeListener;
import io.taskite.placementdriver.client.response.ClusterConfig;
import io.taskite.placementdriver.protocol.DeregisterBrokerRequest;
import io.taskite.placementdriver.protocol.DeregisterBrokerResponse;
import io.taskite.placementdriver.protocol.DeregisterWorkerRequest;
import io.taskite.placementdriver.protocol.DeregisterWorkerResponse;
import io.taskite.placementdriver.protocol.DistributeCommandRequest;
import io.taskite.placementdriver.protocol.DistributeCommandResponse;
import io.taskite.placementdriver.protocol.GetBrokerTopologyRequest;
import io.taskite.placementdriver.protocol.GetClusterConfigRequest;
import io.taskite.placementdriver.protocol.RegisterBrokerRequest;
import io.taskite.placementdriver.protocol.RegisterBrokerResponse;
import io.taskite.placementdriver.protocol.RegisterWorkerRequest;
import io.taskite.placementdriver.protocol.RegisterWorkerResponse;
import io.taskite.placementdriver.protocol.RenewBrokerRequest;
import io.taskite.placementdriver.protocol.RenewBrokerResponse;
import io.taskite.placementdriver.protocol.RenewWorkerRequest;
import io.taskite.placementdriver.protocol.RenewWorkerResponse;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.topology.ClusterTopology;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;

final class GatewayTopologyManagerTest {
  @Test
  void sharesConcurrentCacheMissAndCachesSuccessfulTopology() {
    final GatewayTopologyCache cache = new GatewayTopologyCache();
    final ControllablePlacementDriver placementDriver = new ControllablePlacementDriver();
    try (GatewayTopologyManager manager =
        new GatewayTopologyManager(
            ImmediateConcurrencyControl.INSTANCE, cache, placementDriver, Duration.ofSeconds(1))) {
      final ActorFuture<ClusterTopology> first = manager.topology("cluster-a");
      final ActorFuture<ClusterTopology> second = manager.topology("cluster-a");

      assertSame(first, second);
      assertEquals(1, placementDriver.topologyRequests);

      placementDriver.lookup.complete(topology("cluster-a"));

      assertEquals("cluster-a", first.join().clusterId());
      assertTrue(cache.contains("cluster-a"));
      assertEquals(List.of("cluster-a"), cache.knownClusterIds().stream().toList());
      assertEquals(1, placementDriver.watchRequests);
      assertEquals("cluster-a", manager.topology("cluster-a").join().clusterId());
      assertEquals(1, placementDriver.topologyRequests);
    }
  }

  @Test
  void mapsOnlyExplicitNotFoundToUnknownClusterAndClearsSingleFlight() {
    final GatewayTopologyCache cache = new GatewayTopologyCache();
    cache.update(topology("demo"));
    final ControllablePlacementDriver placementDriver = new ControllablePlacementDriver();
    try (GatewayTopologyManager manager =
        new GatewayTopologyManager(
            ImmediateConcurrencyControl.INSTANCE, cache, placementDriver, Duration.ofSeconds(1))) {
      final ActorFuture<ClusterTopology> demo2 = manager.topology("demo2");
      placementDriver.lookup.complete(topology("demo2"));
      assertEquals("demo2", demo2.join().clusterId());

      placementDriver.lookup = new CompletableActorFuture<>();
      final ActorFuture<ClusterTopology> first = manager.topology("demo3");
      placementDriver.lookup.completeExceptionally(
          Status.NOT_FOUND.withDescription("CLUSTER_NOT_REGISTERED: demo3").asRuntimeException());

      final UnknownClusterException error =
          assertInstanceOf(UnknownClusterException.class, first.getException());
      assertEquals("demo3", error.requestedClusterId());
      assertEquals(List.of("demo", "demo2"), error.knownClusterIds().stream().toList());
      assertEquals(
          "UNKNOWN_CLUSTER: requested cluster \"demo3\" is not known by this gateway, "
              + "know clusters: [\"demo\", \"demo2\"]",
          error.getMessage());

      placementDriver.lookup = new CompletableActorFuture<>();
      manager.topology("demo3");
      assertEquals(3, placementDriver.topologyRequests);
    }
  }

  @Test
  void preservesUnavailableAndRejectsMismatchedTopology() {
    final ControllablePlacementDriver unavailable = new ControllablePlacementDriver();
    try (GatewayTopologyManager manager =
        new GatewayTopologyManager(
            ImmediateConcurrencyControl.INSTANCE,
            new GatewayTopologyCache(),
            unavailable,
            Duration.ofSeconds(1))) {
      final ActorFuture<ClusterTopology> lookup = manager.topology("cluster-a");
      unavailable.lookup.completeExceptionally(
          Status.UNAVAILABLE.withDescription("pd unavailable").asRuntimeException());
      assertInstanceOf(ClusterTopologyUnavailableException.class, lookup.getException());
    }

    final GatewayTopologyCache cache = new GatewayTopologyCache();
    final ControllablePlacementDriver mismatched = new ControllablePlacementDriver();
    try (GatewayTopologyManager manager =
        new GatewayTopologyManager(
            ImmediateConcurrencyControl.INSTANCE, cache, mismatched, Duration.ofSeconds(1))) {
      final ActorFuture<ClusterTopology> lookup = manager.topology("cluster-a");
      mismatched.lookup.complete(topology("cluster-b"));
      assertInstanceOf(ClusterTopologyUnavailableException.class, lookup.getException());
      assertFalse(cache.contains("cluster-a"));
    }
  }

  @Test
  void validatesClusterId() {
    final GatewayTopologyManager manager =
        new GatewayTopologyManager(
            ImmediateConcurrencyControl.INSTANCE,
            new GatewayTopologyCache(),
            new ControllablePlacementDriver(),
            Duration.ofSeconds(1));
    try (manager) {
      assertThrows(IllegalArgumentException.class, () -> manager.topology(" "));
    }
  }

  private static ClusterTopology topology(final String clusterId) {
    return new ClusterTopology(clusterId, 1, 1, 1, List.of(), Map.of(), Instant.EPOCH);
  }

  private enum ImmediateConcurrencyControl implements ConcurrencyControl {
    INSTANCE;

    @Override
    public <T> void runOnCompletion(
        final ActorFuture<T> future, final BiConsumer<T, Throwable> callback) {
      future.onComplete(callback, Runnable::run);
    }

    @Override
    public void run(final Runnable action) {
      action.run();
    }

    @Override
    public <T> ActorFuture<T> call(final Callable<T> callable) {
      try {
        return CompletableActorFuture.completed(callable.call());
      } catch (final Exception error) {
        return CompletableActorFuture.completedExceptionally(error);
      }
    }

    @Override
    public ScheduledTimer schedule(final Duration delay, final Runnable runnable) {
      return () -> {};
    }
  }

  private static final class ControllablePlacementDriver implements Client {
    private CompletableActorFuture<ClusterTopology> lookup = new CompletableActorFuture<>();
    private int topologyRequests;
    private int watchRequests;

    @Override
    public ActorFuture<ClusterTopology> topology(final GetBrokerTopologyRequest request) {
      topologyRequests++;
      return lookup;
    }

    @Override
    public AutoCloseable watchBrokerTopology(
        final GetBrokerTopologyRequest request, final TopologyChangeListener listener) {
      watchRequests++;
      return () -> {};
    }

    @Override
    public ActorFuture<ClusterConfig> clusterConfig(final GetClusterConfigRequest request) {
      return unsupported();
    }

    @Override
    public ActorFuture<RegisterBrokerResponse> registerBroker(final RegisterBrokerRequest request) {
      return unsupported();
    }

    @Override
    public ActorFuture<RenewBrokerResponse> renewBroker(final RenewBrokerRequest request) {
      return unsupported();
    }

    @Override
    public ActorFuture<DeregisterBrokerResponse> deregisterBroker(
        final DeregisterBrokerRequest request) {
      return unsupported();
    }

    @Override
    public ActorFuture<RegisterWorkerResponse> registerWorker(final RegisterWorkerRequest request) {
      return unsupported();
    }

    @Override
    public ActorFuture<RenewWorkerResponse> renewWorker(final RenewWorkerRequest request) {
      return unsupported();
    }

    @Override
    public ActorFuture<DeregisterWorkerResponse> deregisterWorker(
        final DeregisterWorkerRequest request) {
      return unsupported();
    }

    @Override
    public ActorFuture<DistributeCommandResponse> distributeCommand(
        final DistributeCommandRequest request) {
      return unsupported();
    }

    @Override
    public ActorFuture<io.taskite.placementdriver.protocol.CreateTimerSubscriptionResponse>
        createTimerSubscription(
            final io.taskite.placementdriver.protocol.CreateTimerSubscriptionRequest request) {
      return unsupported();
    }

    @Override
    public ActorFuture<io.taskite.placementdriver.protocol.DeleteTimerSubscriptionResponse>
        deleteTimerSubscription(
            final io.taskite.placementdriver.protocol.DeleteTimerSubscriptionRequest request) {
      return unsupported();
    }

    @Override
    public void close() {}

    private static <T> ActorFuture<T> unsupported() {
      return CompletableActorFuture.completedExceptionally(new UnsupportedOperationException());
    }
  }
}
