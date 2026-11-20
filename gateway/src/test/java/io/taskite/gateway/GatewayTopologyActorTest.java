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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.gateway.routing.GatewayTopologyActor;
import io.taskite.gateway.routing.GatewayTopologyCache;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;

final class GatewayTopologyActorTest {
  @Test
  void updatesCacheFromWatchAndConfiguredPeriodicRefresh() {
    final GatewayTopologyCache cache = new GatewayTopologyCache();
    cache.update(topology(1));
    final CapturingConcurrencyControl concurrency = new CapturingConcurrencyControl();
    final CapturingPlacementDriver placementDriver = new CapturingPlacementDriver();
    final GatewayTopologyActor actor =
        new GatewayTopologyActor(
            "demo2", concurrency, cache, placementDriver, Duration.ofMillis(2500));

    actor.start();

    assertEquals(1, placementDriver.watchRequests);
    assertEquals("demo2", placementDriver.watchedClusterId);
    assertEquals(Duration.ofMillis(2500), concurrency.scheduled.getFirst().delay());

    placementDriver.listener.onTopologyChanged(topology(2));
    assertEquals(2, cache.current("demo2").version());

    placementDriver.nextTopology = CompletableActorFuture.completed(topology(3));
    concurrency.scheduled.getFirst().action().run();
    assertEquals(1, placementDriver.topologyRequests);
    assertEquals(3, cache.current("demo2").version());
    assertEquals(Duration.ofMillis(2500), concurrency.scheduled.get(1).delay());

    placementDriver.listener.onTopologyWatchFailed(new RuntimeException("disconnected"));
    assertEquals(Duration.ofMillis(2500), concurrency.scheduled.get(2).delay());
    concurrency.scheduled.get(2).action().run();
    assertEquals(2, placementDriver.watchRequests);

    actor.close();
    assertTrue(placementDriver.closedWatches > 0);
  }

  private static ClusterTopology topology(final long version) {
    return new ClusterTopology("demo2", version, 1, 1, List.of(), Map.of(), Instant.EPOCH);
  }

  private static final class CapturingConcurrencyControl implements ConcurrencyControl {
    private final List<ScheduledAction> scheduled = new ArrayList<>();

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
      final ScheduledAction scheduledAction = new ScheduledAction(delay, runnable);
      scheduled.add(scheduledAction);
      return scheduledAction;
    }
  }

  private static final class CapturingPlacementDriver implements Client {
    private ActorFuture<ClusterTopology> nextTopology;
    private TopologyChangeListener listener;
    private String watchedClusterId;
    private int topologyRequests;
    private int watchRequests;
    private int closedWatches;

    @Override
    public ActorFuture<ClusterTopology> topology(final GetBrokerTopologyRequest request) {
      topologyRequests++;
      return nextTopology;
    }

    @Override
    public AutoCloseable watchBrokerTopology(
        final GetBrokerTopologyRequest request, final TopologyChangeListener listener) {
      watchRequests++;
      watchedClusterId = request.getClusterId();
      this.listener = listener;
      return () -> closedWatches++;
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

  private static final class ScheduledAction implements ScheduledTimer {
    private final Duration delay;
    private final Runnable action;
    private boolean cancelled;

    private ScheduledAction(final Duration delay, final Runnable action) {
      this.delay = delay;
      this.action = action;
    }

    private Duration delay() {
      return delay;
    }

    private Runnable action() {
      return action;
    }

    @Override
    public void cancel() {
      cancelled = true;
    }
  }
}
