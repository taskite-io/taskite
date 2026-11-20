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
package io.taskite.placementdriver.client.topology;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.taskite.placementdriver.client.TopologyChangeListener;
import io.taskite.placementdriver.client.mapper.PlacementDriverResponseMapper;
import io.taskite.placementdriver.protocol.BrokerTopology;
import io.taskite.placementdriver.protocol.GetBrokerTopologyRequest;
import io.taskite.placementdriver.protocol.PlacementDriverGrpc;
import io.taskite.scheduler.Actor;
import io.taskite.scheduler.ActorSchedulingService;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.SchedulingHints;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class BrokerTopologyWatch implements AutoCloseable {
  private static final AtomicLong ACTOR_SEQUENCE = new AtomicLong();

  private final List<ManagedChannel> channels;
  private final GetBrokerTopologyRequest request;
  private final TopologyChangeListener listener;
  private final long reconnectDelayMillis;
  private final ActorSchedulingService schedulingService;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicBoolean reconnectRequested = new AtomicBoolean();
  private final Actor actor;
  private volatile ConcurrencyControl control;
  private ScheduledTimer reconnectTimer;

  public BrokerTopologyWatch(
      final List<ManagedChannel> channels,
      final GetBrokerTopologyRequest request,
      final TopologyChangeListener listener,
      final long reconnectDelayMillis,
      final ActorSchedulingService schedulingService) {
    this.channels = List.copyOf(channels);
    this.request = Objects.requireNonNull(request, "request");
    this.listener = Objects.requireNonNull(listener, "listener");
    this.reconnectDelayMillis = reconnectDelayMillis;
    this.schedulingService = Objects.requireNonNull(schedulingService, "schedulingService");
    actor =
        Actor.newActor()
            .name("broker-topology-watch-" + ACTOR_SEQUENCE.incrementAndGet())
            .actorStartedHandler(this::onActorStarted)
            .build();
  }

  public void start() {
    schedulingService
        .submitActor(actor, SchedulingHints.ioBound())
        .whenComplete(
            (ignored, error) -> {
              if (error != null) {
                closed.set(true);
                listener.onTopologyWatchFailed(error);
              }
            });
  }

  private void onActorStarted(final ConcurrencyControl control) {
    this.control = control;
    if (closed.get()) {
      actor.closeAsync();
      return;
    }
    watchAt(0, null);
  }

  private void watchAt(final int index, final Throwable previousError) {
    if (closed.get()) {
      return;
    }
    if (index >= channels.size()) {
      listener.onTopologyWatchFailed(
          previousError == null
              ? new IllegalStateException("no placement-driver endpoint available")
              : previousError);
      scheduleReconnect();
      return;
    }
    PlacementDriverGrpc.newStub(channels.get(index))
        .withWaitForReady()
        .watchBrokerTopology(
            request,
            new StreamObserver<>() {
              @Override
              public void onNext(final BrokerTopology value) {
                runOnActor(
                    () -> {
                      if (!closed.get()) {
                        listener.onTopologyChanged(
                            PlacementDriverResponseMapper.toClusterTopology(value));
                      }
                    });
              }

              @Override
              public void onError(final Throwable error) {
                runOnActor(() -> watchAt(index + 1, error));
              }

              @Override
              public void onCompleted() {
                runOnActor(() -> watchAt(index + 1, null));
              }
            });
  }

  private void scheduleReconnect() {
    if (closed.get() || !reconnectRequested.compareAndSet(false, true)) {
      return;
    }
    if (reconnectTimer != null) {
      reconnectTimer.cancel();
    }
    reconnectTimer =
        control.schedule(
            Duration.ofMillis(reconnectDelayMillis),
            () -> {
              reconnectRequested.set(false);
              watchAt(0, null);
            });
  }

  private void runOnActor(final Runnable action) {
    final ConcurrencyControl currentControl = control;
    if (currentControl != null && !closed.get()) {
      currentControl.run(action);
    }
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      if (reconnectTimer != null) {
        reconnectTimer.cancel();
      }
      actor.closeAsync();
    }
  }
}
