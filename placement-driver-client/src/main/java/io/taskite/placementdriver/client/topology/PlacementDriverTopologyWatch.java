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
import io.taskite.placementdriver.protocol.PlacementDriverGrpc;
import io.taskite.placementdriver.protocol.PlacementDriverTopology;
import io.taskite.placementdriver.protocol.WatchPlacementDriverTopologyRequest;
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
import java.util.function.Consumer;

public final class PlacementDriverTopologyWatch implements AutoCloseable {
  private static final AtomicLong ACTOR_SEQUENCE = new AtomicLong();

  private final List<ManagedChannel> channels;
  private final Consumer<PlacementDriverTopology> listener;
  private final long reconnectDelayMillis;
  private final ActorSchedulingService schedulingService;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicBoolean reconnectRequested = new AtomicBoolean();
  private final Actor actor;
  private volatile ConcurrencyControl control;
  private ScheduledTimer reconnectTimer;

  public PlacementDriverTopologyWatch(
      final List<ManagedChannel> channels,
      final Consumer<PlacementDriverTopology> listener,
      final long reconnectDelayMillis,
      final ActorSchedulingService schedulingService) {
    this.channels = List.copyOf(channels);
    this.listener = Objects.requireNonNull(listener, "listener");
    this.reconnectDelayMillis = reconnectDelayMillis;
    this.schedulingService = Objects.requireNonNull(schedulingService, "schedulingService");
    actor =
        Actor.newActor()
            .name("placement-driver-topology-watch-" + ACTOR_SEQUENCE.incrementAndGet())
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
              }
            });
  }

  private void onActorStarted(final ConcurrencyControl control) {
    this.control = control;
    if (closed.get()) {
      actor.closeAsync();
      return;
    }
    watchAt(0);
  }

  private void watchAt(final int index) {
    if (closed.get()) {
      return;
    }
    if (index >= channels.size()) {
      scheduleReconnect();
      return;
    }
    PlacementDriverGrpc.newStub(channels.get(index))
        .withWaitForReady()
        .watchPlacementDriverTopology(
            WatchPlacementDriverTopologyRequest.getDefaultInstance(),
            new StreamObserver<>() {
              @Override
              public void onNext(final PlacementDriverTopology value) {
                runOnActor(
                    () -> {
                      if (!closed.get()) {
                        listener.accept(value);
                      }
                    });
              }

              @Override
              public void onError(final Throwable error) {
                runOnActor(() -> watchAt(index + 1));
              }

              @Override
              public void onCompleted() {
                runOnActor(() -> watchAt(index + 1));
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
              watchAt(0);
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
