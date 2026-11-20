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
package io.taskite.gateway.runtime;

import io.taskite.scheduler.Actor;
import io.taskite.scheduler.ActorScheduler;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;

/** Owns the Gateway scheduler and lifecycle actor. */
public final class GatewayRuntime {
  private final int managementThreads;
  private final ActorScheduler scheduler;
  private final ConcurrencyControl lifecycleControl =
      Actor.newActor().name("gateway-runtime").build();
  private volatile boolean started;

  public GatewayRuntime(final int managementThreads) {
    if (managementThreads <= 0) {
      throw new IllegalArgumentException("managementThreads must be positive");
    }
    this.managementThreads = managementThreads;
    scheduler =
        ActorScheduler.newActorScheduler().setCpuBoundActorThreadCount(managementThreads).build();
  }

  public ActorFuture<GatewayRuntime> start() {
    final ActorFuture<GatewayRuntime> result = new CompletableActorFuture<>();
    try {
      scheduler.start();
      started = true;
      scheduler
          .submitActor((Actor) lifecycleControl)
          .onComplete(
              (ignored, failure) -> {
                if (failure != null) {
                  stopAfterFailedStart(failure, result);
                } else {
                  result.complete(this);
                }
              });
    } catch (final Throwable error) {
      stopAfterFailedStart(error, result);
    }
    return result;
  }

  public ActorFuture<GatewayRuntime> stop() {
    if (!started) {
      return CompletableActorFuture.completed(this);
    }
    final ActorFuture<GatewayRuntime> result = new CompletableActorFuture<>();
    scheduler
        .stop()
        .onComplete(
            (ignored, failure) -> {
              started = false;
              if (failure != null) {
                result.completeExceptionally(failure);
              } else {
                result.complete(this);
              }
            });
    return result;
  }

  public ConcurrencyControl concurrencyControl() {
    return lifecycleControl;
  }

  public int managementThreads() {
    return managementThreads;
  }

  public ActorScheduler scheduler() {
    return scheduler;
  }

  public boolean started() {
    return started;
  }

  private void stopAfterFailedStart(
      final Throwable startupFailure, final ActorFuture<GatewayRuntime> result) {
    if (!started) {
      result.completeExceptionally(startupFailure);
      return;
    }
    scheduler
        .stop()
        .onComplete(
            (ignored, stopFailure) -> {
              started = false;
              if (stopFailure != null) {
                startupFailure.addSuppressed(stopFailure);
              }
              result.completeExceptionally(startupFailure);
            });
  }
}
