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
package io.taskite.placementdriver;

import io.micrometer.core.instrument.MeterRegistry;
import io.taskite.placementdriver.api.PlacementDriverEndpointManager;
import io.taskite.placementdriver.bootstrap.PlacementDriverStartupContext;
import io.taskite.placementdriver.bootstrap.PlacementDriverStartupStep;
import io.taskite.placementdriver.bootstrap.PlacementDriverStartupSteps;
import io.taskite.scheduler.Actor;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.SchedulingHints;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PlacementDriver {
  private static final Logger LOG = LoggerFactory.getLogger(PlacementDriver.class);

  private final PlacementDriverStartupContext context;
  private ActorFuture<PlacementDriver> stopFuture;
  private ActorFuture<PlacementDriver> startFuture;
  private volatile boolean stopping;
  private volatile Throwable startupFailure;

  public PlacementDriver(final PlacementDriverConfiguration config) {
    this(config, 0, new PlacementDriverEndpointManager());
  }

  public PlacementDriver(final PlacementDriverConfiguration config, final int httpPort) {
    this(config, httpPort, new PlacementDriverEndpointManager());
  }

  public PlacementDriver(
      final PlacementDriverConfiguration config,
      final int httpPort,
      final PlacementDriverEndpointManager endpointManager) {
    context =
        new PlacementDriverStartupContext(
            config, httpPort, endpointManager, this::stopAfterLocalRemoval);
  }

  public PlacementDriver(
      final PlacementDriverConfiguration config,
      final int httpPort,
      final PlacementDriverEndpointManager endpointManager,
      final Runnable localMemberRemoved) {
    this(config, httpPort, endpointManager, localMemberRemoved, null);
  }

  public PlacementDriver(
      final PlacementDriverConfiguration config,
      final int httpPort,
      final PlacementDriverEndpointManager endpointManager,
      final Runnable localMemberRemoved,
      final MeterRegistry meterRegistry) {
    context =
        new PlacementDriverStartupContext(
            config, httpPort, endpointManager, localMemberRemoved, meterRegistry);
  }

  public synchronized ActorFuture<PlacementDriver> start() {
    if (stopping) {
      return CompletableActorFuture.completedExceptionally(
          new IllegalStateException("placement-driver is stopping"));
    }
    if (startFuture != null) {
      return startFuture;
    }
    final ActorFuture<PlacementDriver> result = new CompletableActorFuture<>();
    startFuture = result;
    try {
      context.scheduler().start();
      context.markSchedulerStarted();
      context
          .scheduler()
          .submitActor((Actor) context.concurrencyControl(), SchedulingHints.ioBound())
          .onComplete(
              (ignored, failure) -> {
                if (failure != null) {
                  failStartup(result, failure);
                } else {
                  context
                      .concurrencyControl()
                      .run(() -> startup(0, PlacementDriverStartupSteps.build(), result));
                }
              });
    } catch (final Throwable error) {
      failStartup(result, error);
    }
    return result;
  }

  public synchronized ActorFuture<PlacementDriver> stop() {
    if (stopFuture != null) {
      return stopFuture;
    }
    final ActorFuture<PlacementDriver> result = new CompletableActorFuture<>();
    stopFuture = result;
    stopping = true;
    if (startupFailure == null && startFuture != null && !startFuture.isDone()) {
      startFuture.completeExceptionally(
          new IllegalStateException("placement-driver stopped during startup"));
    }
    final Runnable shutdown =
        () -> {
          Throwable closeError = null;
          try {
            context.close();
          } catch (final Throwable error) {
            closeError = error;
          }
          final Throwable failure = closeError;
          final ActorFuture<PlacementDriver> schedulerStopped = new CompletableActorFuture<>();
          schedulerStopped.onComplete(
              (ignored, schedulerError) -> {
                if (failure != null) {
                  if (schedulerError != null) {
                    failure.addSuppressed(schedulerError);
                  }
                  result.completeExceptionally(failure);
                } else if (schedulerError != null) {
                  result.completeExceptionally(schedulerError);
                } else {
                  result.complete(this);
                }
              });
          stopScheduler(schedulerStopped);
        };
    if (context.schedulerStarted()) {
      context.concurrencyControl().run(shutdown);
    } else {
      shutdown.run();
    }
    return result;
  }

  private void startup(
      final int stepIndex,
      final List<PlacementDriverStartupStep<PlacementDriverStartupContext>> steps,
      final ActorFuture<PlacementDriver> result) {
    if (stopping) {
      result.completeExceptionally(
          new IllegalStateException("placement-driver stopped during startup"));
      return;
    }
    if (stepIndex >= steps.size()) {
      result.complete(this);
      return;
    }

    final PlacementDriverStartupStep<PlacementDriverStartupContext> step = steps.get(stepIndex);
    final ActorFuture<PlacementDriverStartupContext> startup;
    try {
      startup = step.startup(context);
    } catch (final Throwable error) {
      LOG.error("Failed to start step {}, tearing down context", step.name(), error);
      failStartup(result, error);
      return;
    }
    context
        .concurrencyControl()
        .runOnCompletion(
            startup,
            (ignored, failure) -> {
              if (failure != null) {
                LOG.error("Failed to complete step {}, tearing down context", step.name(), failure);
                failStartup(result, failure);
              } else {
                LOG.info("- Startup {}", step.name());
                startup(stepIndex + 1, steps, result);
              }
            });
  }

  private void failStartup(final ActorFuture<PlacementDriver> result, final Throwable failure) {
    startupFailure = failure;
    stop()
        .onComplete(
            (ignored, closeError) -> {
              if (closeError != null && closeError != failure) {
                failure.addSuppressed(closeError);
              }
              result.completeExceptionally(failure);
            });
  }

  private void stopScheduler(final ActorFuture<PlacementDriver> result) {
    if (!context.schedulerStarted()) {
      result.complete(this);
      return;
    }
    try {
      context
          .scheduler()
          .stop()
          .onComplete(
              (ignored, failure) -> {
                if (failure != null) {
                  result.completeExceptionally(failure);
                } else {
                  context.markSchedulerStopped();
                  result.complete(this);
                }
              });
    } catch (final Throwable error) {
      result.completeExceptionally(error);
    }
  }

  private void stopAfterLocalRemoval() {
    Thread.ofPlatform()
        .name("placement-driver-removed-shutdown")
        .start(
            () -> {
              try {
                stop().join();
              } catch (final RuntimeException error) {
                LOG.warn("failed to stop removed placement-driver node", error);
              }
            });
  }

  /** The single actor scheduler used to serialize admin state access. */
  public ConcurrencyControl concurrencyControl() {
    return context.concurrencyControl();
  }

  /** The bound business gRPC port (0 resolves to an ephemeral port; read after startup). */
  public int grpcPort() {
    return context.grpcServer().port();
  }
}
