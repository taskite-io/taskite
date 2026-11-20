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

import io.taskite.gateway.bootstrap.GatewayStartupContext;
import io.taskite.gateway.bootstrap.GatewayStartupStep;
import io.taskite.gateway.bootstrap.GatewayStartupSteps;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Public Gateway lifecycle entry point. */
public final class Gateway {
  private static final Logger LOG = LoggerFactory.getLogger(Gateway.class);

  private final GatewayStartupContext context;
  private final AtomicReference<ActorFuture<Gateway>> stopFuture = new AtomicReference<>();

  public Gateway(final GatewayConfig config) {
    context = new GatewayStartupContext(Objects.requireNonNull(config, "config"));
  }

  public ActorFuture<Gateway> start() {
    final ActorFuture<Gateway> result = new CompletableActorFuture<>();
    context
        .runtime()
        .start()
        .onComplete(
            (ignored, failure) -> {
              if (failure != null) {
                result.completeExceptionally(failure);
              } else {
                context
                    .runtime()
                    .concurrencyControl()
                    .run(() -> startup(0, GatewayStartupSteps.build(), result));
              }
            });
    return result;
  }

  public ActorFuture<Gateway> stop() {
    final ActorFuture<Gateway> existing = stopFuture.get();
    if (existing != null) {
      return existing;
    }
    final ActorFuture<Gateway> result = new CompletableActorFuture<>();
    if (!stopFuture.compareAndSet(null, result)) {
      return stopFuture.get();
    }

    final Runnable shutdown = () -> closeContextAndRuntime(result);
    if (context.runtime().started()) {
      context.runtime().concurrencyControl().run(shutdown);
    } else {
      shutdown.run();
    }
    return result;
  }

  public int port() {
    return context.serverPort();
  }

  public void blockUntilShutdown() throws InterruptedException {
    Thread.currentThread().join();
  }

  private void startup(
      final int stepIndex,
      final List<GatewayStartupStep> steps,
      final ActorFuture<Gateway> result) {
    if (stepIndex >= steps.size()) {
      LOG.info(
          "gateway started, placementDrivers={}, api={}:{}, managementThreads={}",
          context.config().placementDriver().endpoints(),
          context.config().network().host(),
          context.serverPort(),
          context.config().threads().management());
      result.complete(this);
      return;
    }

    final GatewayStartupStep step = steps.get(stepIndex);
    final ActorFuture<GatewayStartupContext> startup;
    try {
      startup = step.startup(context);
    } catch (final Throwable error) {
      failStartup(step, error, result);
      return;
    }
    context
        .runtime()
        .concurrencyControl()
        .runOnCompletion(
            startup,
            (ignored, failure) -> {
              if (failure != null) {
                failStartup(step, failure, result);
              } else {
                startup(stepIndex + 1, steps, result);
              }
            });
  }

  private void failStartup(
      final GatewayStartupStep step, final Throwable error, final ActorFuture<Gateway> result) {
    LOG.error("Failed to start Gateway step {}, tearing down context", step.name(), error);
    stop()
        .onComplete(
            (ignored, closeError) -> {
              if (closeError != null) {
                error.addSuppressed(closeError);
              }
              result.completeExceptionally(error);
            });
  }

  private void closeContextAndRuntime(final ActorFuture<Gateway> result) {
    Throwable closeFailure = null;
    try {
      context.close();
    } catch (final Throwable error) {
      closeFailure = error;
    }
    final Throwable contextFailure = closeFailure;
    context
        .runtime()
        .stop()
        .onComplete(
            (ignored, runtimeFailure) -> {
              if (contextFailure != null) {
                if (runtimeFailure != null) {
                  contextFailure.addSuppressed(runtimeFailure);
                }
                result.completeExceptionally(contextFailure);
              } else if (runtimeFailure != null) {
                result.completeExceptionally(runtimeFailure);
              } else {
                result.complete(this);
              }
            });
  }
}
