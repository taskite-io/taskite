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
package io.taskite.placementdriver.bootstrap;

import io.taskite.placementdriver.runtime.PlacementDriverRuntimeComponents;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;

public final class PlacementDriverRuntimeComponentsStep
    implements PlacementDriverStartupStep<PlacementDriverStartupContext> {
  @Override
  public String name() {
    return "Runtime Components";
  }

  @Override
  public ActorFuture<PlacementDriverStartupContext> startup(
      final PlacementDriverStartupContext context) {
    final CompletableActorFuture<PlacementDriverStartupContext> result =
        new CompletableActorFuture<>();
    final ActorFuture<ActorFuture<Void>> setup =
        context
            .concurrencyControl()
            .call(
                () -> {
                  final PlacementDriverRuntimeComponents components =
                      new PlacementDriverRuntimeComponents(
                          context.config(),
                          context.concurrencyControl(),
                          context.raftRuntime(),
                          context.localMemberRemoved(),
                          context.meterRegistry());
                  context.runtimeComponents(components);
                  context
                      .endpointManager()
                      .bindRuntime(
                          context.raftRuntime().commandSubmitter(),
                          context.raftRuntime().eventRegistry(),
                          context.raftRuntime()::nodeSnapshot,
                          components::watchBrokerTopology,
                          components::watchPlacementDriverTopology,
                          components,
                          context.raftRuntime()::updateLocalTopology);
                  return components.start();
                });
    setup.onComplete(
        (initialization, setupError) -> {
          if (setupError != null) {
            result.completeExceptionally(setupError);
            return;
          }
          initialization.onComplete(
              (ignored, initializationError) -> {
                if (initializationError != null) {
                  result.completeExceptionally(initializationError);
                } else {
                  result.complete(context);
                }
              });
        });
    return result;
  }
}
