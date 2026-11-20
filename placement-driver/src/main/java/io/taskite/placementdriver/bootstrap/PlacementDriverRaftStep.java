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

import io.taskite.placementdriver.runtime.PlacementDriverRaftRuntime;
import io.taskite.scheduler.future.ActorFuture;

public final class PlacementDriverRaftStep
    implements PlacementDriverStartupStep<PlacementDriverStartupContext> {
  @Override
  public String name() {
    return "Raft";
  }

  @Override
  public ActorFuture<PlacementDriverStartupContext> startup(
      final PlacementDriverStartupContext context) {
    return context
        .concurrencyControl()
        .call(
            () -> {
              context.raftRuntime(
                  new PlacementDriverRaftRuntime(
                      new PlacementDriverRaftRuntime.Configuration(
                          context.config(),
                          new PlacementDriverRaftRuntime.Endpoints(
                              context.httpPort(), context.grpcServer().port()),
                          new PlacementDriverRaftRuntime.Bootstrap(
                              context.bootstrapPeers(),
                              context.bootstrapLeaderEndpoint().orElse(""))),
                      new PlacementDriverRaftRuntime.Context(
                          context.concurrencyControl(),
                          context.scheduler(),
                          context.eventRegistry(),
                          context.processorDependencies(),
                          context.meterRegistry())));
              return context;
            })
        .thenCompose(
            startedContext ->
                startedContext.raftRuntime().startupFuture().thenApply(ignored -> startedContext));
  }
}
