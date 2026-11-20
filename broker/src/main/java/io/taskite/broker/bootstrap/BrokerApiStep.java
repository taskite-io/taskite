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
package io.taskite.broker.bootstrap;

import io.taskite.broker.api.BrokerEndpointManager;
import io.taskite.broker.api.grpc.GrpcBrokerServer;
import io.taskite.engine.Engine;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.streamplatform.command.CommandRequestHandler;

public final class BrokerApiStep implements BrokerStartupStep<BrokerStartupContext> {
  @Override
  public String name() {
    return "Broker API";
  }

  @Override
  public ActorFuture<BrokerStartupContext> startup(final BrokerStartupContext context) {
    return context
        .concurrencyControl()
        .call(
            () -> {
              final CommandRequestHandler requestHandler =
                  new CommandRequestHandler(
                      context.runtime()::submit,
                      Engine.eventRegistry(),
                      context.clusterConfig().clusterKey());
              final BrokerEndpointManager endpointManager =
                  new BrokerEndpointManager(
                      context.config().clusterId(),
                      context.clusterConfig().clusterKey(),
                      requestHandler,
                      context.runtime().partitionChangeExecutor());
              final GrpcBrokerServer server =
                  new GrpcBrokerServer(context.config().network().commandPort(), endpointManager);
              server.start();
              context.server(server);
              return context;
            });
  }
}
