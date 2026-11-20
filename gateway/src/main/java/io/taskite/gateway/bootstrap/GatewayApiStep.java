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
package io.taskite.gateway.bootstrap;

import io.grpc.ServerInterceptor;
import io.taskite.gateway.api.GatewayEndpointManager;
import io.taskite.gateway.api.grpc.GatewayClusterHeaderInterceptor;
import io.taskite.gateway.api.grpc.GatewayCommandApi;
import io.taskite.gateway.api.grpc.GatewayTenantInterceptor;
import io.taskite.gateway.api.grpc.GrpcGatewayServer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.util.ArrayList;
import java.util.List;

/** Starts the public gRPC API after clients and routing are available. */
public final class GatewayApiStep implements GatewayStartupStep {
  @Override
  public String name() {
    return "API";
  }

  @Override
  public ActorFuture<GatewayStartupContext> startup(final GatewayStartupContext context) {
    try {
      final GatewayEndpointManager endpointManager =
          new GatewayEndpointManager(
              context.brokerDispatcher(),
              context.placementDriverClient(),
              context.topologyManager(),
              context.runtime().scheduler());
      context.registerResource(GatewayEndpointManager.class, endpointManager, endpointManager);
      final List<ServerInterceptor> interceptors = new ArrayList<>();
      interceptors.add(new GatewayClusterHeaderInterceptor());
      interceptors.addAll(context.interceptorRepository().interceptors());
      interceptors.add(new GatewayTenantInterceptor(context.config().multiTenancy().enabled()));
      final GrpcGatewayServer server =
          new GrpcGatewayServer(
              context.config().network().host(),
              context.config().network().port(),
              new GatewayCommandApi(endpointManager),
              context.config().network().minKeepAliveInterval(),
              interceptors);
      context.server(server);
      server.start();
      return CompletableActorFuture.completed(context);
    } catch (final Exception error) {
      return CompletableActorFuture.completedExceptionally(error);
    }
  }
}
