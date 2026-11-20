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

import io.taskite.gateway.broker.GatewayBrokerCommandDispatcher;
import io.taskite.gateway.routing.GatewayBrokerRouter;
import io.taskite.gateway.routing.GatewayTopologyCache;
import io.taskite.gateway.routing.GatewayTopologyManager;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;

/** Starts topology consumption and constructs the Broker routing path. */
public final class GatewayRoutingStep implements GatewayStartupStep {
  @Override
  public String name() {
    return "Routing";
  }

  @Override
  public ActorFuture<GatewayStartupContext> startup(final GatewayStartupContext context) {
    final GatewayTopologyCache topologyCache = new GatewayTopologyCache();
    context.topologyCache(topologyCache);

    final GatewayTopologyManager topologyManager =
        new GatewayTopologyManager(
            context.runtime().concurrencyControl(),
            topologyCache,
            context.placementDriverClient(),
            context.config().placementDriver().topologyRefreshInterval());
    context.topologyManager(topologyManager);

    final GatewayBrokerRouter router = new GatewayBrokerRouter(topologyCache);
    context.brokerRouter(router);
    context.brokerDispatcher(
        new GatewayBrokerCommandDispatcher(context.brokerClient(), context.brokerRouter()));

    return CompletableActorFuture.completed(context);
  }
}
