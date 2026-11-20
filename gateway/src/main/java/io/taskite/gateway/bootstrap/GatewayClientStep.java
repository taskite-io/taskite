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

import io.taskite.broker.client.impl.BrokerClientImpl;
import io.taskite.broker.client.transport.GrpcBrokerClientTransport;
import io.taskite.placementdriver.client.GrpcClient;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;

/** Creates the PD and Broker transport clients owned by the Gateway process. */
public final class GatewayClientStep implements GatewayStartupStep {
  @Override
  public String name() {
    return "Clients";
  }

  @Override
  public ActorFuture<GatewayStartupContext> startup(final GatewayStartupContext context) {
    context.placementDriverClient(
        GrpcClient.connect(
            context.config().placementDriver().endpoints(),
            context.runtime().scheduler(),
            context.config().placementDriver().requestTimeout()));
    context.brokerClient(
        new BrokerClientImpl(
            new GrpcBrokerClientTransport(),
            context.config().broker().requestTimeout(),
            context.config().broker().maxRetries()));
    return CompletableActorFuture.completed(context);
  }
}
