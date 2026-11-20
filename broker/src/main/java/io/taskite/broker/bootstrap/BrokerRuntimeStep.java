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

import io.taskite.broker.BrokerConfig;
import io.taskite.broker.runtime.BrokerRuntime;
import io.taskite.scheduler.future.ActorFuture;

public final class BrokerRuntimeStep implements BrokerStartupStep<BrokerStartupContext> {
  @Override
  public String name() {
    return "Broker Runtime";
  }

  @Override
  public ActorFuture<BrokerStartupContext> startup(final BrokerStartupContext context) {
    return context
        .concurrencyControl()
        .call(
            () -> {
              final BrokerConfig config = context.config();
              final BrokerRuntime runtime =
                  new BrokerRuntime(
                      config,
                      context.clusterConfig().clusterKey(),
                      context.scheduler(),
                      context.meterRegistry());
              context.runtime(runtime);
              runtime.start();
              return context;
            });
  }
}
