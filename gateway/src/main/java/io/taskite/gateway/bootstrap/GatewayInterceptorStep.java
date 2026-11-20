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

import io.taskite.gateway.interceptor.GatewayInterceptorRepository;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;

/** Loads configured gRPC interceptors before the public API starts accepting requests. */
public final class GatewayInterceptorStep implements GatewayStartupStep {
  @Override
  public String name() {
    return "Interceptors";
  }

  @Override
  public ActorFuture<GatewayStartupContext> startup(final GatewayStartupContext context) {
    try {
      context.interceptorRepository(
          new GatewayInterceptorRepository(context.config().multiTenancy().interceptors()));
      return CompletableActorFuture.completed(context);
    } catch (final RuntimeException error) {
      return CompletableActorFuture.completedExceptionally(error);
    }
  }
}
