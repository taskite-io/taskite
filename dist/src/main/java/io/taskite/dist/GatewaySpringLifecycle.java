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
package io.taskite.dist;

import io.taskite.gateway.Gateway;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/** Starts and stops the framework-independent Gateway under Spring's process lifecycle. */
@Component
public final class GatewaySpringLifecycle implements SmartLifecycle {
  private static final Logger LOG = LoggerFactory.getLogger(GatewaySpringLifecycle.class);
  private static final long STARTUP_TIMEOUT_SECONDS = 30;
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;

  private final GatewayConfiguration configuration;
  private volatile Gateway gateway;
  private volatile boolean running;

  public GatewaySpringLifecycle(final GatewayConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public void start() {
    if (running) {
      return;
    }
    final Gateway startedGateway = new Gateway(configuration.toGatewayConfig());
    try {
      startedGateway.start().join(STARTUP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (final RuntimeException error) {
      throw new IllegalStateException("gateway runtime failed to start", error);
    }
    gateway = startedGateway;
    running = true;
    LOG.info("gateway runtime ready on port {}", startedGateway.port());
  }

  @Override
  public void stop() {
    final Gateway current = gateway;
    running = false;
    if (current == null) {
      return;
    }
    try {
      current.stop().join(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (final RuntimeException error) {
      LOG.warn("gateway runtime failed to stop cleanly", error);
    } finally {
      gateway = null;
    }
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  public int port() {
    final Gateway current = gateway;
    if (current == null) {
      throw new IllegalStateException("gateway runtime is not running");
    }
    return current.port();
  }
}
