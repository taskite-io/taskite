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
package io.taskite.dist.broker;

import io.micrometer.core.instrument.MeterRegistry;
import io.taskite.broker.Broker;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/** Starts and stops the framework-independent Broker under Spring's process lifecycle. */
@Component
@ConditionalOnProperty(prefix = "taskite.broker", name = "enabled", matchIfMissing = true)
@Profile("broker")
public final class BrokerSpringLifecycle implements SmartLifecycle {
  private static final Logger LOG = LoggerFactory.getLogger(BrokerSpringLifecycle.class);
  private static final long STARTUP_TIMEOUT_SECONDS = 360;
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;

  private final BrokerConfiguration configuration;
  private final MeterRegistry meterRegistry;
  private final ConfigurableApplicationContext applicationContext;
  private final AtomicBoolean localRemovalShutdownStarted = new AtomicBoolean();
  private volatile Broker broker;
  private volatile boolean running;

  public BrokerSpringLifecycle(
      final BrokerConfiguration configuration,
      final MeterRegistry meterRegistry,
      final ConfigurableApplicationContext applicationContext) {
    this.configuration = configuration;
    this.meterRegistry = meterRegistry;
    this.applicationContext = applicationContext;
  }

  @Override
  public int getPhase() {
    return 0;
  }

  @Override
  public void start() {
    if (running) {
      return;
    }
    final Broker startedBroker =
        new Broker(
            configuration.toBrokerConfig(), meterRegistry, this::closeAfterLocalBrokerRemoval);
    try {
      startedBroker.start().join(STARTUP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (final RuntimeException error) {
      throw new IllegalStateException("broker runtime failed to start", error);
    }
    broker = startedBroker;
    running = true;
    LOG.info(
        "broker runtime started (broker {}, grpc {})",
        configuration.cluster().brokerId(),
        startedBroker.port());
  }

  @Override
  public void stop() {
    final Broker current = broker;
    running = false;
    if (current == null) {
      return;
    }
    try {
      current.stop().join(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (final RuntimeException error) {
      LOG.warn("broker runtime failed to stop cleanly", error);
    } finally {
      broker = null;
    }
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  public int port() {
    final Broker current = broker;
    if (current == null) {
      throw new IllegalStateException("broker runtime is not running");
    }
    return current.port();
  }

  public boolean ready() {
    final Broker current = broker;
    return running && current != null && current.ready();
  }

  private void closeAfterLocalBrokerRemoval() {
    if (!localRemovalShutdownStarted.compareAndSet(false, true)) {
      return;
    }
    Thread.ofPlatform()
        .name("broker-removed-context-shutdown")
        .start(
            () -> {
              LOG.info(
                  "broker {} was removed; closing application context",
                  configuration.cluster().brokerId());
              applicationContext.close();
            });
  }
}
