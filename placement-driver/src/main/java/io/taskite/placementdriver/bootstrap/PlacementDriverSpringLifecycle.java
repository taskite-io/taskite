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

import io.micrometer.core.instrument.MeterRegistry;
import io.taskite.placementdriver.PlacementDriver;
import io.taskite.placementdriver.PlacementDriverConfiguration;
import io.taskite.placementdriver.api.PlacementDriverEndpointManager;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Owns the placement-driver actor/raft/gRPC runtime under Spring's lifecycle.
 *
 * <p>Phase is {@code 0}, strictly below Boot's {@code WebServerStartStopLifecycle} phase ({@code
 * Integer.MAX_VALUE - 2048}). SmartLifecycle starts in ascending phase order and stops in
 * descending order, so this runtime finishes {@link #start()} <em>before</em> Tomcat begins
 * accepting requests, and on shutdown Tomcat stops accepting <em>before</em> {@link #stop()} tears
 * the runtime down, closing the request/startup race on both ends.
 */
@Component
public class PlacementDriverSpringLifecycle implements SmartLifecycle {
  private static final Logger LOG = LoggerFactory.getLogger(PlacementDriverSpringLifecycle.class);
  private static final long STARTUP_TIMEOUT_SECONDS = 60;
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;

  private final PlacementDriverConfiguration configuration;
  private final ServerProperties serverProperties;
  private final PlacementDriverEndpointManager endpointManager;
  private final ConfigurableApplicationContext applicationContext;
  private final MeterRegistry meterRegistry;
  private final AtomicBoolean localRemovalShutdownStarted = new AtomicBoolean();
  private volatile PlacementDriver placementDriver;
  private volatile boolean running;

  public PlacementDriverSpringLifecycle(
      final PlacementDriverConfiguration configuration,
      final ServerProperties serverProperties,
      final PlacementDriverEndpointManager endpointManager,
      final ConfigurableApplicationContext applicationContext,
      final MeterRegistry meterRegistry) {
    this.configuration = configuration;
    this.serverProperties = serverProperties;
    this.endpointManager = endpointManager;
    this.applicationContext = applicationContext;
    this.meterRegistry = meterRegistry;
  }

  @Override
  public int getPhase() {
    // Must be < WebServerStartStopLifecycle.getPhase() (Integer.MAX_VALUE - 2048) so the runtime is
    // ready before Tomcat accepts and torn down after Tomcat stops accepting.
    return 0;
  }

  @Override
  public void start() {
    if (running) {
      return;
    }
    // The real bound port is only available after this lifecycle starts. Preserve server.port=0 as
    // the ephemeral-port marker in the runtime snapshot.
    final Integer configuredHttpPort = serverProperties.getPort();
    final int httpPort = configuredHttpPort == null ? 8080 : configuredHttpPort;
    final PlacementDriver driver =
        new PlacementDriver(
            configuration,
            httpPort,
            endpointManager,
            this::closeAfterLocalMemberRemoval,
            meterRegistry);
    placementDriver = driver;
    try {
      driver.start().join(STARTUP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (final RuntimeException error) {
      try {
        driver.stop().join(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (final RuntimeException closeError) {
        error.addSuppressed(closeError);
      }
      // Fail the context refresh so SpringApplication.run exits non-zero rather than serving on a
      // half-initialized runtime.
      throw new IllegalStateException("placement-driver runtime failed to start", error);
    }
    // The broker-lease/timeout scanners are already started by PlacementDriverRuntimeComponentsStep
    // on the actor
    // thread during driver.start(); do not start them again here (a second, off-actor call would
    // leak scan timers and read actor-confined state from the Spring thread).
    running = true;
    LOG.info(
        "placement-driver runtime ready (node {}, grpc {})",
        configuration.nodeId(),
        driver.grpcPort());
  }

  @Override
  public void stop() {
    final PlacementDriver driver = placementDriver;
    running = false;
    if (driver == null) {
      return;
    }
    try {
      driver.stop().join(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (final RuntimeException error) {
      LOG.warn("placement-driver runtime failed to stop cleanly", error);
    } finally {
      placementDriver = null;
    }
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  private void closeAfterLocalMemberRemoval() {
    if (!localRemovalShutdownStarted.compareAndSet(false, true)) {
      return;
    }
    Thread.ofPlatform()
        .name("placement-driver-removed-context-shutdown")
        .start(
            () -> {
              LOG.info(
                  "placement-driver member {} was removed; closing application context",
                  configuration.nodeId());
              try {
                applicationContext.close();
                LOG.info(
                    "application context closed for removed placement-driver member {}",
                    configuration.nodeId());
              } catch (final RuntimeException error) {
                LOG.error(
                    "failed to close application context for removed placement-driver member {}",
                    configuration.nodeId(),
                    error);
              }
            });
  }

  /** The bound business gRPC port (resolved after startup; used by tests). */
  public int grpcPort() {
    final PlacementDriver driver = placementDriver;
    if (driver == null) {
      throw new IllegalStateException("placement-driver runtime is not running");
    }
    return driver.grpcPort();
  }
}
