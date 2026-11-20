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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.micrometer.core.instrument.MeterRegistry;
import io.taskite.placementdriver.PlacementDriver;
import io.taskite.placementdriver.PlacementDriverConfiguration;
import io.taskite.placementdriver.api.PlacementDriverEndpointManager;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.context.ConfigurableApplicationContext;

final class PlacementDriverSpringLifecycleTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("unchecked")
  void startupTimeoutStopsDriverAndPreservesCleanupFailure(final boolean closeFails) {
    final var timeout = new IllegalStateException("startup timeout");
    final var closeError = new IllegalStateException("cleanup failed");
    final ActorFuture<PlacementDriver> started = mock(ActorFuture.class);
    when(started.join(60, TimeUnit.SECONDS)).thenThrow(timeout);
    try (var drivers =
        mockConstruction(
            PlacementDriver.class,
            (driver, context) -> {
              when(driver.start()).thenReturn(started);
              when(driver.stop())
                  .thenReturn(
                      closeFails
                          ? CompletableActorFuture.completedExceptionally(closeError)
                          : CompletableActorFuture.completed(driver));
            })) {
      final var lifecycle =
          new PlacementDriverSpringLifecycle(
              mock(PlacementDriverConfiguration.class),
              new ServerProperties(),
              new PlacementDriverEndpointManager(),
              mock(ConfigurableApplicationContext.class),
              mock(MeterRegistry.class));
      final var failure = assertThrows(IllegalStateException.class, lifecycle::start);
      assertSame(timeout, failure.getCause());
      assertFalse(lifecycle.isRunning());
      final var driver = drivers.constructed().getFirst();
      verify(driver).stop();
      assertEquals(closeFails ? 1 : 0, timeout.getSuppressed().length);
      if (closeFails) {
        assertTrue(timeout.getSuppressed()[0].toString().contains("cleanup failed"));
      }
      // The lifecycle retains ownership so Spring's subsequent stop still reaches the driver.
      lifecycle.stop();
      verify(driver, times(2)).stop();
    }
  }
}
