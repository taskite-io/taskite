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
package io.taskite.placementdriver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import io.taskite.placementdriver.api.PlacementDriverEndpointManager;
import io.taskite.placementdriver.bootstrap.PlacementDriverStartupContext;
import io.taskite.scheduler.ActorScheduler;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

final class PlacementDriverTest {
  @ParameterizedTest(name = "{0}, external removal callback={1}")
  @MethodSource("shutdownScenarios")
  void preservesDataOnStopAndLocalMemberRemoval(
      final ShutdownScenario scenario,
      final boolean externalCallback,
      @TempDir final Path directory)
      throws Exception {
    final var dataDir = Files.createDirectories(directory.resolve("member"));
    final var marker = Files.writeString(dataDir.resolve("marker"), "keep");
    final var config = mock(PlacementDriverConfiguration.class);
    when(config.storage()).thenReturn(new PlacementDriverConfiguration.Storage(dataDir, null));
    final var scheduler = mock(ActorScheduler.class);
    final var control = mock(ConcurrencyControl.class);
    final var failure = new IllegalStateException("shutdown failed");
    when(scheduler.stop())
        .thenReturn(
            scenario == ShutdownScenario.SCHEDULER_FAILURE
                ? CompletableActorFuture.completedExceptionally(failure)
                : CompletableActorFuture.completed(null));
    doAnswer(
            invocation -> {
              final Runnable shutdown = invocation.getArgument(0);
              shutdown.run();
              return null;
            })
        .when(control)
        .run(any(Runnable.class));
    final List<Runnable> removalNotifications = new ArrayList<>();
    final var externalNotifications = new AtomicInteger();
    try (var contexts =
        mockConstruction(
            PlacementDriverStartupContext.class,
            (context, construction) -> {
              when(context.config()).thenReturn(config);
              when(context.schedulerStarted()).thenReturn(true);
              when(context.scheduler()).thenReturn(scheduler);
              when(context.concurrencyControl()).thenReturn(control);
              if (scenario == ShutdownScenario.RUNTIME_FAILURE) {
                doThrow(failure).when(context).close();
              }
              removalNotifications.add((Runnable) construction.arguments().get(3));
            })) {
      final var driver =
          externalCallback
              ? new PlacementDriver(
                  config,
                  0,
                  new PlacementDriverEndpointManager(),
                  externalNotifications::incrementAndGet)
              : new PlacementDriver(config);
      if (scenario != ShutdownScenario.NORMAL_STOP) {
        removalNotifications.getFirst().run();
      }
      final var stopped = driver.stop();
      if (scenario == ShutdownScenario.RUNTIME_FAILURE
          || scenario == ShutdownScenario.SCHEDULER_FAILURE) {
        final var error =
            assertThrows(ExecutionException.class, () -> stopped.get(5, TimeUnit.SECONDS));
        assertSame(failure, error.getCause());
      } else {
        assertSame(driver, stopped.get(5, TimeUnit.SECONDS));
      }
      assertSame(stopped, driver.stop());
      assertTrue(Files.exists(dataDir));
      assertEquals("keep", Files.readString(marker));
      assertEquals(
          externalCallback && scenario != ShutdownScenario.NORMAL_STOP ? 1 : 0,
          externalNotifications.get());
    }
  }

  private static Stream<Arguments> shutdownScenarios() {
    return Stream.of(ShutdownScenario.values())
        .flatMap(
            scenario -> Stream.of(Arguments.of(scenario, false), Arguments.of(scenario, true)));
  }

  private enum ShutdownScenario {
    NORMAL_STOP,
    REMOVED,
    RUNTIME_FAILURE,
    SCHEDULER_FAILURE
  }
}
