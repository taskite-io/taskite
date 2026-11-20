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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.PlacementDriver;
import io.taskite.placementdriver.PlacementDriverConfiguration;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class PlacementDriverStartupStepsTest {

  @Test
  void failedRaftStartupAndRepeatedStopsShareSafeCleanup(@TempDir final Path directory)
      throws Exception {
    try (var occupiedRaftPort = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
        var availableGrpcPort = new ServerSocket(0)) {
      final int grpcPort = availableGrpcPort.getLocalPort();
      availableGrpcPort.close();
      final var driver =
          new PlacementDriver(
              new PlacementDriverConfiguration(
                  "meta-0",
                  new PlacementDriverConfiguration.Network(
                      "127.0.0.1", grpcPort, occupiedRaftPort.getLocalPort()),
                  new PlacementDriverConfiguration.Consensus(
                      Duration.ofMillis(150), Duration.ofMillis(50)),
                  new PlacementDriverConfiguration.Storage(directory, null),
                  PlacementDriverConfiguration.DEFAULT_CHANGE_HISTORY_LIMIT,
                  null,
                  null,
                  null));
      try {
        final var failure =
            assertThrows(ExecutionException.class, () -> driver.start().get(15, TimeUnit.SECONDS));
        assertTrue(failure.getCause().getMessage().contains("Failed to bind"));
        final var stopped = driver.stop();
        assertSame(driver, stopped.get(10, TimeUnit.SECONDS));
        assertSame(stopped, driver.stop());
        assertSame(stopped, CompletableFuture.supplyAsync(driver::stop).get(10, TimeUnit.SECONDS));
      } finally {
        driver.stop().get(10, TimeUnit.SECONDS);
      }
    }
  }

  @Test
  void startupStepsKeepDependencyOrder() {
    final List<Class<?>> stepTypes =
        PlacementDriverStartupSteps.build().stream().map(Object::getClass).toList();

    assertEquals(
        List.of(
            PlacementDriverConfigurationStep.class,
            PlacementDriverProcessorStep.class,
            PlacementDriverTopologyManifestRestoreStep.class,
            PlacementDriverApiStep.class,
            PlacementDriverBootstrapMembershipStep.class,
            PlacementDriverRaftStep.class,
            PlacementDriverRuntimeComponentsStep.class,
            PlacementDriverHealthCheckStep.class),
        stepTypes);
  }
}
