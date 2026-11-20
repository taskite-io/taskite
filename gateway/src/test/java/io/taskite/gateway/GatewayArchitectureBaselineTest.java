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
package io.taskite.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

final class GatewayArchitectureBaselineTest {
  private static final Path MAIN_SOURCE = Path.of("src/main/java");
  private static final Path GATEWAY_SOURCE = MAIN_SOURCE.resolve("io/taskite/gateway");
  private static final List<String> REMOVED_ROOT_CLASSES =
      List.of(
          "EndpointManager.java",
          "GatewayGrpcService.java",
          "RequestMapper.java",
          "ResponseMapper.java",
          "TopologyCache.java",
          "TopologyRefresher.java");

  @Test
  void rootPackageContainsOnlyPublicLifecycleTypes() throws IOException {
    try (Stream<Path> sources = Files.list(GATEWAY_SOURCE)) {
      final Set<String> rootTypes =
          sources
              .filter(GatewayArchitectureBaselineTest::isJava)
              .map(path -> path.getFileName().toString())
              .collect(Collectors.toSet());
      assertEquals(Set.of("Gateway.java", "GatewayConfig.java"), rootTypes);
    }
    for (final String removedClass : REMOVED_ROOT_CLASSES) {
      assertTrue(Files.notExists(GATEWAY_SOURCE.resolve(removedClass)), removedClass);
    }
  }

  @Test
  void grpcAdapterDelegatesOnlyToEndpointManager() throws IOException {
    final String api = Files.readString(GATEWAY_SOURCE.resolve("api/grpc/GatewayCommandApi.java"));

    assertTrue(api.contains("GatewayEndpointManager"));
    assertTrue(!api.contains("GatewayBrokerCommandDispatcher"));
    assertTrue(!api.contains("GatewayBrokerRouter"));
    assertTrue(!api.contains("Client"));
  }

  @Test
  void startupStepsAreFixedInDependencyOrder() throws IOException {
    final String startupSteps =
        Files.readString(GATEWAY_SOURCE.resolve("bootstrap/GatewayStartupSteps.java"));

    assertTrue(startupSteps.contains("return List.of("));
    assertTrue(
        startupSteps.indexOf("GatewayClientStep") < startupSteps.indexOf("GatewayRoutingStep"));
    assertTrue(
        startupSteps.indexOf("GatewayRoutingStep")
            < startupSteps.indexOf("GatewayInterceptorStep"));
    assertTrue(
        startupSteps.indexOf("GatewayInterceptorStep") < startupSteps.indexOf("GatewayApiStep"));
  }

  @Test
  void topologyActorDoesNotOwnSharedPlacementDriverClient() throws IOException {
    final String actor =
        Files.readString(GATEWAY_SOURCE.resolve("routing/GatewayTopologyActor.java"));

    assertTrue(!actor.contains("placementDriver.close()"));
  }

  @Test
  void startupContextClosesResourcesInReverseOrder() throws IOException {
    final String context =
        Files.readString(GATEWAY_SOURCE.resolve("bootstrap/GatewayStartupContext.java"));

    assertTrue(context.contains("closeOrder.push("));
    assertTrue(context.contains("closeOrder.pop()"));
  }

  @Test
  void targetPackagesRespectDependencyDirection() throws IOException {
    try (Stream<Path> sources = Files.walk(GATEWAY_SOURCE)) {
      for (final Path source : sources.filter(GatewayArchitectureBaselineTest::isJava).toList()) {
        final String relative = relativeSource(source);
        final String content = Files.readString(source);
        if (relative.contains("/api/")) {
          assertNoGatewayImport(relative, content, ".bootstrap.");
          assertNoGatewayImport(relative, content, ".routing.");
          assertNoGatewayImport(relative, content, ".runtime.");
        }
        if (relative.contains("/routing/")) {
          assertNoGatewayImport(relative, content, ".api.");
          assertNoGatewayImport(relative, content, ".bootstrap.");
          assertNoGatewayImport(relative, content, ".runtime.");
        }
        if (relative.contains("/broker/")) {
          assertNoGatewayImport(relative, content, ".api.");
          assertNoGatewayImport(relative, content, ".bootstrap.");
          assertNoGatewayImport(relative, content, ".runtime.");
        }
      }
    }
  }

  @Test
  void responsibilityClassesRemainWithinSizeLimit() throws IOException {
    try (Stream<Path> sources = Files.walk(GATEWAY_SOURCE)) {
      for (final Path source : sources.filter(GatewayArchitectureBaselineTest::isJava).toList()) {
        final String name = source.getFileName().toString();
        if (name.endsWith("Actor.java")
            || name.endsWith("Manager.java")
            || name.endsWith("Step.java")) {
          try (Stream<String> lines = Files.lines(source)) {
            assertTrue(lines.count() <= 500, () -> relativeSource(source) + " exceeds 500 lines");
          }
        }
      }
    }
  }

  private static void assertNoGatewayImport(
      final String relative, final String content, final String forbiddenPackage) {
    assertTrue(
        content
            .lines()
            .filter(line -> line.startsWith("import io.taskite.gateway."))
            .noneMatch(line -> line.contains(forbiddenPackage)),
        () -> relative + " must not import " + forbiddenPackage);
  }

  private static boolean isJava(final Path path) {
    return Files.isRegularFile(path) && path.getFileName().toString().endsWith(".java");
  }

  private static String relativeSource(final Path source) {
    return MAIN_SOURCE.relativize(source).toString().replace('\\', '/');
  }
}
