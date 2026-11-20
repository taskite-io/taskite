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
package io.taskite.broker;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

final class BrokerArchitectureBaselineTest {
  private static final Path MAIN_SOURCE = Path.of("src/main/java");
  private static final Path BROKER_SOURCE = MAIN_SOURCE.resolve("io/taskite/broker");
  private static final List<String> REMOVED_BOUNDARIES =
      List.of(
          "runtime/bootstrap",
          "server",
          "runtime/MultiRaftGroupManager.java",
          "runtime/PartitionChangeExecutor.java",
          "runtime/JRaftPartitionChangeExecutor.java",
          "runtime/TopologyOperationManifestStore.java",
          "runtime/Broker.java",
          "runtime/BrokerConfig.java");

  @Test
  void legacyBoundariesCannotBeRestored() {
    for (final String removedBoundary : REMOVED_BOUNDARIES) {
      assertTrue(
          Files.notExists(BROKER_SOURCE.resolve(removedBoundary)),
          () -> removedBoundary + " must remain deleted");
    }
  }

  @Test
  void grpcAdapterDelegatesOnlyToEndpointManager() throws IOException {
    final String api = Files.readString(BROKER_SOURCE.resolve("api/grpc/BrokerRequestApi.java"));

    assertTrue(api.contains("BrokerEndpointManager"));
    assertTrue(!api.contains("BrokerPartitionChangeExecutor"));
    assertTrue(!api.contains("CommandRequestHandler"));
    assertTrue(!api.contains("BrokerRuntime"));
  }

  @Test
  void startupStepsAreFixedAndHealthCheckIsLast() throws IOException {
    final String startupSteps =
        Files.readString(BROKER_SOURCE.resolve("bootstrap/BrokerStartupSteps.java"));

    assertTrue(startupSteps.contains("return List.of("));
    assertTrue(!startupSteps.contains("ClusterTopologyManagerStep"));
    assertTrue(
        startupSteps.lastIndexOf("BrokerHealthCheckStep")
            > startupSteps.lastIndexOf("BrokerRegistrationStep"));
  }

  @Test
  void bootstrapContainsNoBusinessRuntimeComponents() throws IOException {
    try (Stream<Path> sources = Files.walk(BROKER_SOURCE.resolve("bootstrap"))) {
      for (final Path source : sources.filter(BrokerArchitectureBaselineTest::isJava).toList()) {
        final String name = source.getFileName().toString();
        assertTrue(!name.endsWith("Actor.java"), name);
        assertTrue(!name.endsWith("Manager.java"), name);
        assertTrue(!name.endsWith("Store.java"), name);
      }
    }
  }

  @Test
  void targetPackagesRespectDependencyDirection() throws IOException {
    try (Stream<Path> sources = Files.walk(BROKER_SOURCE)) {
      for (final Path source : sources.filter(BrokerArchitectureBaselineTest::isJava).toList()) {
        final String relative = relativeSource(source);
        final String content = Files.readString(source);
        if (relative.contains("/api/")) {
          assertNoImport(relative, content, ".bootstrap.");
          assertNoImport(relative, content, ".runtime.");
          assertNoImport(relative, content, ".state.");
          assertNoImport(relative, content, ".registry.");
          assertNoImport(relative, content, ".topology.");
        }
        if (relative.endsWith("Actor.java")) {
          assertNoImport(relative, content, ".bootstrap.");
          assertNoImport(relative, content, ".api.");
          assertNoImport(relative, content, ".state.");
        }
        if (relative.contains("/state/")) {
          assertNoImport(relative, content, ".api.");
          assertNoImport(relative, content, ".bootstrap.");
          assertNoImport(relative, content, ".runtime.");
          assertNoImport(relative, content, ".registry.");
          assertNoImport(relative, content, ".topology.");
        }
      }
    }
  }

  @Test
  void responsibilityClassesRemainWithinSizeLimit() throws IOException {
    try (Stream<Path> sources = Files.walk(BROKER_SOURCE)) {
      for (final Path source : sources.filter(BrokerArchitectureBaselineTest::isJava).toList()) {
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

  private static void assertNoImport(
      final String relative, final String content, final String forbiddenPackage) {
    assertTrue(
        content
            .lines()
            .filter(line -> line.startsWith("import "))
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
