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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

final class PlacementDriverArchitectureBaselineTest {
  private static final Path MAIN_SOURCE = Path.of("src/main/java");
  private static final List<String> REMOVED_BOUNDARIES =
      List.of(
          "io/taskite/placementdriver/ClusterServices.java",
          "io/taskite/placementdriver/api/LegacyPlacementDriverFacade.java",
          "io/taskite/placementdriver/forwarding/PlacementDriverHttpForwarder.java",
          "io/taskite/placementdriver/forwarding/PlacementDriverLeaderForwardingFilter.java",
          "io/taskite/placementdriver/server/PlacementDriverApiService.java",
          "io/taskite/placementdriver/TopologyAdminPlanner.java",
          "io/taskite/placementdriver/TopologyScalePlanner.java",
          "io/taskite/placementdriver/TopologyRecoveryPlanner.java",
          "io/taskite/placementdriver/VoterRoster.java",
          "io/taskite/placementdriver/processing/placementdriver/PlacementDriverMembersQueryBehavior.java",
          "io/taskite/placementdriver/processing/broker/registry/BrokerRegistryQueryBehavior.java",
          "io/taskite/placementdriver/processing/broker/cluster/BrokerClusterChangeBatchQueryBehavior.java");

  @Test
  void removedBoundariesCannotBeRestored() {
    for (final String removedBoundary : REMOVED_BOUNDARIES) {
      assertTrue(
          Files.notExists(MAIN_SOURCE.resolve(removedBoundary)),
          () -> removedBoundary + " must remain deleted");
    }
  }

  @Test
  void legacyRootPackagesCannotBeRestored() {
    assertTrue(Files.notExists(MAIN_SOURCE.resolve("io/taskite/meta")));
    assertTrue(Files.notExists(MAIN_SOURCE.resolve("io/taskite/clustermeta")));
  }

  @Test
  void endpointManagerCannotBypassHealthQueryProcessor() throws IOException {
    final String endpointManager =
        Files.readString(
            MAIN_SOURCE.resolve(
                "io/taskite/placementdriver/api/PlacementDriverEndpointManager.java"));

    assertTrue(!endpointManager.contains("PlacementDriverClusterViewPort"));
    assertTrue(!endpointManager.contains("currentPlacementDriverClusterView"));
  }

  @Test
  void publicGrpcApiUsesGeneratedProtocolAndNotClientTransport() throws IOException {
    final String server =
        Files.readString(
            MAIN_SOURCE.resolve(
                "io/taskite/placementdriver/api/grpc/PlacementDriverGrpcServer.java"));
    final String endpointManager =
        Files.readString(
            MAIN_SOURCE.resolve(
                "io/taskite/placementdriver/api/PlacementDriverEndpointManager.java"));

    assertTrue(server.contains("PlacementDriverGrpcApi"));
    assertTrue(!server.contains("PlacementDriverRegistryApi"));
    assertTrue(!endpointManager.contains("placementdriver.client.request"));
    assertTrue(!endpointManager.contains("placementdriver.client.transport"));
    assertTrue(!endpointManager.contains("RequestManager"));
  }

  @Test
  void adminEndpointsMustKeepVersionedDomainPrefixes() throws IOException {
    final String controller =
        Files.readString(
            MAIN_SOURCE.resolve(
                "io/taskite/placementdriver/api/rest/PlacementDriverAdminController.java"));

    assertTrue(controller.contains("@RequestMapping(\"/api/v1\")"));
    assertTrue(controller.contains("/pd/health"));
    assertTrue(controller.contains("/pd/topology"));
    assertTrue(controller.contains("/broker/clusters"));
    assertTrue(controller.contains("/{clusterId}/changes/active"));
    assertTrue(controller.contains("/{clusterId}/changes/{changeId}"));
    assertTrue(!controller.contains("@GetMapping(\"/health\")"));
    assertTrue(!controller.contains("@PostMapping(\"/clusters\")"));
  }

  @Test
  void eventAppliersCannotReadWallClock() throws IOException {
    final Path appliers = MAIN_SOURCE.resolve("io/taskite/placementdriver/state/appliers");
    try (Stream<Path> sources = Files.walk(appliers)) {
      for (final Path source :
          sources.filter(PlacementDriverArchitectureBaselineTest::isJava).toList()) {
        final String content = Files.readString(source);
        assertTrue(!content.contains("Instant::now"), () -> relativeSource(source));
        assertTrue(!content.contains("Instant.now()"), () -> relativeSource(source));
        assertTrue(!content.contains("System.currentTimeMillis()"), () -> relativeSource(source));
      }
    }
  }

  @Test
  void runtimeCompositionCannotInterpretCommittedIntents() throws IOException {
    final String runtime =
        Files.readString(
            MAIN_SOURCE.resolve(
                "io/taskite/placementdriver/runtime/PlacementDriverRuntimeComponents.java"));

    assertTrue(!runtime.contains("onCommittedRecord("));
    assertTrue(!runtime.contains("BrokerIntent"));
    assertTrue(!runtime.contains("BrokerPartitionChangeIntent"));
    assertTrue(!runtime.contains("CommandDistributionIntent"));
  }

  @Test
  void leadershipListenerCannotPublishBeforeTopologyIsCommitted() throws IOException {
    final String listener =
        Files.readString(
            MAIN_SOURCE.resolve(
                "io/taskite/placementdriver/runtime/PlacementDriverClusterLeadershipListener.java"));

    assertTrue(!listener.contains("TopologyWatchPublisher"));
    assertTrue(!listener.contains("publishPlacementDriverClusterTopology"));
  }

  @Test
  void targetApiAndRuntimePackagesRespectDependencyDirection() throws IOException {
    try (Stream<Path> sources = Files.walk(MAIN_SOURCE)) {
      for (final Path source :
          sources.filter(PlacementDriverArchitectureBaselineTest::isJava).toList()) {
        final String relative = relativeSource(source);
        final String content = Files.readString(source);
        assertNoImport(relative, content, "io.taskite.placementdriver.client");
        if (relative.contains("/api/")) {
          assertNoImport(relative, content, ".state.");
          assertNoImport(relative, content, ".processing.");
          assertNoImport(relative, content, ".state.appliers.");
        }
        if (relative.contains("/broker/") && isRuntimeComponent(relative)) {
          assertNoImport(relative, content, ".state.mutable.");
          assertNoImport(relative, content, ".state.appliers.");
          assertNoImport(relative, content, ".processing.");
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

  private static boolean isRuntimeComponent(final String source) {
    return source.endsWith("Actor.java")
        || source.endsWith("Dispatcher.java")
        || source.endsWith("Publisher.java")
        || source.endsWith("Listener.java")
        || source.endsWith("Observer.java");
  }

  private static boolean isJava(final Path path) {
    return Files.isRegularFile(path) && path.getFileName().toString().endsWith(".java");
  }

  private static String relativeSource(final Path source) {
    return MAIN_SOURCE.relativize(source).toString().replace('\\', '/');
  }
}
