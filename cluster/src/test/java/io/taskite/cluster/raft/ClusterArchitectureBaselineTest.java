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
package io.taskite.cluster.raft;

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

final class ClusterArchitectureBaselineTest {
  private static final Path MAIN_SOURCE = Path.of("src/main/java");
  private static final Path TASKITE_SOURCE = MAIN_SOURCE.resolve("io/taskite");
  private static final Path RAFT_SOURCE = TASKITE_SOURCE.resolve("cluster/raft");
  private static final List<String> REMOVED_BOUNDARIES =
      List.of(
          "PartitionGroupDescriptor.java",
          "PartitionGroupFactory.java",
          "PartitionGroupManager.java",
          "PartitionGroupReusePolicy.java",
          "RaftPartitionApi.java",
          "jraft/JRaftServer.java",
          "jraft/PartitionStateMachine.java",
          "jraft/PeerIdMapping.java",
          "jraft/PositionSpanCodec.java",
          "jraft/PartitionStorageSnapshotBackend.java");

  @Test
  void rootRaftPackageContainsOnlyEngineAgnosticApi() throws IOException {
    try (Stream<Path> sources = Files.list(RAFT_SOURCE)) {
      final Set<String> rootTypes =
          sources
              .filter(ClusterArchitectureBaselineTest::isJava)
              .map(path -> path.getFileName().toString())
              .collect(Collectors.toSet());
      assertEquals(
          Set.of(
              "RaftMember.java",
              "RaftNodeState.java",
              "RaftPartition.java",
              "RaftPartitionConfig.java",
              "RaftPeer.java",
              "RaftRole.java"),
          rootTypes);
    }
  }

  @Test
  void legacyBoundariesCannotBeRestored() {
    assertTrue(Files.notExists(TASKITE_SOURCE.resolve("journal")));
    assertTrue(Files.notExists(TASKITE_SOURCE.resolve("log")));
    for (final String removedBoundary : REMOVED_BOUNDARIES) {
      assertTrue(
          Files.notExists(RAFT_SOURCE.resolve(removedBoundary)),
          () -> removedBoundary + " must remain deleted");
    }
  }

  @Test
  void genericApiGroupAndLogRemainEngineAgnostic() throws IOException {
    assertDirectoryHasNoImport(RAFT_SOURCE, "com.alipay.sofa.jraft");
    assertPackageHasNoImport(RAFT_SOURCE.resolve("group"), "com.alipay.sofa.jraft");
    assertPackageHasNoImport(RAFT_SOURCE.resolve("log"), "com.alipay.sofa.jraft");
  }

  @Test
  void clusterDoesNotDependOnBusinessProtocol() throws IOException {
    final String pom = Files.readString(Path.of("pom.xml"));

    assertTrue(!pom.contains("<artifactId>protocol</artifactId>"));
    assertTrue(!pom.contains("<artifactId>agrona</artifactId>"));
  }

  @Test
  void jraftFacadeDelegatesMembershipOperations() throws IOException {
    final String facade = Files.readString(RAFT_SOURCE.resolve("jraft/JRaftPartition.java"));
    final String membership =
        Files.readString(RAFT_SOURCE.resolve("jraft/membership/JRaftMembershipManager.java"));

    assertTrue(facade.contains("JRaftMembershipManager"));
    assertTrue(!facade.contains(".changePeers("));
    assertTrue(!facade.contains(".removeLearners("));
    assertTrue(!facade.contains(".addLearners("));
    assertTrue(!facade.contains(".resetPeers("));
    assertTrue(!facade.contains(".transferLeadershipTo("));
    assertTrue(membership.contains(".changePeers("));
    assertTrue(membership.contains(".resetPeers("));
    assertTrue(membership.contains(".transferLeadershipTo("));
  }

  @Test
  void snapshotDomainRemainsOutsideTheJraftAdapter() throws IOException {
    final Path oldSnapshotPackage = RAFT_SOURCE.resolve("jraft/snapshot");
    if (Files.exists(oldSnapshotPackage)) {
      try (Stream<Path> sources = Files.walk(oldSnapshotPackage)) {
        assertTrue(sources.noneMatch(ClusterArchitectureBaselineTest::isJava));
      }
    }
    final Path snapshot = Path.of("../snapshot/src/main/java/io/taskite/snapshot");
    assertPackageHasNoImport(snapshot, "io.taskite.cluster.");
    assertPackageHasNoImport(snapshot, "com.alipay.sofa.jraft");
  }

  @Test
  void responsibilityClassesRemainWithinSizeLimit() throws IOException {
    try (Stream<Path> sources = Files.walk(RAFT_SOURCE)) {
      for (final Path source : sources.filter(ClusterArchitectureBaselineTest::isJava).toList()) {
        final String name = source.getFileName().toString();
        if (name.equals("JRaftPartition.java")
            || name.endsWith("Manager.java")
            || name.endsWith("StateMachine.java")
            || name.endsWith("Server.java")) {
          try (Stream<String> lines = Files.lines(source)) {
            assertTrue(lines.count() <= 500, () -> relativeSource(source) + " exceeds 500 lines");
          }
        }
      }
    }
  }

  private static void assertPackageHasNoImport(final Path root, final String forbidden)
      throws IOException {
    try (Stream<Path> sources = Files.walk(root)) {
      for (final Path source : sources.filter(ClusterArchitectureBaselineTest::isJava).toList()) {
        final String content = Files.readString(source);
        assertTrue(
            content
                .lines()
                .filter(line -> line.startsWith("import "))
                .noneMatch(line -> line.contains(forbidden)),
            () -> relativeSource(source) + " must not import " + forbidden);
      }
    }
  }

  private static void assertDirectoryHasNoImport(final Path root, final String forbidden)
      throws IOException {
    try (Stream<Path> sources = Files.list(root)) {
      for (final Path source : sources.filter(ClusterArchitectureBaselineTest::isJava).toList()) {
        final String content = Files.readString(source);
        assertTrue(
            content
                .lines()
                .filter(line -> line.startsWith("import "))
                .noneMatch(line -> line.contains(forbidden)),
            () -> relativeSource(source) + " must not import " + forbidden);
      }
    }
  }

  private static boolean isJava(final Path path) {
    return Files.isRegularFile(path) && path.getFileName().toString().endsWith(".java");
  }

  private static String relativeSource(final Path source) {
    return MAIN_SOURCE.relativize(source).toString().replace('\\', '/');
  }
}
