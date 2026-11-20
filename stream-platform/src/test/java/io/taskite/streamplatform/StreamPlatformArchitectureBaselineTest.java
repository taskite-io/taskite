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
package io.taskite.streamplatform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

final class StreamPlatformArchitectureBaselineTest {
  private static final Path MAIN_SOURCE = Path.of("src/main/java");
  private static final Path ROOT = MAIN_SOURCE.resolve("io/taskite/streamplatform");

  @Test
  void rootPackageContainsNoJavaTypes() throws IOException {
    try (Stream<Path> files = Files.list(ROOT)) {
      assertTrue(files.noneMatch(StreamPlatformArchitectureBaselineTest::isJava));
    }
  }

  @Test
  void topLevelPackagesAreFixed() throws IOException {
    try (Stream<Path> files = Files.list(ROOT)) {
      final Set<String> directories =
          files
              .filter(Files::isDirectory)
              .map(path -> path.getFileName().toString())
              .collect(Collectors.toSet());
      assertEquals(
          Set.of(
              "api",
              "command",
              "logstream",
              "processing",
              "record",
              "runtime",
              "scheduler",
              "state"),
          directories);
    }
  }

  @Test
  void removedBoundariesStayRemoved() {
    assertTrue(Files.notExists(ROOT.resolve("processing/streamprocessor")));
    assertTrue(Files.notExists(ROOT.resolve("runtime/partitions")));
    assertTrue(Files.notExists(ROOT.resolve("runtime/command")));
    assertTrue(Files.notExists(ROOT.resolve("api/state/key")));
    assertTrue(Files.notExists(ROOT.resolve("RecordValues.java")));
    assertTrue(Files.notExists(ROOT.resolve("RecordValueRegistry.java")));
    assertTrue(Files.notExists(ROOT.resolve("EmptyRecordValue.java")));
    assertTrue(Files.notExists(ROOT.resolve("api/processor/RecordProcessorMap.java")));
    assertTrue(Files.notExists(ROOT.resolve("processing/CommittedRecordObserver.java")));
    assertTrue(Files.notExists(ROOT.resolve("logstream/LogStreamWriter.java")));
    assertTrue(Files.notExists(ROOT.resolve("logstream/RaftLogStream.java")));
    assertTrue(Files.notExists(ROOT.resolve("runtime/transition/PartitionTransitionProcess.java")));
  }

  @Test
  void packagesRespectDependencyDirection() throws IOException {
    assertPackageHasNoImport(ROOT.resolve("api"), "io.taskite.streamplatform.command");
    assertPackageHasNoImport(ROOT.resolve("api"), "io.taskite.streamplatform.processing");
    assertPackageHasNoImport(ROOT.resolve("api"), "io.taskite.streamplatform.runtime");
    assertPackageHasNoImport(ROOT.resolve("api"), "com.alipay.sofa.jraft");
    assertPackageHasNoImport(ROOT.resolve("command"), "io.taskite.cluster");
    assertPackageHasNoImport(ROOT.resolve("command"), "io.taskite.streamplatform.runtime");
    assertPackageHasNoImport(ROOT.resolve("scheduler"), "io.taskite.streamplatform.processing");
    assertPackageHasNoImport(ROOT.resolve("scheduler"), "io.taskite.streamplatform.runtime");
    assertPackageHasNoImport(ROOT.resolve("scheduler"), "io.taskite.placementdriver");
    assertPackageHasNoImport(ROOT.resolve("scheduler"), "io.taskite.engine");
    assertPackageHasNoImport(ROOT.resolve("processing"), "io.taskite.cluster");
    assertPackageHasNoImport(ROOT.resolve("processing"), "io.taskite.streamplatform.runtime");
    assertPackageHasNoImport(ROOT.resolve("logstream"), "io.taskite.streamplatform.command");
    assertPackageHasNoImport(ROOT.resolve("logstream"), "io.taskite.streamplatform.processing");
    assertPackageHasNoImport(ROOT.resolve("logstream"), "io.taskite.streamplatform.runtime");
  }

  @Test
  void facadeAndResponsibilityClassesStayBounded() throws IOException {
    assertLineLimit(ROOT.resolve("runtime/StreamPartition.java"), 300);
    assertLineLimit(ROOT.resolve("runtime/StreamPartitionFactory.java"), 500);
    assertLineLimit(ROOT.resolve("logstream/LogStream.java"), 500);
    assertLineLimit(ROOT.resolve("logstream/Sequencer.java"), 500);
    try (Stream<Path> sources = Files.walk(ROOT)) {
      for (final Path source :
          sources.filter(StreamPlatformArchitectureBaselineTest::isJava).toList()) {
        final String name = source.getFileName().toString();
        if (name.endsWith("StateMachine.java")
            || name.endsWith("Dispatcher.java")
            || name.endsWith("Controller.java")
            || name.equals("PartitionTransition.java")) {
          assertLineLimit(source, 500);
        }
      }
    }
  }

  private static void assertPackageHasNoImport(final Path root, final String forbidden)
      throws IOException {
    try (Stream<Path> sources = Files.walk(root)) {
      for (final Path source :
          sources.filter(StreamPlatformArchitectureBaselineTest::isJava).toList()) {
        final String content = Files.readString(source);
        assertTrue(
            content
                .lines()
                .filter(line -> line.startsWith("import "))
                .noneMatch(line -> line.contains(forbidden)),
            () -> MAIN_SOURCE.relativize(source) + " must not import " + forbidden);
      }
    }
  }

  private static void assertLineLimit(final Path source, final long limit) throws IOException {
    try (Stream<String> lines = Files.lines(source)) {
      assertTrue(
          lines.count() <= limit,
          () -> MAIN_SOURCE.relativize(source) + " exceeds " + limit + " lines");
    }
  }

  private static boolean isJava(final Path path) {
    return Files.isRegularFile(path) && path.getFileName().toString().endsWith(".java");
  }
}
