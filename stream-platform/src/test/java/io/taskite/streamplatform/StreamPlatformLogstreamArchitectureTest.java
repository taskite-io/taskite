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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

final class StreamPlatformLogstreamArchitectureTest {
  private static final Path PROJECT_ROOT = Path.of("..").toAbsolutePath().normalize();
  private static final Path MODULE_SOURCE = Path.of("src/main/java");
  private static final Path LOGSTREAM =
      MODULE_SOURCE.resolve("io/taskite/streamplatform/logstream");
  private static final String LOGSTREAM_IMPORT = "import io.taskite.streamplatform.logstream.";
  private static final String LOGGED_EVENT_IMPORT =
      "import io.taskite.streamplatform.logstream.LoggedEvent;";
  private static final Path ENGINE_TEST_RUNTIME =
      PROJECT_ROOT.resolve("engine/src/test/java/io/taskite/engine/util/runtime");
  // The engine fixture implements the reader/appender ports exposed by StreamProcessorContext.Log.
  private static final Set<String> ENGINE_TEST_RUNTIME_IMPORTS =
      Set.of(
          LOGGED_EVENT_IMPORT,
          "import io.taskite.streamplatform.logstream.LogAppendEntry;",
          "import io.taskite.streamplatform.logstream.LogStreamReader;");

  @Test
  void removedMavenModuleStaysRemoved() throws IOException {
    assertTrue(Files.notExists(PROJECT_ROOT.resolve("logstream/pom.xml")));
    final String rootPom = Files.readString(PROJECT_ROOT.resolve("pom.xml"));
    assertFalse(rootPom.contains("<module>logstream</module>"));
    assertFalse(rootPom.contains("<artifactId>logstream</artifactId>"));
  }

  @Test
  void legacyPackagesStayRemoved() {
    assertTrue(Files.notExists(MODULE_SOURCE.resolve("io/taskite/logstream/log")));
    assertTrue(
        Files.notExists(PROJECT_ROOT.resolve("db/src/main/java/io/taskite/logstream/storage")));
  }

  @Test
  void logstreamDoesNotDependOnHigherPlatformLayers() throws IOException {
    assertPackageHasNoImport("io.taskite.streamplatform.command");
    assertPackageHasNoImport("io.taskite.streamplatform.processing");
    assertPackageHasNoImport("io.taskite.streamplatform.runtime");
  }

  @Test
  void logstreamUsesOnlyStableClusterPorts() throws IOException {
    final Set<String> allowed =
        Set.of(
            "import io.taskite.cluster.raft.RaftPartition;",
            "import io.taskite.cluster.raft.log.PersistedRaftRecord;",
            "import io.taskite.cluster.raft.log.RaftJournalReader;");
    try (Stream<Path> sources = Files.walk(LOGSTREAM)) {
      for (final Path source :
          sources.filter(StreamPlatformLogstreamArchitectureTest::isJava).toList()) {
        for (final String line : Files.readAllLines(source)) {
          if (line.startsWith("import io.taskite.cluster.")) {
            assertTrue(
                allowed.contains(line),
                () -> MODULE_SOURCE.relativize(source) + " imports unstable cluster type " + line);
          }
        }
      }
    }
  }

  @Test
  void externalModulesOnlyImportAllowedLogstreamTypes() throws IOException {
    for (final String module : Set.of("engine", "broker", "placement-driver")) {
      assertAllowedLogstreamImports(PROJECT_ROOT.resolve(module).resolve("src/main"));
      assertAllowedLogstreamImports(PROJECT_ROOT.resolve(module).resolve("src/test"));
    }
  }

  @Test
  void apiOnlyImportsLoggedEventFromLogstream() throws IOException {
    assertAllowedLogstreamImports(MODULE_SOURCE.resolve("io/taskite/streamplatform/api"));
  }

  @Test
  void codecsAndStorageAdaptersStayInternal() throws IOException {
    assertNotPublic("BatchPayloadCodec.java");
    assertNotPublic("LogEntryDescriptor.java");
    assertNotPublic("LogStorage.java");
    assertNotPublic("RaftLogStorage.java");
    assertNotPublic("RaftLogStreamReader.java");
    assertNotPublic("Sequencer.java");
  }

  private static void assertPackageHasNoImport(final String forbidden) throws IOException {
    try (Stream<Path> sources = Files.walk(LOGSTREAM)) {
      for (final Path source :
          sources.filter(StreamPlatformLogstreamArchitectureTest::isJava).toList()) {
        final String content = Files.readString(source);
        assertTrue(
            content
                .lines()
                .filter(line -> line.startsWith("import "))
                .noneMatch(line -> line.contains(forbidden)),
            () -> MODULE_SOURCE.relativize(source) + " must not import " + forbidden);
      }
    }
  }

  private static void assertAllowedLogstreamImports(final Path sourceRoot) throws IOException {
    if (Files.notExists(sourceRoot)) {
      return;
    }
    try (Stream<Path> sources = Files.walk(sourceRoot)) {
      for (final Path source :
          sources.filter(StreamPlatformLogstreamArchitectureTest::isJava).toList()) {
        final Set<String> allowed =
            source.toAbsolutePath().normalize().startsWith(ENGINE_TEST_RUNTIME)
                ? ENGINE_TEST_RUNTIME_IMPORTS
                : Set.of(LOGGED_EVENT_IMPORT);
        for (final String line : Files.readAllLines(source)) {
          if (line.startsWith(LOGSTREAM_IMPORT)) {
            assertTrue(
                allowed.contains(line),
                () -> source + " must not import Stream Platform logstream type " + line);
          }
        }
      }
    }
  }

  private static void assertNotPublic(final String fileName) throws IOException {
    final String content = Files.readString(LOGSTREAM.resolve(fileName));
    assertFalse(
        content.contains("public class ")
            || content.contains("public final class ")
            || content.contains("public interface "),
        () -> fileName + " must remain package-private");
  }

  private static boolean isJava(final Path path) {
    return Files.isRegularFile(path) && path.getFileName().toString().endsWith(".java");
  }
}
