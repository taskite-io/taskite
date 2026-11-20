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
package io.taskite.broker.partition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Q7: pre-open fenced-recovery generation guard. */
final class BrokerPartitionRecoveryGuardTest {
  @TempDir Path tempDir;

  @Test
  void unmarkedPartitionReportsEpochZero() {
    final Path partitionDir = tempDir.resolve("partitions").resolve("0");
    assertEquals(0L, BrokerPartitionRecoveryGuard.localEpoch(partitionDir));
  }

  @Test
  void recordThenReadRoundTrips() {
    final Path partitionDir = tempDir.resolve("partitions").resolve("0");
    BrokerPartitionRecoveryGuard.recordEpoch(partitionDir, 7L);
    assertEquals(7L, BrokerPartitionRecoveryGuard.localEpoch(partitionDir));
  }

  @Test
  void recordIgnoresNonPositiveEpoch() {
    final Path partitionDir = tempDir.resolve("partitions").resolve("0");
    BrokerPartitionRecoveryGuard.recordEpoch(partitionDir, 0L);
    BrokerPartitionRecoveryGuard.recordEpoch(partitionDir, -3L);
    assertEquals(0L, BrokerPartitionRecoveryGuard.localEpoch(partitionDir));
  }

  @Test
  void staleDataDirectoryIsQuarantinedAndEpochAdvances() throws IOException {
    final Path partitionDir = tempDir.resolve("partitions").resolve("0");
    Files.createDirectories(partitionDir.resolve("log"));
    Files.writeString(partitionDir.resolve("log").resolve("meta"), "old-generation");

    final Path opened = BrokerPartitionRecoveryGuard.quarantineIfStale(partitionDir, 5L);

    assertEquals(partitionDir, opened);
    // The returning node's superseded state is moved aside, never reused, and never deleted.
    assertFalse(Files.exists(partitionDir), "stale data directory must be quarantined");
    assertEquals(5L, BrokerPartitionRecoveryGuard.localEpoch(partitionDir));
    try (Stream<Path> siblings = Files.list(partitionDir.getParent())) {
      final List<Path> quarantined =
          siblings.filter(p -> p.getFileName().toString().contains("__quarantine-")).toList();
      assertEquals(1, quarantined.size());
      assertTrue(Files.isDirectory(quarantined.get(0)));
    }
  }

  @Test
  void alreadyAtOrAboveTargetEpochIsNotQuarantined() throws IOException {
    final Path partitionDir = tempDir.resolve("partitions").resolve("0");
    BrokerPartitionRecoveryGuard.recordEpoch(partitionDir, 5L);
    Files.createDirectories(partitionDir);
    Files.writeString(partitionDir.resolve("keep"), "current-generation");

    // A replayed/older reset epoch must not disturb the current generation (monotonic guard).
    BrokerPartitionRecoveryGuard.quarantineIfStale(partitionDir, 5L);
    BrokerPartitionRecoveryGuard.quarantineIfStale(partitionDir, 3L);

    assertTrue(Files.exists(partitionDir.resolve("keep")), "current generation must be preserved");
    assertEquals(
        "current-generation",
        Files.readString(partitionDir.resolve("keep"), StandardCharsets.UTF_8));
    assertEquals(5L, BrokerPartitionRecoveryGuard.localEpoch(partitionDir));
  }

  @Test
  void zeroTargetEpochNeverQuarantines() throws IOException {
    final Path partitionDir = tempDir.resolve("partitions").resolve("0");
    Files.createDirectories(partitionDir);
    Files.writeString(partitionDir.resolve("keep"), "no-recovery-ever");

    final Path opened = BrokerPartitionRecoveryGuard.quarantineIfStale(partitionDir, 0L);

    assertEquals(partitionDir, opened);
    assertTrue(Files.exists(partitionDir.resolve("keep")));
    assertEquals(0L, BrokerPartitionRecoveryGuard.localEpoch(partitionDir));
  }
}
