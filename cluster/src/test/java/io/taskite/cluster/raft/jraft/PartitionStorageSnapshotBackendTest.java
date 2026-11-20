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
package io.taskite.cluster.raft.jraft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.cluster.raft.jraft.runtime.JRaftStateMachine;
import io.taskite.snapshot.PartitionStorageSnapshotBackend;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 4 unit test for the snapshot state backend (migration decisions D4/D5): a RocksDB
 * checkpoint of the business state ({@link PartitionStorageSnapshotBackend#save}) round-trips back
 * into a second, independent store ({@link PartitionStorageSnapshotBackend#load}), and the {@code
 * processedPosition} cursor — which lives inside the business column family — is preserved without
 * any separate handling. This is the follower-install path jraft's {@code InstallSnapshot} drives.
 */
final class PartitionStorageSnapshotBackendTest {

  @Test
  void checkpointRoundTripsBusinessStateAndProcessedPosition(@TempDir final Path baseDir)
      throws Exception {
    final Path sourceDir = baseDir.resolve("source-runtime");
    final Path checkpointDir =
        baseDir.resolve("snapshot").resolve(JRaftStateMachine.CHECKPOINT_DIR);
    final Path targetDir = baseDir.resolve("target-runtime");

    // 1) A source store with a known processed position, then snapshot it.
    try (final PartitionStorage source = new PartitionStorage(sourceDir)) {
      final StateStoreProcessingStore sourceStore = new StateStoreProcessingStore(source);
      sourceStore.markAsProcessed(42);
      sourceStore.getCurrentTransaction().commit();
      assertEquals(42, sourceStore.getLastProcessedPosition());

      new PartitionStorageSnapshotBackend(source).save(checkpointDir);
    }

    // 2) A fresh, empty target store starts at position 0.
    try (final PartitionStorage target = new PartitionStorage(targetDir)) {
      final StateStoreProcessingStore before = new StateStoreProcessingStore(target);
      assertEquals(0, before.getLastProcessedPosition(), "fresh store must start at position 0");

      // 3) Load (import) the checkpoint in place, then the processed position must be recovered.
      new PartitionStorageSnapshotBackend(target).load(checkpointDir);
      final StateStoreProcessingStore after = new StateStoreProcessingStore(target);
      assertEquals(
          42,
          after.getLastProcessedPosition(),
          "processedPosition must round-trip through the checkpoint (D5)");
    }

    assertTrue(Files.isDirectory(checkpointDir), "checkpoint directory must be written");
  }
}
