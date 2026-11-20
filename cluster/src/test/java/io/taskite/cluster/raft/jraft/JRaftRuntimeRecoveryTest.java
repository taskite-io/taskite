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

import com.alipay.sofa.jraft.util.Endpoint;
import io.taskite.cluster.raft.RaftPartitionConfig;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
import io.taskite.cluster.raft.jraft.runtime.JRaftStateMachine;
import io.taskite.cluster.raft.storage.RaftStorageLayout;
import io.taskite.snapshot.PartitionStorageSnapshotBackend;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * P0 recovery-semantics test for the snapshots-module removal: after the flat-layout refactor, a
 * lost {@code runtime/} directory must be rebuilt purely by jraft's {@code onSnapshotLoad} (there
 * is no longer a separate {@code SnapshotRuntimeRecovery} pre-open scan). This starts a single-node
 * group, writes business state, snapshots it into {@code snapshots/}, then simulates a crash that
 * loses {@code runtime/} and restarts the group on the SAME data directory — the processed position
 * must be restored from the snapshot jraft installs on start.
 */
final class JRaftRuntimeRecoveryTest {

  private static final String CLUSTER_ID = "recovery-cluster";
  private static final TimeUnit SECONDS = TimeUnit.SECONDS;

  @Test
  void runtimeLostAfterSnapshotIsRebuiltByJraftOnRestart(@TempDir final Path baseDir)
      throws Exception {
    final int raftPort = freePort();
    final RaftPeer self = new RaftPeer("solo", "127.0.0.1", raftPort, true);
    final Path partitionDir = baseDir.resolve("partition-0");
    final Path runtimeDir = RaftStorageLayout.runtimeDirectory(partitionDir);
    final long processedPosition = 4242;

    // 1) First incarnation: apply entries, record a processed position, and snapshot it. The
    // snapshot (a RocksDB checkpoint) lands under <partitionDir>/snapshots and jraft truncates its
    // log to that snapshot.
    Member member = Member.start(partitionDir, runtimeDir, self, raftPort);
    try {
      awaitLeader(member);
      member.partition.append(1, 3, "batch".getBytes(StandardCharsets.UTF_8)).join(10, SECONDS);
      member.store.markAsProcessed(processedPosition);
      member.store.getCurrentTransaction().commit();
      assertEquals(processedPosition, member.store.getLastProcessedPosition());

      member.partition.snapshot().join(15, SECONDS);
      assertTrue(
          hasRocksDbCheckpointFiles(partitionDir.resolve(RaftStorageLayout.SNAPSHOTS_DIR)),
          "a snapshot checkpoint must exist under snapshots/ before the crash");
    } finally {
      member.close();
    }

    // 2) Simulate a crash that loses the live runtime store (but keeps
    // raft-log/raft-meta/snapshots).
    deleteRecursively(runtimeDir);
    assertTrue(!Files.exists(runtimeDir), "runtime/ must be gone before restart");

    // 3) Second incarnation on the SAME data dir with a FRESH (empty) runtime store. There is no
    // SnapshotRuntimeRecovery anymore; jraft must call onSnapshotLoad on start and import the
    // checkpoint back into the fresh store, restoring the processed position.
    member = Member.start(partitionDir, runtimeDir, self, raftPort);
    try {
      awaitLeader(member);
      for (int i = 0;
          i < 200 && member.store.getLastProcessedPosition() != processedPosition;
          i++) {
        Thread.sleep(25);
      }
      assertEquals(
          processedPosition,
          member.store.getLastProcessedPosition(),
          "processed position must be rebuilt from the jraft snapshot after runtime/ was lost");
    } finally {
      member.close();
    }
  }

  private static void awaitLeader(final Member member) throws InterruptedException {
    for (int i = 0; i < 200 && member.partition.role() != RaftRole.LEADER; i++) {
      Thread.sleep(50);
    }
    assertTrue(member.partition.role() == RaftRole.LEADER, "single-node group must elect itself");
  }

  private static boolean hasRocksDbCheckpointFiles(final Path snapshotRoot) throws IOException {
    if (!Files.isDirectory(snapshotRoot)) {
      return false;
    }
    try (final Stream<Path> tree = Files.walk(snapshotRoot)) {
      return tree.anyMatch(
          p ->
              Files.isRegularFile(p)
                  && p.getParent() != null
                  && p.getParent().getFileName() != null
                  && JRaftStateMachine.CHECKPOINT_DIR.equals(
                      p.getParent().getFileName().toString()));
    }
  }

  private static void deleteRecursively(final Path dir) throws IOException {
    if (!Files.exists(dir)) {
      return;
    }
    try (final Stream<Path> tree = Files.walk(dir)) {
      tree.sorted(Comparator.reverseOrder())
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (final IOException e) {
                  throw new RuntimeException(e);
                }
              });
    }
  }

  private static int freePort() throws IOException {
    try (final ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  /** A single-node group: a fresh {@link JRaftServer} + one partition over a real RocksDB store. */
  private static final class Member implements AutoCloseable {
    private final JRaftServer server;
    private final JRaftPartition partition;
    private final PartitionStorage storage;
    private final StateStoreProcessingStore store;

    private Member(
        final JRaftServer server,
        final JRaftPartition partition,
        final PartitionStorage storage,
        final StateStoreProcessingStore store) {
      this.server = server;
      this.partition = partition;
      this.storage = storage;
      this.store = store;
    }

    static Member start(
        final Path partitionDir, final Path runtimeDir, final RaftPeer self, final int raftPort)
        throws IOException {
      final JRaftServer server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
      final PartitionStorage storage = new PartitionStorage(runtimeDir);
      final StateStoreProcessingStore store = new StateStoreProcessingStore(storage);
      final RaftPartitionConfig config =
          RaftPartitionConfig.builder(CLUSTER_ID, 0, self.brokerId())
              .peers(List.of(self))
              .dataDirectory(partitionDir)
              .electionTimeout(Duration.ofSeconds(1))
              .build();
      final JRaftPartition partition =
          JRaftPartition.builder(config, server, partitionDir)
              .storage(storage)
              .snapshotBackend(new PartitionStorageSnapshotBackend(storage))
              .build();
      partition.start();
      server.start();
      return new Member(server, partition, storage, store);
    }

    @Override
    public void close() {
      server.close();
      storage.close();
    }
  }
}
