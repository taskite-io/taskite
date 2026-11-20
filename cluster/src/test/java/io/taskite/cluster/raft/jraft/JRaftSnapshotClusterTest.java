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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alipay.sofa.jraft.util.Endpoint;
import io.taskite.cluster.raft.RaftPartitionConfig;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
import io.taskite.cluster.raft.jraft.runtime.JRaftStateMachine;
import io.taskite.snapshot.PartitionStorageSnapshotBackend;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 4 integration test (migration decisions D4/D5): on a real 3-node jraft cluster whose state
 * machines are backed by {@link PartitionStorageSnapshotBackend} over real {@link PartitionStorage}
 * RocksDB stores, appending entries and triggering {@link JRaftPartition#snapshot()} drives jraft's
 * {@code onSnapshotSave}, which materializes a RocksDB checkpoint under jraft's snapshot directory.
 * Verifies the snapshot completes and the checkpoint files are physically present (so jraft's own
 * {@code InstallSnapshot} can replicate them to lagging followers, replacing the in-house chunked
 * install).
 */
final class JRaftSnapshotClusterTest {

  private static final String CLUSTER_ID = "snapshot-cluster";

  @Test
  void committedSnapshotRecoversUncertifiedStorageDespiteStaleLearnerTopology(
      @TempDir final Path baseDir) throws Exception {
    final int port = freePort();
    final RaftPeer voter = new RaftPeer("solo", "127.0.0.1", port, true, 100);
    final Path dir = baseDir.resolve("legacy");
    try (Member member = Member.start(dir, voter, List.of(voter), port)) {
      assertNotNull(awaitLeader(List.of(member)));
      member.partition.append(1, 1, new byte[] {1}).join(10, SECONDS);
      member.store.markAsProcessed(1);
      member.store.getCurrentTransaction().commit();
      member.partition.takeSnapshot().join(15, SECONDS);
    }
    Files.delete(dir.resolve("partition-0/applied-raft-configuration"));
    final RaftPeer stale = new RaftPeer("solo", "127.0.0.1", port, false, 0);
    final RaftPeer staleVoter = new RaftPeer("other", "127.0.0.1", freePort(), true, 10);
    try (Member recovered = Member.start(dir, stale, List.of(stale, staleVoter), port)) {
      assertNotNull(awaitLeader(List.of(recovered)));
      assertTrue(recovered.partition.node().getOptions().getElectionPriority() > 0);
      recovered.partition.append(2, 2, new byte[] {2}).join(10, SECONDS);
      assertTrue(Files.exists(dir.resolve("partition-0/applied-raft-configuration")));
    }
  }

  @Test
  void triggeringSnapshotWritesRocksDbCheckpointUnderJraftSnapshotDir(@TempDir final Path baseDir)
      throws Exception {
    final int[] ports = {freePort(), freePort(), freePort()};
    final List<RaftPeer> peers =
        List.of(
            new RaftPeer("b0", "127.0.0.1", ports[0], true),
            new RaftPeer("b1", "127.0.0.1", ports[1], true),
            new RaftPeer("b2", "127.0.0.1", ports[2], true));

    final List<Member> members = new ArrayList<>();
    try {
      for (int i = 0; i < 3; i++) {
        members.add(Member.start(baseDir.resolve("node-" + i), peers.get(i), peers, ports[i]));
      }

      final Member leader = awaitLeader(members);
      assertNotNull(leader, "a leader must be elected");

      // Append a couple of position-span batches and advance the business processed position so the
      // checkpoint captures real state.
      leader
          .partition
          .append(1, 3, "batch-1..3".getBytes(StandardCharsets.UTF_8))
          .join(10, SECONDS);
      leader
          .partition
          .append(4, 6, "batch-4..6".getBytes(StandardCharsets.UTF_8))
          .join(10, SECONDS);
      leader.store.markAsProcessed(6);
      leader.store.getCurrentTransaction().commit();

      // D4: trigger a snapshot on the leader; jraft drives onSnapshotSave -> RocksDB checkpoint.
      final var persisted = leader.partition.takeSnapshot().join(15, SECONDS);
      assertFalse(persisted.skipped());
      assertTrue(persisted.snapshotSizeBytes() > 0);

      final var skipped = leader.partition.takeSnapshot().join(15, SECONDS);
      assertTrue(skipped.skipped());

      // The checkpoint must be physically present under the flat snapshots/ dir for this group.
      final Path snapshotRoot = leader.groupDir.resolve("snapshots");
      assertTrue(
          hasRocksDbCheckpointFiles(snapshotRoot),
          "a RocksDB checkpoint must be written under the jraft snapshot dir: " + snapshotRoot);
    } finally {
      for (final Member member : members) {
        member.close();
      }
    }
  }

  @Test
  void snapshotWaitsForBusinessStateToReachCommittedPosition(@TempDir final Path baseDir)
      throws Exception {
    final int raftPort = freePort();
    final RaftPeer self = new RaftPeer("b0", "127.0.0.1", raftPort, true);
    try (Member member =
        Member.start(baseDir.resolve("lagging-business-state"), self, List.of(self), raftPort)) {
      assertNotNull(awaitLeader(List.of(member)), "the single node must become leader");
      member
          .partition
          .append(1, 3, "batch-1..3".getBytes(StandardCharsets.UTF_8))
          .join(10, SECONDS);

      assertThrows(RuntimeException.class, () -> member.partition.snapshot().join(10, SECONDS));

      member.store.markAsProcessed(3);
      member.store.getCurrentTransaction().commit();
      member.partition.snapshot().join(15, SECONDS);
      assertTrue(hasRocksDbCheckpointFiles(member.groupDir.resolve("snapshots")));
    }
  }

  /**
   * The periodic snapshot driver (StreamPartition's timer) repeatedly calls {@code snapshot()} on
   * the leader. This asserts that a repeated, leader-only trigger is safe and keeps producing a
   * valid RocksDB checkpoint across rounds (L3: periodic snapshotting must actually run and bound
   * raft-log growth) — and that a follower-side trigger is a harmless no-op.
   */
  @Test
  void repeatedLeaderSnapshotTriggersKeepWritingCheckpoints(@TempDir final Path baseDir)
      throws Exception {
    final int[] ports = {freePort(), freePort(), freePort()};
    final List<RaftPeer> peers =
        List.of(
            new RaftPeer("b0", "127.0.0.1", ports[0], true),
            new RaftPeer("b1", "127.0.0.1", ports[1], true),
            new RaftPeer("b2", "127.0.0.1", ports[2], true));

    final List<Member> members = new ArrayList<>();
    try {
      for (int i = 0; i < 3; i++) {
        members.add(Member.start(baseDir.resolve("node-" + i), peers.get(i), peers, ports[i]));
      }
      final Member leader = awaitLeader(members);
      assertNotNull(leader, "a leader must be elected");

      long position = 0;
      for (int round = 1; round <= 3; round++) {
        final long from = position + 1;
        final long to = position + 3;
        leader
            .partition
            .append(from, to, ("round-" + round).getBytes(StandardCharsets.UTF_8))
            .join(10, SECONDS);
        leader.store.markAsProcessed(to);
        leader.store.getCurrentTransaction().commit();
        position = to;
        // Simulate one tick of the periodic timer.
        leader.partition.snapshot().join(15, SECONDS);
        assertTrue(
            hasRocksDbCheckpointFiles(leader.groupDir.resolve("snapshots")),
            "round " + round + " must leave a RocksDB checkpoint");
      }
    } finally {
      for (final Member member : members) {
        member.close();
      }
    }
  }

  @Test
  void exposesEncodedLogBytesForSizeBasedSnapshotPolicy(@TempDir final Path baseDir)
      throws Exception {
    final int port = freePort();
    final RaftPeer self = new RaftPeer("solo", "127.0.0.1", port, true);
    final JRaftServer server = new JRaftServer(new Endpoint("127.0.0.1", port));
    final PartitionStorage storage = new PartitionStorage(baseDir.resolve("runtime"));
    final RaftPartitionConfig config =
        RaftPartitionConfig.builder(CLUSTER_ID, 0, self.brokerId())
            .peers(List.of(self))
            .dataDirectory(baseDir)
            .electionTimeout(Duration.ofSeconds(1))
            .build();
    final JRaftPartition partition =
        JRaftPartition.builder(config, server, baseDir.resolve("partition-0"))
            .snapshotBackend(new PartitionStorageSnapshotBackend(storage))
            .build();
    try {
      partition.start();
      server.start();
      // Wait for self-election so appends actually apply.
      for (int i = 0; i < 200 && partition.role() != RaftRole.LEADER; i++) {
        Thread.sleep(50);
      }
      assertTrue(partition.role() == RaftRole.LEADER, "single-node group must elect itself");

      for (int i = 1; i <= 6; i++) {
        partition.append(i, i, ("e" + i).getBytes(StandardCharsets.UTF_8)).join(10, SECONDS);
      }

      final var progress = partition.snapshotProgress().current();
      assertTrue(
          progress.uncoveredLogBytes() > 0, () -> "log-byte progress must advance: " + progress);
      assertTrue(progress.retainedLogBytes() >= progress.uncoveredLogBytes());
      assertTrue(progress.lastLogIndex() >= 6);
    } finally {
      server.close();
      storage.close();
    }
  }

  /**
   * True if some jraft snapshot under {@code snapshotRoot} contains a non-empty rocksdb/ subdir.
   */
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

  private static Member awaitLeader(final List<Member> members) throws InterruptedException {
    for (int i = 0; i < 200; i++) {
      for (final Member member : members) {
        if (member.partition.role() == RaftRole.LEADER) {
          return member;
        }
      }
      Thread.sleep(50);
    }
    return null;
  }

  private static final TimeUnit SECONDS = TimeUnit.SECONDS;

  private static int freePort() throws IOException {
    // JRaft's gRPC factory requires port < 65535, while the OS may allocate 65535.
    while (true) {
      try (final ServerSocket socket = new ServerSocket(0)) {
        final int port = socket.getLocalPort();
        if (port < 65535) {
          return port;
        }
      }
    }
  }

  /** A cluster member: a shared {@link JRaftServer} + one partition backed by real RocksDB. */
  private static final class Member implements AutoCloseable {
    private final JRaftServer server;
    private final JRaftPartition partition;
    private final PartitionStorage storage;
    private final StateStoreProcessingStore store;
    private final Path groupDir;

    private Member(
        final JRaftServer server,
        final JRaftPartition partition,
        final PartitionStorage storage,
        final StateStoreProcessingStore store,
        final Path groupDir) {
      this.server = server;
      this.partition = partition;
      this.storage = storage;
      this.store = store;
      this.groupDir = groupDir;
    }

    static Member start(
        final Path dir, final RaftPeer self, final List<RaftPeer> peers, final int raftPort)
        throws IOException {
      final JRaftServer server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
      final PartitionStorage storage = new PartitionStorage(dir.resolve("runtime"));
      final StateStoreProcessingStore store = new StateStoreProcessingStore(storage);
      final RaftPartitionConfig config =
          RaftPartitionConfig.builder(CLUSTER_ID, 0, self.brokerId())
              .peers(peers)
              .dataDirectory(dir)
              .electionTimeout(Duration.ofSeconds(1))
              .build();
      final Path groupDir = dir.resolve("partition-0");
      final JRaftPartition partition =
          JRaftPartition.builder(config, server, groupDir)
              .snapshotBackend(new PartitionStorageSnapshotBackend(storage))
              .priorityFn(RaftPeer::priority)
              .build();
      partition.setAppliedPositionSupplier(store::getLastProcessedPosition);
      partition.start();
      server.start();
      return new Member(server, partition, storage, store, groupDir);
    }

    @Override
    public void close() {
      server.close();
      storage.close();
    }
  }
}
