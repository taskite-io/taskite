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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alipay.sofa.jraft.util.Endpoint;
import io.taskite.cluster.raft.RaftPartitionConfig;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
import io.taskite.cluster.raft.log.PersistedRaftRecord;
import io.taskite.cluster.raft.log.RaftJournalReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 3 integration test for the log & commit model (migration decision D1: position/index
 * mismatch). It proves, on a real 3-node jraft cluster using the production {@link JRaftServer} +
 * {@link JRaftPartition}, that:
 *
 * <ul>
 *   <li>{@code append} returns the reconstructed {@link PersistedRaftRecord} (jraft term/index +
 *       the caller's {@code [lowestPosition, highestPosition]} span) — façade parity with the
 *       in-house {@code RaftPartition.append};
 *   <li>{@link JRaftPartition#openLogReader()} streams committed entries back in index order, each
 *       carrying its original span intact — so the logstream layer's {@code RaftLogStreamReader}
 *       can expand batches into per-position records over jraft with no change.
 * </ul>
 */
final class JRaftLogModelClusterTest {

  private static final String CLUSTER_ID = "log-model-cluster";

  @Test
  void appendReturnsRecordAndCommittedLogReadsBackSpans(@TempDir final Path baseDir)
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

      final Member leaderMember = awaitLeader(members);
      final JRaftPartition leader = leaderMember.partition;

      // D1: append three contiguous position-span batches through the leader, exactly as the
      // sequencer would assign them. append must resolve to the reconstructed PersistedRaftRecord.
      final PersistedRaftRecord r1 = leader.append(1, 3, payload("batch-1..3")).join(10, SECONDS);
      final PersistedRaftRecord r2 = leader.append(4, 4, payload("batch-4")).join(10, SECONDS);
      final PersistedRaftRecord r3 = leader.append(5, 8, payload("batch-5..8")).join(10, SECONDS);

      // The returned record carries the caller's span and a real (monotonic) jraft index/term.
      assertEquals(1, r1.lowestPosition());
      assertEquals(3, r1.highestPosition());
      assertEquals(4, r2.lowestPosition());
      assertEquals(8, r3.highestPosition());
      assertTrue(r1.index() > 0, "index must be a real jraft index");
      assertTrue(r2.index() > r1.index() && r3.index() > r2.index(), "indexes must be monotonic");
      assertTrue(r1.term() >= 1, "term must be the leader's term");

      // Every node must reach committedPosition=8 (D3), then its committed log must read back the
      // three batches in order with spans intact — this is exactly what RaftLogStreamReader walks.
      for (final Member member : members) {
        assertTrue(
            awaitCommittedPosition(member.partition, 8, 5),
            "node " + member.partition.serverId() + " must reach committedPosition=8");

        final List<PersistedRaftRecord> readBack = drain(member.partition.openLogReader());
        assertEquals(3, readBack.size(), "three committed batches must be readable back");
        assertEquals(1, readBack.get(0).lowestPosition());
        assertEquals(3, readBack.get(0).highestPosition());
        assertEquals(4, readBack.get(1).lowestPosition());
        assertEquals(4, readBack.get(1).highestPosition());
        assertEquals(5, readBack.get(2).lowestPosition());
        assertEquals(8, readBack.get(2).highestPosition());
        // Indexes strictly increase (jraft may interleave a no-op/conf entry, which the reader
        // transparently skips), and the payloads survive the round-trip.
        assertTrue(
            readBack.get(0).index() < readBack.get(1).index()
                && readBack.get(1).index() < readBack.get(2).index(),
            "read-back indexes must be strictly increasing");
        assertEquals("batch-1..3", text(readBack.get(0)));
        assertEquals("batch-4", text(readBack.get(1)));
        assertEquals("batch-5..8", text(readBack.get(2)));
      }

      // A reader seeked past the end sees nothing (bounded by lastAppliedIndex).
      final RaftJournalReader reader = leader.openLogReader();
      reader.seek(Long.MAX_VALUE / 2);
      assertFalse(reader.hasNext(), "a reader past the applied tail must be empty");
      reader.close();
    } finally {
      for (final Member member : members) {
        member.close();
      }
    }
  }

  private static List<PersistedRaftRecord> drain(final RaftJournalReader reader) {
    final List<PersistedRaftRecord> out = new ArrayList<>();
    try (reader) {
      reader.seekToFirst();
      while (reader.hasNext()) {
        out.add(reader.next());
      }
    }
    return out;
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
    throw new IllegalStateException("no leader elected");
  }

  private static boolean awaitCommittedPosition(
      final JRaftPartition partition, final long target, final int seconds)
      throws InterruptedException {
    for (int i = 0; i < seconds * 20; i++) {
      if (partition.committedPosition() >= target) {
        return true;
      }
      Thread.sleep(50);
    }
    return partition.committedPosition() >= target;
  }

  private static final TimeUnit SECONDS = TimeUnit.SECONDS;

  private static byte[] payload(final String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static String text(final PersistedRaftRecord record) {
    return new String(record.data(), StandardCharsets.UTF_8);
  }

  private static int freePort() throws IOException {
    try (final ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  /** One cluster member: a shared {@link JRaftServer} hosting a single partition group. */
  private static final class Member implements AutoCloseable {
    private final JRaftServer server;
    private final JRaftPartition partition;

    private Member(final JRaftServer server, final JRaftPartition partition) {
      this.server = server;
      this.partition = partition;
    }

    static Member start(
        final Path dir, final RaftPeer self, final List<RaftPeer> peers, final int raftPort)
        throws IOException {
      final JRaftServer server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
      final RaftPartitionConfig config =
          RaftPartitionConfig.builder(CLUSTER_ID, 0, self.brokerId())
              .peers(peers)
              .dataDirectory(dir)
              .electionTimeout(Duration.ofSeconds(1))
              .build();
      final Path groupDir = dir.resolve("partition-0");
      final JRaftPartition partition = JRaftPartition.builder(config, server, groupDir).build();
      partition.start();
      server.start();
      return new Member(server, partition);
    }

    @Override
    public void close() {
      server.close();
    }
  }
}
