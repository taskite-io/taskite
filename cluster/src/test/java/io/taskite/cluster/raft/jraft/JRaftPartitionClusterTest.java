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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alipay.sofa.jraft.util.Endpoint;
import io.taskite.cluster.raft.RaftPartitionConfig;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
import io.taskite.cluster.raft.log.PersistedRaftRecord;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 2 integration test: brings up a real 3-node cluster using the production {@link
 * JRaftServer} + {@link JRaftPartition} classes (not the Phase 0 spike harness), each node hosting
 * two partition groups on one shared gRPC RpcServer (D6). Verifies election, {@code append} of
 * position-span batches (D1), committed-position convergence, and that the {@code subscribe} seam
 * delivers reconstructed {@link PersistedRaftRecord}s to the business layer.
 */
final class JRaftPartitionClusterTest {

  private static final String CLUSTER_ID = "test-cluster";
  private static final int PARTITION_COUNT = 2;

  @Test
  void consecutiveLearnerAndPromotedVoterFailuresCanBeCleanedWithoutRestartingEitherNode(
      @TempDir final Path baseDir) throws Exception {
    for (final boolean promoted : List.of(false, true)) {
      final int[] ports = {freePort(), freePort(), freePort(), freePort(), freePort()};
      final List<RaftPeer> peers = new ArrayList<>();
      for (int index = 0; index < 5; index++) {
        peers.add(
            new RaftPeer(
                "b" + index, "127.0.0.1", ports[index], index < 3 || promoted && index == 4));
      }
      final List<Node> nodes = new ArrayList<>();
      try {
        for (int index = 0; index < 5; index++) {
          nodes.add(
              Node.start(
                  baseDir.resolve((promoted ? "promoted-" : "learner-") + index),
                  peers.get(index),
                  peers,
                  ports[index]));
        }
        JRaftPartition leader = awaitLeader(nodes, 0);
        assertNotNull(leader);
        leader.append(1, 1, payload("initial")).get(10, TimeUnit.SECONDS);
        nodes.remove(3).close();
        leader.installExecutionAuthority(0, 1, "recover-through-b4").get(15, TimeUnit.SECONDS);
        // The intermediate plan was authorized while b4 was still reachable.
        nodes.remove(3).close();
        leader = awaitLeader(nodes, 0);
        assertNotNull(leader);
        final var intermediate = leader.readCommittedConfiguration().get(10, TimeUnit.SECONDS);
        assertEquals(5, intermediate.peers().size());
        assertEquals(
            promoted,
            intermediate.peers().stream()
                .filter(peer -> peer.brokerId().equals("b4"))
                .findFirst()
                .orElseThrow()
                .voter());
        leader.installExecutionAuthority(1, 2, "healthy-successor").get(15, TimeUnit.SECONDS);
        final List<RaftPeer> healthy = peers.subList(0, 3);
        leader.updateRaftMembership(healthy, 2, "healthy-successor").get(15, TimeUnit.SECONDS);
        final var committed = leader.readCommittedConfiguration().get(10, TimeUnit.SECONDS);
        assertEquals(new HashSet<>(healthy), new HashSet<>(committed.peers()));
        assertEquals(2, committed.authority().generation());
        final JRaftPartition currentLeader = leader;
        Assertions.assertThrows(
            Exception.class,
            () ->
                currentLeader
                    .updateRaftMembership(peers, 1, "recover-through-b4")
                    .get(10, TimeUnit.SECONDS));
        assertEquals(
            committed.digest(),
            leader.readCommittedConfiguration().get(10, TimeUnit.SECONDS).digest());
        leader.append(2, 2, payload("after-recovery")).get(10, TimeUnit.SECONDS);
        for (final var node : nodes) {
          assertTrue(awaitCommittedPosition(node.partition(0), 2, 5));
        }
      } finally {
        for (final var node : nodes) node.close();
      }
    }
  }

  @Test
  void authorityReplicatesAcrossLeaderFailureAndRejectsOldGenerations(@TempDir final Path baseDir)
      throws Exception {
    final int[] ports = {freePort(), freePort(), freePort()};
    final List<RaftPeer> peers =
        List.of(
            new RaftPeer("b0", "127.0.0.1", ports[0], true),
            new RaftPeer("b1", "127.0.0.1", ports[1], true),
            new RaftPeer("b2", "127.0.0.1", ports[2], true));
    final List<Node> nodes = new ArrayList<>();
    try {
      for (int i = 0; i < 3; i++) {
        nodes.add(Node.start(baseDir.resolve("node-" + i), peers.get(i), peers, ports[i]));
      }
      final var leader = awaitLeader(nodes, 0);
      assertNotNull(leader);
      awaitLeaderCommit(leader);
      final var first = leader.installExecutionAuthority(0, 1, "first").get(15, TimeUnit.SECONDS);
      assertEquals(1, first.generation());
      final var actual = leader.readCommittedConfiguration().get(10, TimeUnit.SECONDS);
      assertEquals(first, actual.authority());
      assertTrue(actual.authority().index() <= leader.lastAppliedIndex());
      assertTrue(actual.configurationIndex() <= leader.lastAppliedIndex());
      assertEquals(new HashSet<>(peers), new HashSet<>(actual.peers()));
      assertEquals(leader.appliedConfigurationDigest(), actual.digest());
      awaitAuthorities(nodes, 1);
      assertTrue(leader.executionAuthorized(1, "first"));
      assertFalse(leader.executionAuthorized(0, "legacy"));
      assertTrue(nodes.stream().allMatch(node -> node.records(0).isEmpty()));
      try (var reader = leader.openLogReader()) {
        assertFalse(reader.hasNext());
      }
      final int stopped = nodes.indexOf(leaderNodeOf(nodes, 0));
      final var failed = nodes.remove(stopped);
      failed.close();
      final var successor = awaitLeader(nodes, 0);
      assertNotNull(successor);
      awaitLeaderCommit(successor);
      assertEquals(1, successor.executionAuthority().generation());
      final var second =
          successor.installExecutionAuthority(1, 2, "second").get(15, TimeUnit.SECONDS);
      assertEquals(2, second.generation());
      assertFalse(successor.executionAuthorized(1, "first"));
      assertTrue(successor.executionAuthorized(2, "second"));
      final var restarted =
          Node.start(baseDir.resolve("node-" + stopped), peers.get(stopped), peers, ports[stopped]);
      nodes.add(restarted);
      awaitAuthorities(nodes, 2);
      assertEquals(second, restarted.partition(0).executionAuthority());
      successor.append(1, 1, payload("business")).get(10, TimeUnit.SECONDS);
      assertTrue(awaitCommittedPosition(restarted.partition(0), 1, 5));
      try (var reader = successor.openLogReader()) {
        assertTrue(reader.hasNext());
        assertEquals(1, reader.next().lowestPosition());
        assertFalse(reader.hasNext());
      }
    } finally {
      for (final var node : nodes) node.close();
    }
  }

  private static void awaitLeaderCommit(final JRaftPartition leader) throws Exception {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      try {
        leader.readCommittedConfiguration().get(2, TimeUnit.SECONDS);
        return;
      } catch (final ExecutionException error) {
        if (!error.getCause().getMessage().contains("CONFIGURATION_UNSETTLED")) {
          throw error;
        }
      }
      Thread.sleep(20);
    }
    throw new AssertionError("leader did not apply its committed term barrier");
  }

  private static void awaitAuthorities(final List<Node> nodes, final long generation)
      throws InterruptedException {
    for (int attempt = 0; attempt < 200; attempt++) {
      if (nodes.stream()
          .allMatch(node -> node.partition(0).executionAuthority().generation() == generation)) {
        return;
      }
      Thread.sleep(25);
    }
    assertTrue(
        nodes.stream()
            .allMatch(node -> node.partition(0).executionAuthority().generation() == generation));
  }

  @Test
  void threeNodesElectAppendAndConverge(@TempDir final Path baseDir) throws Exception {
    final int[] ports = {freePort(), freePort(), freePort()};
    final List<RaftPeer> peers =
        List.of(
            new RaftPeer("b0", "127.0.0.1", ports[0], true),
            new RaftPeer("b1", "127.0.0.1", ports[1], true),
            new RaftPeer("b2", "127.0.0.1", ports[2], true));

    final List<Node> nodes = new ArrayList<>();
    try {
      for (int i = 0; i < 3; i++) {
        nodes.add(Node.start(baseDir.resolve("node-" + i), peers.get(i), peers, ports[i]));
      }

      for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
        final int pid = partitionId;
        // Find the elected leader for this partition group.
        final JRaftPartition leader = awaitLeader(nodes, pid);
        assertNotNull(leader, "partition " + pid + " must elect a leader");

        // D1: append two position-span batches through the leader.
        leader.append(1, 3, payload("p" + pid + "-a")).join(10, TimeUnit.SECONDS);
        leader.append(4, 6, payload("p" + pid + "-b")).join(10, TimeUnit.SECONDS);

        // Every node's partition must converge to committedPosition=6 (D3), and followers must have
        // received the reconstructed records through the subscribe seam.
        for (final Node node : nodes) {
          final JRaftPartition partition = node.partition(pid);
          assertTrue(
              awaitCommittedPosition(partition, 6, 5),
              "node "
                  + partition.serverId()
                  + " partition "
                  + pid
                  + " must reach committedPosition=6");
        }
        // The leader's listener must have seen both batches, with spans intact.
        final List<PersistedRaftRecord> seen = leaderNodeOf(nodes, pid).records(pid);
        assertTrue(
            seen.stream().anyMatch(r -> r.lowestPosition() == 1 && r.highestPosition() == 3),
            "first batch span must be delivered");
        assertTrue(
            seen.stream().anyMatch(r -> r.lowestPosition() == 4 && r.highestPosition() == 6),
            "second batch span must be delivered");
      }
    } finally {
      for (final Node node : nodes) {
        node.close();
      }
    }
  }

  private static JRaftPartition awaitLeader(final List<Node> nodes, final int partitionId)
      throws InterruptedException {
    for (int i = 0; i < 200; i++) {
      for (final Node node : nodes) {
        final JRaftPartition partition = node.partition(partitionId);
        if (partition.role() == RaftRole.LEADER) {
          return partition;
        }
      }
      Thread.sleep(50);
    }
    return null;
  }

  private static Node leaderNodeOf(final List<Node> nodes, final int partitionId) {
    for (final Node node : nodes) {
      if (node.partition(partitionId).role() == RaftRole.LEADER) {
        return node;
      }
    }
    throw new IllegalStateException("no leader for partition " + partitionId);
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

  private static byte[] payload(final String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static int freePort() throws IOException {
    try (final ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  /**
   * One cluster member: a shared {@link JRaftServer} hosting {@link #PARTITION_COUNT} partitions.
   */
  private static final class Node implements AutoCloseable {
    private final JRaftServer server;
    private final List<JRaftPartition> partitions = new ArrayList<>();
    private final List<List<PersistedRaftRecord>> recordsByPartition = new ArrayList<>();

    private Node(final JRaftServer server) {
      this.server = server;
    }

    static Node start(
        final Path dir, final RaftPeer self, final List<RaftPeer> peers, final int raftPort)
        throws IOException {
      final JRaftServer server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
      final Node node = new Node(server);
      for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
        final RaftPartitionConfig config =
            RaftPartitionConfig.builder(CLUSTER_ID, partitionId, self.brokerId())
                .peers(peers)
                .dataDirectory(dir)
                .electionTimeout(Duration.ofSeconds(1))
                .build();
        final Path groupDir = dir.resolve("partition-" + partitionId);
        final JRaftPartition partition =
            JRaftPartition.builder(config, server, groupDir).priorityFn(RaftPeer::priority).build();
        final List<PersistedRaftRecord> records = new CopyOnWriteArrayList<>();
        partition.subscribe(records::add);
        partition.start();
        node.partitions.add(partition);
        node.recordsByPartition.add(records);
      }
      server.start();
      return node;
    }

    JRaftPartition partition(final int partitionId) {
      return partitions.get(partitionId);
    }

    List<PersistedRaftRecord> records(final int partitionId) {
      return recordsByPartition.get(partitionId);
    }

    @Override
    public void close() {
      server.close();
    }
  }
}
