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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.alipay.sofa.jraft.util.Endpoint;
import io.taskite.cluster.raft.RaftPartitionConfig;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 6 integration test (migration "priority election", ZEP006): on a real 3-node jraft cluster
 * each node is assigned an election priority via a {@link ToIntFunction} (the seam ZEP006's dynamic
 * "fewer local leaders ⇒ higher priority" supplier plugs into). The highest-priority node must win
 * the election, since jraft only lets a node campaign while its priority is {@code >=} the maximum
 * priority in the configuration. This proves leaders can be steered/balanced across nodes.
 */
final class JRaftPriorityElectionClusterTest {

  private static final String CLUSTER_ID = "priority-cluster";

  @Test
  void highestPriorityNodeWinsElection(@TempDir final Path baseDir) throws Exception {
    final int[] ports = {freePort(), freePort(), freePort()};
    final List<RaftPeer> peers =
        List.of(
            new RaftPeer("b0", "127.0.0.1", ports[0], true),
            new RaftPeer("b1", "127.0.0.1", ports[1], true),
            new RaftPeer("b2", "127.0.0.1", ports[2], true));

    // b1 gets the strictly-highest priority, so it must become the leader.
    final Map<String, Integer> priorities = Map.of("b0", 10, "b1", 100, "b2", 10);
    final ToIntFunction<RaftPeer> priorityFn = peer -> priorities.get(peer.brokerId());

    final List<Member> members = new ArrayList<>();
    try {
      for (int i = 0; i < 3; i++) {
        members.add(
            Member.start(baseDir.resolve("node-" + i), peers.get(i), peers, ports[i], priorityFn));
      }

      final Member leader = awaitLeader(members);
      assertNotNull(leader, "a leader must be elected");
      assertEquals(
          "b1",
          leader.selfBrokerId,
          "the highest-priority node (b1) must win the priority election");
    } finally {
      for (final Member member : members) {
        member.close();
      }
    }
  }

  private static Member awaitLeader(final List<Member> members) throws InterruptedException {
    // Allow extra time: priority election settles over a few election rounds (targetPriority
    // decay).
    for (int i = 0; i < 400; i++) {
      for (final Member member : members) {
        if (member.partition.role() == RaftRole.LEADER) {
          return member;
        }
      }
      Thread.sleep(50);
    }
    return null;
  }

  private static int freePort() throws IOException {
    try (final ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  /** A cluster member started with a priority function (ZEP006 election-priority seam). */
  private static final class Member implements AutoCloseable {
    private final JRaftServer server;
    private final JRaftPartition partition;
    private final String selfBrokerId;

    private Member(final JRaftServer server, final JRaftPartition partition, final String self) {
      this.server = server;
      this.partition = partition;
      this.selfBrokerId = self;
    }

    static Member start(
        final Path dir,
        final RaftPeer self,
        final List<RaftPeer> peers,
        final int raftPort,
        final ToIntFunction<RaftPeer> priorityFn)
        throws IOException {
      final JRaftServer server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
      final RaftPartitionConfig config =
          RaftPartitionConfig.builder(CLUSTER_ID, 0, self.brokerId())
              .peers(peers)
              .dataDirectory(dir)
              .electionTimeout(Duration.ofSeconds(1))
              .build();
      final Path groupDir = dir.resolve("partition-0");
      final JRaftPartition partition =
          JRaftPartition.builder(config, server, groupDir)
              .snapshotBackend(null)
              .priorityFn(priorityFn)
              .build();
      partition.start();
      server.start();
      return new Member(server, partition, self.brokerId());
    }

    @Override
    public void close() {
      server.close();
    }
  }
}
