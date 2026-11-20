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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alipay.sofa.jraft.util.Endpoint;
import io.taskite.cluster.raft.RaftPartitionConfig;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
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
 * De-risks the broker/PD assembly ordering for the Phase 7a cut-over: partitions are bootstrapped
 * incrementally, so a jraft group may be registered on the shared {@link JRaftServer}
 * <em>after</em> the server has already been started ({@code rpcServer.init}). This verifies that a
 * group added after {@link JRaftServer#start()} still elects and replicates — i.e. jraft's {@code
 * MutableHandlerRegistry}-backed gRPC server accepts late group registration.
 */
final class JRaftLateGroupRegistrationTest {

  private static final String CLUSTER_ID = "late-reg-cluster";

  @Test
  void groupRegisteredAfterServerStartStillElectsAndAppends(@TempDir final Path baseDir)
      throws Exception {
    final int[] ports = {freePort(), freePort(), freePort()};
    final List<RaftPeer> peers =
        List.of(
            new RaftPeer("b0", "127.0.0.1", ports[0], true),
            new RaftPeer("b1", "127.0.0.1", ports[1], true),
            new RaftPeer("b2", "127.0.0.1", ports[2], true));

    final List<JRaftServer> servers = new ArrayList<>();
    final List<JRaftPartition> partitions = new ArrayList<>();
    try {
      // Start each node's shared server FIRST (no groups yet), mirroring a node whose RPC server is
      // up before any partition is bootstrapped.
      for (int i = 0; i < 3; i++) {
        final JRaftServer server = new JRaftServer(new Endpoint("127.0.0.1", ports[i]));
        server.start();
        servers.add(server);
      }

      // THEN register the partition group on each already-started server (late registration).
      for (int i = 0; i < 3; i++) {
        final Path dir = baseDir.resolve("node-" + i);
        final RaftPartitionConfig config =
            RaftPartitionConfig.builder(CLUSTER_ID, 0, peers.get(i).brokerId())
                .peers(peers)
                .dataDirectory(dir)
                .electionTimeout(Duration.ofSeconds(1))
                .build();
        final Path groupDir = dir.resolve("partition-0");
        final JRaftPartition partition =
            JRaftPartition.builder(config, servers.get(i), groupDir).build();
        partition.start();
        assertEquals(0, partition.node().getOptions().getSnapshotIntervalSecs());
        partitions.add(partition);
      }

      final JRaftPartition leader = awaitLeader(partitions);
      assertNotNull(leader, "a group registered after server start must still elect a leader");
      leader.append(1, 1, "late".getBytes(StandardCharsets.UTF_8)).join(10, TimeUnit.SECONDS);
      for (final JRaftPartition partition : partitions) {
        assertTrue(
            awaitCommittedPosition(partition, 1),
            "every node must converge after late registration");
      }
    } finally {
      for (final JRaftServer server : servers) {
        server.close();
      }
    }
  }

  private static JRaftPartition awaitLeader(final List<JRaftPartition> partitions)
      throws InterruptedException {
    for (int i = 0; i < 200; i++) {
      for (final JRaftPartition partition : partitions) {
        if (partition.role() == RaftRole.LEADER) {
          return partition;
        }
      }
      Thread.sleep(50);
    }
    return null;
  }

  private static boolean awaitCommittedPosition(final JRaftPartition partition, final long target)
      throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      if (partition.committedPosition() >= target) {
        return true;
      }
      Thread.sleep(50);
    }
    return partition.committedPosition() >= target;
  }

  private static int freePort() throws IOException {
    try (final ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
