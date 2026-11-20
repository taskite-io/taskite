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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alipay.sofa.jraft.util.Endpoint;
import io.taskite.cluster.raft.RaftPartitionConfig;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class JRaftIpv6ClusterTest {
  @TempDir Path directory;

  @Test
  void electsReplicatesFailsOverAndRecoversOverIpv6() throws Exception {
    final List<RaftPeer> peers = new ArrayList<>();
    // Reserve all ports together so the kernel cannot assign the same free port twice.
    try (var first = socket();
        var second = socket();
        var third = socket()) {
      peers.add(new RaftPeer("b0", "::1", first.getLocalPort(), true, 10));
      peers.add(new RaftPeer("b1", "::1", second.getLocalPort(), true, 10));
      peers.add(new RaftPeer("b2", "::1", third.getLocalPort(), true, 10));
    }
    final List<Member> members = new ArrayList<>();
    try {
      for (final var peer : peers) members.add(start(peer, peers));
      leader(members).partition.append(1, 1, new byte[] {1}).get(10, TimeUnit.SECONDS);
      await(() -> members.stream().allMatch(m -> m.partition.committedPosition() == 1));
      assertIdentities(members);

      final Member stopped = leader(members);
      members.remove(stopped);
      stopped.close();
      leader(members).partition.append(2, 2, new byte[] {2}).get(10, TimeUnit.SECONDS);
      await(() -> members.stream().allMatch(m -> m.partition.committedPosition() == 2));
      members.add(
          start(
              peers.stream().filter(p -> p.brokerId().equals(stopped.id)).findFirst().orElseThrow(),
              peers));
      await(() -> members.stream().allMatch(m -> m.partition.committedPosition() == 2));

      members.forEach(Member::close);
      members.clear();
      for (final var peer : peers) members.add(start(peer, peers));
      leader(members).partition.append(3, 3, new byte[] {3}).get(10, TimeUnit.SECONDS);
      await(() -> members.stream().allMatch(m -> m.partition.committedPosition() == 3));
      assertIdentities(members);
      for (final var member : members) {
        try (var reader = member.partition.openLogReader()) {
          for (int position = 1; position <= 3; position++) {
            assertTrue(reader.hasNext());
            final var record = reader.next();
            assertEquals(position, record.lowestPosition());
            assertArrayEquals(new byte[] {(byte) position}, record.data());
          }
          assertFalse(reader.hasNext());
        }
      }
    } finally {
      members.forEach(Member::close);
    }
  }

  private Member start(final RaftPeer self, final List<RaftPeer> peers) {
    final var server = new JRaftServer(new Endpoint(self.host(), self.port()));
    assertEquals("[::1]", server.endpoint().getIp());
    final var partition =
        JRaftPartition.builder(
                RaftPartitionConfig.builder("ipv6", 1, self.brokerId())
                    .peers(peers)
                    .dataDirectory(directory.resolve(self.brokerId()))
                    .electionTimeout(Duration.ofMillis(500))
                    .build(),
                server,
                directory.resolve(self.brokerId()))
            .priorityFn(RaftPeer::priority)
            .build();
    try {
      partition.start();
      return new Member(self.brokerId(), server, partition);
    } catch (final RuntimeException error) {
      partition.close();
      server.close();
      throw error;
    }
  }

  private static ServerSocket socket() throws Exception {
    return new ServerSocket(0, 50, InetAddress.getByName("::1"));
  }

  private static Member leader(final List<Member> members) throws Exception {
    await(() -> members.stream().anyMatch(m -> m.partition.role() == RaftRole.LEADER));
    return members.stream()
        .filter(m -> m.partition.role() == RaftRole.LEADER)
        .findFirst()
        .orElseThrow();
  }

  private static void assertIdentities(final List<Member> members) {
    for (final var member : members) {
      assertEquals(
          List.of("b0", "b1", "b2"),
          member.partition.members().stream().map(m -> m.memberId()).toList());
      assertTrue(List.of("b0", "b1", "b2").contains(member.partition.leaderId()));
    }
  }

  private static void await(final BooleanSupplier condition) throws Exception {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(25);
    assertTrue(condition.getAsBoolean(), "IPv6 cluster did not converge within 20s");
  }

  private record Member(String id, JRaftServer server, JRaftPartition partition)
      implements AutoCloseable {
    @Override
    public void close() {
      partition.close();
      server.close();
    }
  }
}
