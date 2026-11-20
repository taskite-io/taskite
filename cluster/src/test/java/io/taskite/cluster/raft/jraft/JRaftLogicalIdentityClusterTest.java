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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.core.TaskiteElectionLifecycle;
import com.alipay.sofa.jraft.util.Endpoint;
import io.taskite.cluster.raft.RaftPartitionConfig;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
import io.taskite.snapshot.PartitionStorageSnapshotBackend;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class JRaftLogicalIdentityClusterTest {
  private static final String CLUSTER = "logical-identity-cluster";
  private static final byte[] STATE_KEY =
      "logical-identity-business-state".getBytes(StandardCharsets.UTF_8);

  @Test
  void recoveredLearnerMissingFromInitialDirectoryCatchesUpFromCompactedSnapshot(
      @TempDir final Path directory) throws Exception {
    final var voter = new RaftPeer("broker-0", "127.0.0.1", freePort(), true, 100);
    final var learner = new RaftPeer("broker-1", "127.0.0.1", freePort(), false, 0);
    final var peers = new ArrayList<>(List.of(voter, learner));
    final var members = new ArrayList<Member>();
    try {
      members.add(Member.start(directory.resolve("broker-0"), voter, peers));
      members.add(Member.start(directory.resolve("broker-1"), learner, peers));
      final var original = leader(members);
      original.partition.append(1, 1, new byte[] {11}).join(10, TimeUnit.SECONDS);
      await(() -> members.get(1).position() == 1);
      members.get(1).close();
      original.partition.append(2, 100, new byte[] {22}).join(10, TimeUnit.SECONDS);
      await(() -> original.position() == 100);
      assertTrue(!original.partition.takeSnapshot().join(15, TimeUnit.SECONDS).skipped());
      original.partition.append(101, 200, new byte[] {33}).join(10, TimeUnit.SECONDS);
      await(() -> original.position() == 200);
      assertTrue(!original.partition.takeSnapshot().join(15, TimeUnit.SECONDS).skipped());
      final String digest = original.partition.appliedConfigurationDigest();
      final String configuration =
          ((NodeImpl) original.partition.node()).getCurrentConf().toString();
      original.close();
      // Startup receives a target roster containing only the voter. Persisted Raft membership
      // still contains the learner; its missing address must be a connection failure.
      members.set(0, Member.start(directory.resolve("broker-0"), voter, List.of(voter)));
      final var recovered = leader(members);
      assertEquals(
          configuration, ((NodeImpl) recovered.partition.node()).getCurrentConf().toString());
      assertEquals(2, recovered.partition.members().size());
      recovered.partition.append(201, 201, new byte[] {44}).join(10, TimeUnit.SECONDS);
      await(() -> recovered.position() == 201);
      final var movedLearner = new RaftPeer("broker-1", "::1", freePort(), false, 0);
      peers.set(1, movedLearner);
      recovered.partition.observeTopology(peers);
      members.set(1, Member.start(directory.resolve("broker-1"), movedLearner, peers));
      await(() -> members.get(1).position() == 201);
      assertEquals(200L, members.get(1).partition.snapshotPosition());
      assertArrayEquals(new byte[] {44}, members.get(1).state());
      await(() -> digest.equals(members.get(1).partition.appliedConfigurationDigest()));
      assertEquals(digest, recovered.partition.appliedConfigurationDigest());
    } finally {
      for (final var member : members) {
        member.close();
      }
    }
  }

  @Test
  void movedLeaderInstallsIpv6SnapshotAndMovedLearnerResumesReplication(
      @TempDir final Path directory) throws Exception {
    final int originalPort = freePort();
    final var original = new RaftPeer("broker-0", "127.0.0.1", originalPort, true, 100);
    final List<RaftPeer> peers = new ArrayList<>(List.of(original));
    final List<Member> members = new ArrayList<>();
    try {
      members.add(Member.start(directory.resolve("broker-0"), original, peers));
      leader(members).partition.append(1, 1, new byte[] {11}).join(10, TimeUnit.SECONDS);
      await(() -> members.getFirst().position() == 1);
      members.getFirst().close();
      final var moved = new RaftPeer("broker-0", "::1", freePort(), true, 100);
      peers.set(0, moved);
      members.set(0, Member.start(directory.resolve("broker-0"), moved, peers));
      final var leader = leader(members);
      leader.partition.append(2, 100, new byte[] {22}).join(10, TimeUnit.SECONDS);
      await(() -> leader.position() == 100);
      assertTrue(!leader.partition.takeSnapshot().join(15, TimeUnit.SECONDS).skipped());
      // JRaft keeps the previous snapshot's logs. A second generation compacts the earlier
      // entries so a fresh learner cannot obtain the checkpoint state through AppendEntries.
      leader.partition.append(101, 200, new byte[] {22}).join(10, TimeUnit.SECONDS);
      await(() -> leader.position() == 200);
      assertTrue(!leader.partition.takeSnapshot().join(15, TimeUnit.SECONDS).skipped());
      // Reuse the leader's old physical endpoint for a different member. Snapshot file requests
      // must reach the IPv6 server advertised in the URI, without member-directory resolution.
      final var learner = new RaftPeer("broker-1", "127.0.0.1", originalPort, false, 0);
      peers.add(learner);
      members.add(Member.start(directory.resolve("broker-1"), learner, peers));
      leader.partition.observeTopology(peers);
      leader.partition.updateRaftMembership(peers).join(10, TimeUnit.SECONDS);
      await(() -> members.get(1).position() == 200);
      assertArrayEquals(new byte[] {22}, members.get(1).state());
      assertEquals(200L, members.get(1).partition.snapshotPosition());
      try (final var snapshots = Files.walk(directory.resolve("broker-1/partition-0/snapshots"))) {
        assertTrue(
            snapshots.anyMatch(path -> path.getFileName().toString().endsWith(".sst")),
            "fresh learner must install the remote RocksDB checkpoint");
      }
      final String digest = leader.partition.appliedConfigurationDigest();
      await(() -> digest.equals(members.get(1).partition.appliedConfigurationDigest()));
      members.get(1).close();
      final var movedLearner = new RaftPeer("broker-1", "::1", freePort(), false, 0);
      peers.set(1, movedLearner);
      leader.partition.observeTopology(peers);
      leader.partition.append(201, 201, new byte[] {33}).join(10, TimeUnit.SECONDS);
      members.set(1, Member.start(directory.resolve("broker-1"), movedLearner, peers));
      await(() -> members.get(1).position() == 201);
      await(() -> digest.equals(members.get(1).partition.appliedConfigurationDigest()));
      assertArrayEquals(new byte[] {33}, members.get(1).state());
      assertEquals(digest, leader.partition.appliedConfigurationDigest());
      assertEquals(digest, members.get(1).partition.appliedConfigurationDigest());
    } finally {
      for (final var member : members) member.close();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3})
  void retainedVotersRecoverAfterConcurrentAddressChanges(
      final int changedCount, @TempDir final Path directory) throws Exception {
    final List<RaftPeer> peers = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      final int port = freePort();
      peers.add(new RaftPeer("broker-" + i, "127.0.0.1", port, true, 100));
    }
    final List<Member> members = new ArrayList<>();
    try {
      for (int i = 0; i < 3; i++)
        members.add(Member.start(directory.resolve("broker-" + i), peers.get(i), peers));
      final var firstLeader = leader(members);
      firstLeader.partition.append(1, 1, new byte[] {11}).join(10, TimeUnit.SECONDS);
      await(() -> members.stream().allMatch(member -> member.position() >= 1));
      final String digest = firstLeader.partition.appliedConfigurationDigest();
      final long term = firstLeader.partition.term();
      for (final var member : members) member.partition.snapshot().join(15, TimeUnit.SECONDS);
      // Both restarted voters stop before either one rejoins. Never reset configuration or data.
      for (int i = 0; i < changedCount; i++) members.get(i).close();
      for (int i = 0; i < changedCount; i++) {
        final var original = peers.get(i);
        peers.set(i, new RaftPeer(original.brokerId(), "::1", freePort(), true, 100));
      }
      for (int i = changedCount; i < 3; i++) members.get(i).partition.observeTopology(peers);
      for (int i = 0; i < changedCount; i++)
        members.set(i, Member.start(directory.resolve("broker-" + i), peers.get(i), peers));
      final var recoveredLeader = leader(members);
      await(() -> recoveredLeader.partition.term() >= term);
      assertTrue(recoveredLeader.partition.term() >= term);
      assertEquals(digest, recoveredLeader.partition.appliedConfigurationDigest());
      for (final var member : members) assertArrayEquals(new byte[] {11}, member.state());
      recoveredLeader.partition.append(2, 2, new byte[] {22}).join(10, TimeUnit.SECONDS);
      await(() -> members.stream().allMatch(member -> member.position() >= 2));
      for (final var member : members) {
        assertArrayEquals(new byte[] {22}, member.state());
        assertEquals(digest, member.partition.appliedConfigurationDigest());
        assertEquals(3, member.partition.members().size());
      }
    } finally {
      for (final var member : members) member.close();
    }
  }

  @Test
  void committedConfigurationRemainsReadableAfterSnapshotCompaction(@TempDir final Path directory)
      throws Exception {
    final var peer = new RaftPeer("broker-0", "127.0.0.1", freePort(), true, 100);
    try (final var member = Member.start(directory.resolve("broker-0"), peer, List.of(peer))) {
      final var leader = leader(List.of(member));
      await(
          () -> TaskiteElectionLifecycle.configurationSettled((NodeImpl) leader.partition.node()));
      final var original = leader.partition.readCommittedConfiguration().join(10, TimeUnit.SECONDS);
      leader.partition.append(1, 1, new byte[] {11}).join(10, TimeUnit.SECONDS);
      await(() -> leader.position() == 1);
      assertTrue(!leader.partition.takeSnapshot().join(15, TimeUnit.SECONDS).skipped());
      leader.partition.append(2, 2, new byte[] {22}).join(10, TimeUnit.SECONDS);
      await(() -> leader.position() == 2);
      assertTrue(!leader.partition.takeSnapshot().join(15, TimeUnit.SECONDS).skipped());
      final var compacted =
          leader.partition.readCommittedConfiguration().join(10, TimeUnit.SECONDS);
      assertEquals(original.digest(), compacted.digest());
      assertEquals(original.peers(), compacted.peers());
      assertEquals(original.authority(), compacted.authority());
      assertEquals("broker-0", compacted.leaderId());
      assertEquals(original.term(), compacted.term());
      assertArrayEquals(new byte[] {22}, member.state());
    }
  }

  private static Member leader(final List<Member> members) throws Exception {
    await(
        () ->
            members.stream()
                .anyMatch(member -> !member.closed && member.partition.role() == RaftRole.LEADER));
    final var leader =
        members.stream()
            .filter(member -> !member.closed && member.partition.role() == RaftRole.LEADER)
            .findFirst()
            .orElseThrow();
    assertNotNull(leader.partition.leaderId());
    return leader;
  }

  private static void await(final BooleanSupplier condition) throws Exception {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(25);
    assertTrue(condition.getAsBoolean(), "Raft and business state must converge");
  }

  private static int freePort() throws Exception {
    try (final var socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static final class Member implements AutoCloseable {
    private final JRaftServer server;
    private final JRaftPartition partition;
    private final PartitionStorage storage;
    private final StateStoreProcessingStore store;
    private boolean closed;

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

    static Member start(final Path directory, final RaftPeer self, final List<RaftPeer> peers) {
      final var server = new JRaftServer(new Endpoint(self.host(), self.port()));
      final var storage = new PartitionStorage(directory.resolve("runtime"));
      final var store = new StateStoreProcessingStore(storage);
      final var config =
          RaftPartitionConfig.builder(CLUSTER, 0, self.brokerId())
              .peers(peers)
              .dataDirectory(directory)
              .electionTimeout(Duration.ofMillis(500))
              .build();
      final var partition =
          JRaftPartition.builder(config, server, directory.resolve("partition-0"))
              .logicalMemberIdentity()
              .snapshotBackend(new PartitionStorageSnapshotBackend(storage))
              .priorityFn(RaftPeer::priority)
              .storage(storage)
              .build();
      partition.setAppliedPositionSupplier(store::getLastProcessedPosition);
      partition.subscribe(
          record -> {
            if (record.highestPosition() <= store.getLastProcessedPosition()) {
              return;
            }
            try {
              storage.runtimeDb().put(STATE_KEY, record.data());
              store.markAsProcessed(record.highestPosition());
              store.getCurrentTransaction().commit();
            } catch (final Exception error) {
              throw new IllegalStateException(error);
            }
          });
      try {
        partition.start();
        return new Member(server, partition, storage, store);
      } catch (final RuntimeException error) {
        server.close();
        storage.close();
        throw error;
      }
    }

    long position() {
      return store.getLastProcessedPosition();
    }

    byte[] state() throws Exception {
      return storage.runtimeDb().get(STATE_KEY);
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      server.close();
      storage.close();
    }
  }
}
