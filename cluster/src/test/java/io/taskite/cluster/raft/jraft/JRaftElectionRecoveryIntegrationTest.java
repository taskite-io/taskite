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

import static org.junit.jupiter.api.Assertions.*;

import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.core.TaskiteElectionLifecycle;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.codec.v2.LogEntryV2CodecFactory;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.storage.impl.RocksDBLogStorage;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.RepeatedTimer;
import io.taskite.cluster.raft.RaftNodeState;
import io.taskite.cluster.raft.RaftPartitionConfig;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.membership.JRaftElectionCoordinator;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Exercises real RPC, persistent log recovery, promotion, and timeout elections. */
final class JRaftElectionRecoveryIntegrationTest {
  @TempDir Path directory;

  @Test
  void unknownResetSourceCannotAuthorizeConfigurationBeyondItsCoverage() throws Exception {
    final var self = new RaftPeer("b0", "127.0.0.1", port(), true, 10);
    final var learner = new RaftPeer("b1", "127.0.0.1", port(), false, 0);
    try (var member = start(List.of(self, learner), 0)) {
      member.partition.updateRaftMembership(List.of(self)).get(15, TimeUnit.SECONDS);
      await(() -> member.partition.members().size() == 1);
      final var node = (NodeImpl) member.partition.node();
      final var getConf = NodeImpl.class.getDeclaredMethod("getConf");
      getConf.setAccessible(true);
      final var actual = (ConfigurationEntry) getConf.invoke(node);
      assertTrue(actual.getId().getIndex() > 1);
      final var source =
          new TaskiteElectionLifecycle.Observation(
              new TaskiteElectionLifecycle.Observation.Membership(actual.getConf().copy(), 0, 0),
              new TaskiteElectionLifecycle.Observation.Runtime(10, 10, RaftNodeState.UNKNOWN, 0));
      final var rejected =
          TaskiteElectionLifecycle.resetConfiguration(node, source, actual.getConf().copy());
      assertFalse(rejected.isOk());
      assertEquals("STALE_CONFIGURATION", rejected.getErrorMsg());
      assertEquals(1, member.partition.members().size());
    }
  }

  @Test
  void blockedPartitionDoesNotBlockReadsOrRepairInAnotherPartition() throws Exception {
    final var slowPeer = new RaftPeer("slow", "127.0.0.1", port(), true, 10);
    final var healthyPeer = new RaftPeer("healthy", "127.0.0.1", port(), true, 10);
    try (var slow = start(List.of(slowPeer), 0);
        var healthy = start(List.of(healthyPeer), 0)) {
      await(
          () ->
              slow.partition.role() == RaftRole.LEADER
                  && healthy.partition.role() == RaftRole.LEADER
                  && healthy.partition.term() > 0);
      final long term = healthy.partition.term();
      final var lockField = NodeImpl.class.getDeclaredField("writeLock");
      lockField.setAccessible(true);
      final var lock = (Lock) lockField.get(slow.partition.node());
      final var electionField = JRaftPartition.class.getDeclaredField("election");
      electionField.setAccessible(true);
      final var coordinator = (JRaftElectionCoordinator) electionField.get(slow.partition);
      lock.lock();
      try {
        // Simulates a partition whose Raft/FSM path holds the node lock during backpressure.
        // Its query and FSM notification must remain bounded and free from reconciliation.
        CompletableFuture.runAsync(
                () -> {
                  for (int i = 0; i < 1000; i++) {
                    assertNotNull(coordinator.state());
                    coordinator.digest();
                    coordinator.requestReconciliation();
                  }
                })
            .get(2, TimeUnit.SECONDS);
        healthy.partition.node().getOptions().setElectionPriority(0);
        await(() -> healthy.partition.node().getOptions().getElectionPriority() == 10);
        Thread.sleep(600);
        assertEquals(RaftRole.LEADER, healthy.partition.role());
        assertEquals(
            term,
            healthy.partition.term(),
            "periodic repair must retain a healthy leader and its term");
      } finally {
        lock.unlock();
      }
    }
  }

  @Test
  void uncertifiedSingletonCannotElectDuringInitialization() throws Exception {
    final int[] ports = {port(), port()};
    final List<RaftPeer> initial =
        List.of(
            new RaftPeer("b0", "127.0.0.1", ports[0], true, 100),
            new RaftPeer("b1", "127.0.0.1", ports[1], false, 0));
    start(initial, 1).close();
    final var storage =
        new RocksDBLogStorage(directory.resolve("b1/raft-log").toString(), new RaftOptions());
    final var options = new LogStorageOptions();
    options.setGroupId("election-recovery-partition-1");
    options.setConfigurationManager(new ConfigurationManager());
    options.setLogEntryCodecFactory(LogEntryV2CodecFactory.getInstance());
    assertTrue(storage.init(options));
    try {
      final var entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
      entry.setId(new LogId(storage.getLastLogIndex() + 1, 1));
      entry.setPeers(List.of(PeerId.parsePeer("127.0.0.1:" + ports[1] + "::10")));
      assertTrue(storage.appendEntry(entry));
    } finally {
      storage.shutdown();
    }
    final var failure = assertThrows(IllegalStateException.class, () -> start(initial, 1));
    assertTrue(failure.getMessage().contains("UNCERTIFIED_SINGLETON_ELECTION"));
  }

  @Test
  void uncommittedPromotionLogCannotAuthorizeLearnerOnRestart() throws Exception {
    final int[] ports = {port(), port()};
    final List<RaftPeer> initial =
        List.of(
            new RaftPeer("b0", "127.0.0.1", ports[0], true, 100),
            new RaftPeer("b1", "127.0.0.1", ports[1], false, 0));
    final Member learner = start(initial, 1);
    learner.close();
    final var storage =
        new RocksDBLogStorage(directory.resolve("b1/raft-log").toString(), new RaftOptions());
    final var options = new LogStorageOptions();
    options.setGroupId("election-recovery-partition-1");
    options.setConfigurationManager(new ConfigurationManager());
    options.setLogEntryCodecFactory(LogEntryV2CodecFactory.getInstance());
    assertTrue(storage.init(options));
    try {
      final var entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
      entry.setId(new LogId(storage.getLastLogIndex() + 1, 1));
      entry.setPeers(
          List.of(
              PeerId.parsePeer("127.0.0.1:" + ports[0] + "::100"),
              PeerId.parsePeer("127.0.0.1:" + ports[1] + "::10")));
      assertTrue(storage.appendEntry(entry));
    } finally {
      storage.shutdown();
    }
    try (Member recovered = start(initial, 1)) {
      assertEquals(0, recovered.partition.node().getOptions().getElectionPriority());
      assertEquals(0, recovered.partition.electionState().priority());
      assertTrue(
          recovered.partition.members().stream()
              .anyMatch(
                  member -> member.memberId().equals("b1") && member.role() == RaftRole.LEARNER));
    }
  }

  @Test
  void promotionRestartsFollowerTimerAndIgnoresStaleLearnerObservation() throws Exception {
    final int[] ports = {port(), port(), port()};
    final List<RaftPeer> initial =
        List.of(
            new RaftPeer("b0", "127.0.0.1", ports[0], true, 100),
            new RaftPeer("b1", "127.0.0.1", ports[1], false, 0),
            new RaftPeer("b2", "127.0.0.1", ports[2], false, 0));
    final List<Member> members = new ArrayList<>();
    try {
      for (int i = 0; i < 3; i++) members.add(start(initial, i));
      await(() -> members.getFirst().partition.role() == RaftRole.LEADER);
      final List<RaftPeer> promoted =
          initial.stream()
              .map(
                  peer ->
                      new RaftPeer(
                          peer.brokerId(),
                          peer.host(),
                          peer.port(),
                          true,
                          peer.brokerId().equals("b0") ? 100 : 10))
              .toList();
      members.getFirst().partition.updateRaftMembership(promoted).join(20, TimeUnit.SECONDS);
      await(
          () ->
              members.subList(1, 3).stream()
                  .allMatch(
                      member -> member.partition.node().getOptions().getElectionPriority() == 10));
      for (final Member member : members.subList(1, 3)) {
        member.partition.observeTopology(initial);
        assertEquals(10, member.partition.node().getOptions().getElectionPriority());
        final var field = NodeImpl.class.getDeclaredField("electionTimer");
        field.setAccessible(true);
        final Object timer = field.get(member.partition.node());
        final var stopped = timer.getClass().getSuperclass().getDeclaredField("stopped");
        stopped.setAccessible(true);
        assertFalse(stopped.getBoolean(timer), "promoted follower timer must run");
        ((RepeatedTimer) timer).stop();
        await(
            () -> {
              try {
                return !stopped.getBoolean(timer);
              } catch (final IllegalAccessException error) {
                throw new AssertionError(error);
              }
            });
        assertFalse(stopped.getBoolean(timer), "eligible follower repairs a stopped timer");
      }
      // Stop the leader's transport before shutdown: TimeoutNow cannot reach either follower.
      final var rpcField = JRaftServer.class.getDeclaredField("rpcServer");
      rpcField.setAccessible(true);
      ((RpcServer) rpcField.get(members.getFirst().server)).shutdown();
      // Suppress graceful wakeup by stopping its replicators before shutdown selects a candidate.
      final var replicators = NodeImpl.class.getDeclaredField("replicatorGroup");
      replicators.setAccessible(true);
      ((ReplicatorGroup) replicators.get(members.getFirst().partition.node())).stopAll();
      members.getFirst().server.close();
      await(
          () ->
              members.subList(1, 3).stream()
                  .anyMatch(member -> member.partition.role() == RaftRole.LEADER));
      final Member leader =
          members.subList(1, 3).stream()
              .filter(member -> member.partition.role() == RaftRole.LEADER)
              .findFirst()
              .orElseThrow();
      leader.partition.append(1, 1, new byte[] {1}).join(10, TimeUnit.SECONDS);
      assertEquals(3, leader.partition.members().size(), "failover must preserve peers");
    } finally {
      members.forEach(Member::close);
    }
  }

  @Test
  void rollingRestartAndColdRecoveryUsePersistedPriority() throws Exception {
    final int[] ports = {port(), port(), port()};
    final List<RaftPeer> peers =
        List.of(
            new RaftPeer("b0", "127.0.0.1", ports[0], true, 100),
            new RaftPeer("b1", "127.0.0.1", ports[1], true, 10),
            new RaftPeer("b2", "127.0.0.1", ports[2], true, 10));
    final List<Member> members = new ArrayList<>();
    try {
      for (int i = 0; i < 3; i++) members.add(start(peers, i));
      await(() -> members.stream().anyMatch(member -> member.partition.role() == RaftRole.LEADER));
      for (int i = 0; i < 3; i++) {
        members.get(i).close();
        members.set(i, start(peers, i));
        await(
            () -> members.stream().anyMatch(member -> member.partition.role() == RaftRole.LEADER));
        assertEquals(
            peers.get(i).priority(),
            members.get(i).partition.node().getOptions().getElectionPriority());
      }
      members.forEach(Member::close);
      members.clear();
      for (int i = 0; i < 3; i++) {
        // Simulate stale PD learner startup assignment against existing voter logs.
        final int local = i;
        final List<RaftPeer> stale =
            peers.stream()
                .map(
                    peer ->
                        peer.brokerId().equals("b" + local)
                            ? new RaftPeer(peer.brokerId(), peer.host(), peer.port(), false, 0)
                            : peer)
                .toList();
        members.add(start(stale, i));
      }
      await(() -> members.stream().anyMatch(member -> member.partition.role() == RaftRole.LEADER));
      for (int i = 0; i < 3; i++)
        assertEquals(
            peers.get(i).priority(),
            members.get(i).partition.node().getOptions().getElectionPriority());
    } finally {
      members.forEach(Member::close);
    }
  }

  private Member start(final List<RaftPeer> peers, final int index) throws Exception {
    final RaftPeer self = peers.get(index);
    final JRaftServer server = new JRaftServer(new Endpoint(self.host(), self.port()));
    final JRaftPartition partition =
        JRaftPartition.builder(
                RaftPartitionConfig.builder("election-recovery", 1, self.brokerId())
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
    } catch (final RuntimeException error) {
      partition.close();
      server.close();
      throw error;
    }
    return new Member(server, partition);
  }

  @Test
  void coldMajorityWithNewerLogsElectsWithinPriorityGrace() throws Exception {
    final List<RaftPeer> peers =
        List.of(
            new RaftPeer("b0", "127.0.0.1", port(), true, 100),
            new RaftPeer("b1", "127.0.0.1", port(), true, 10),
            new RaftPeer("b2", "127.0.0.1", port(), true, 10));
    final List<Member> members = new ArrayList<>();
    try {
      for (int i = 0; i < 3; i++) members.add(start(peers, i));
      await(() -> members.stream().anyMatch(m -> m.partition.role() == RaftRole.LEADER));
      final var initialLeader =
          members.stream()
              .filter(m -> m.partition.role() == RaftRole.LEADER)
              .findFirst()
              .orElseThrow();
      initialLeader.partition.append(1, 1, new byte[] {1}).join(10, TimeUnit.SECONDS);
      final long oldIndex = members.getFirst().partition.lastIndex();
      await(() -> members.stream().allMatch(m -> m.partition.lastAppliedIndex() >= oldIndex));
      members.getFirst().close();
      await(
          () ->
              members.subList(1, 3).stream().anyMatch(m -> m.partition.role() == RaftRole.LEADER));
      final var newerLeader =
          members.subList(1, 3).stream()
              .filter(m -> m.partition.role() == RaftRole.LEADER)
              .findFirst()
              .orElseThrow();
      newerLeader.partition.append(2, 2, new byte[] {2}).join(10, TimeUnit.SECONDS);
      final long newIndex = newerLeader.partition.lastIndex();
      assertTrue(newIndex > oldIndex);
      await(
          () ->
              members.subList(1, 3).stream()
                  .allMatch(m -> m.partition.lastAppliedIndex() >= newIndex));
      members.subList(1, 3).forEach(Member::close);
      members.clear();
      for (int i = 0; i < 3; i++) members.add(start(peers, i));
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(4);
      while (members.stream().noneMatch(m -> m.partition.role() == RaftRole.LEADER)
          && System.nanoTime() < deadline) Thread.sleep(25);
      assertTrue(
          members.stream().anyMatch(m -> m.partition.role() == RaftRole.LEADER),
          "newer-log majority must elect within the bounded priority grace");
      final var leader =
          members.stream()
              .filter(m -> m.partition.role() == RaftRole.LEADER)
              .findFirst()
              .orElseThrow();
      leader.partition.append(1, 1, new byte[] {42}).join(10, TimeUnit.SECONDS);
      assertEquals(3, leader.partition.members().size());
      assertEquals(10, members.get(1).partition.node().getOptions().getElectionPriority());
      assertEquals(10, members.get(2).partition.node().getOptions().getElectionPriority());
    } finally {
      members.forEach(Member::close);
    }
  }

  private static int port() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static void await(final BooleanSupplier condition) throws Exception {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(25);
    assertTrue(condition.getAsBoolean(), "condition did not converge within 20s");
  }

  private record Member(JRaftServer server, JRaftPartition partition) implements AutoCloseable {
    @Override
    public void close() {
      partition.close();
      server.close();
    }
  }
}
