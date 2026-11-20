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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.util.Endpoint;
import io.taskite.cluster.raft.RaftMember;
import io.taskite.cluster.raft.RaftPartitionConfig;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.membership.JRaftPeerMapper;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 5 integration test (migration "role events" + membership): on a real 3-node jraft cluster,
 * verifies the {@link JRaftPartition#addRoleChangeListener} and {@link
 * JRaftPartition#addLeaderChangeListener} seams fire on election — the leader observes {@code
 * LEADER} + its own id, followers observe {@code FOLLOWER} + the leader's id — matching the
 * in-house {@code RaftPartition} contract that drives the {@code StreamPartition} PROCESSING/REPLAY
 * lifecycle. Also exercises {@link JRaftPartition#updateRaftMembership} adding a learner.
 */
final class JRaftRoleAndMembershipClusterTest {

  private static final String CLUSTER_ID = "role-cluster";

  @Test
  void roleAndLeaderListenersFireAndMembershipUpdates(@TempDir final Path baseDir)
      throws Exception {
    final int[] ports = {freePort(), freePort(), freePort(), freePort()};
    final List<RaftPeer> voters =
        List.of(
            new RaftPeer("b0", "127.0.0.1", ports[0], true),
            new RaftPeer("b1", "127.0.0.1", ports[1], true),
            new RaftPeer("b2", "127.0.0.1", ports[2], true));

    final List<Member> members = new ArrayList<>();
    try {
      for (int i = 0; i < 3; i++) {
        members.add(Member.start(baseDir.resolve("node-" + i), voters.get(i), voters, ports[i]));
      }

      final Member leader = awaitLeader(members);
      assertNotNull(leader, "a leader must be elected");

      // Role events: the leader observed LEADER; every follower observed FOLLOWER.
      assertTrue(awaitRole(leader, RaftRole.LEADER), "leader's role listener must observe LEADER");
      // Leader-change events surface the member id (broker/node id), not the raft endpoint — the
      // business-facing contract every caller relies on.
      final String leaderPeer = leader.memberId;
      for (final Member member : members) {
        if (member == leader) {
          continue;
        }
        assertTrue(
            awaitRole(member, RaftRole.FOLLOWER),
            "follower " + member.memberId + " must observe FOLLOWER");
        // Leader-change events: followers converge on the elected leader's id.
        assertTrue(
            awaitLeaderId(member, leaderPeer), "follower must observe leader id " + leaderPeer);
      }
      // The leader's own leader-change listener resolves to its own id (a leader is its own
      // leader).
      assertTrue(awaitLeaderId(leader, leaderPeer), "leader must observe its own id as leader");

      // Members view: every node surfaces the raft-consistent configuration — all three voters
      // listed by member id, itself/leader as LEADER and the rest FOLLOWER. This is what the PD
      // /members endpoint reports (no per-node liveness, which is not raft-consistent).
      assertTrue(awaitMemberCount(leader, 3), "leader must list all 3 voters in members()");
      final List<RaftMember> leaderMembers = leader.partition.members();
      assertEquals(3, leaderMembers.size(), "members() must list all 3 voters");
      for (final RaftMember m : leaderMembers) {
        if (m.memberId().equals(leader.memberId)) {
          assertEquals(RaftRole.LEADER, m.role(), "self must be LEADER in members()");
        } else {
          assertEquals(RaftRole.FOLLOWER, m.role(), "peer " + m.memberId() + " must be FOLLOWER");
        }
      }

      final RaftPeer priorityTarget =
          voters.stream()
              .filter(peer -> !peer.brokerId().equals(leader.memberId))
              .findFirst()
              .orElseThrow();
      final List<RaftPeer> reprioritized =
          voters.stream()
              .map(
                  peer ->
                      new RaftPeer(
                          peer.brokerId(),
                          peer.host(),
                          peer.port(),
                          true,
                          peer.brokerId().equals(priorityTarget.brokerId()) ? 100 : 10))
              .toList();
      leader.partition.updateRaftMembership(reprioritized).join(15, SECONDS);
      assertTrue(
          awaitPriority(leader, priorityTarget, 100),
          "priority-only membership update must be committed to jraft");

      // Membership: add a 4th node as a learner via the leader, then confirm jraft accepts it.
      final RaftPeer learner = new RaftPeer("b3", "127.0.0.1", ports[3], false);
      final Member learnerMember =
          Member.start(baseDir.resolve("node-3"), learner, concat(voters, learner), ports[3]);
      members.add(learnerMember);

      final List<RaftPeer> newMembership = concat(voters, learner);
      leader.partition.updateRaftMembership(newMembership).join(15, SECONDS);

      // jraft records the learner in the group configuration.
      assertTrue(
          awaitLearnerListed(leader, learner),
          "the learner must appear in the leader's jraft learner set");
      final Member existingFollower =
          members.stream()
              .filter(member -> member != leader && !"b3".equals(member.memberId))
              .findFirst()
              .orElseThrow();
      existingFollower.partition.observePeerIdentities(newMembership);
      assertTrue(
          awaitMemberId(existingFollower, "b3"),
          "committed topology identities must resolve dynamic members on followers");

      final Member transferTarget =
          members.stream()
              .filter(member -> member != leader && !"b3".equals(member.memberId))
              .findFirst()
              .orElseThrow();
      leader.partition.transferLeadership(transferTarget.memberId).join(15, SECONDS);
      assertTrue(
          awaitSpecificLeader(transferTarget),
          "leadership must converge to the explicitly requested voter");
    } finally {
      for (final Member member : members) {
        member.close();
      }
    }
  }

  private static List<RaftPeer> concat(final List<RaftPeer> base, final RaftPeer extra) {
    final List<RaftPeer> all = new ArrayList<>(base);
    all.add(extra);
    return all;
  }

  /**
   * Reproduces the "Invalid conf entry" crash: when a node that was a <em>learner</em> in the
   * committed configuration is presented as a <em>voter</em> in a topology-driven update (e.g. a
   * replica dies and a learner is promoted to keep the replication factor), jraft validates the
   * merged (old ∪ new) configuration and rejects it because the promoted node appears in both the
   * old learner set and the new voter set. {@link JRaftPartition#updateRaftMembership} must
   * sequence the change (remove-learner, then change-peers) so the promotion is accepted without
   * ever presenting a peer as both a voter and a learner.
   */
  @Test
  void promotingLearnerToVoterDoesNotRaiseInvalidConfEntry(@TempDir final Path baseDir)
      throws Exception {
    final int[] ports = {freePort(), freePort(), freePort(), freePort()};
    final List<RaftPeer> voters =
        List.of(
            new RaftPeer("b0", "127.0.0.1", ports[0], true),
            new RaftPeer("b1", "127.0.0.1", ports[1], true),
            new RaftPeer("b2", "127.0.0.1", ports[2], true));
    final RaftPeer learnerPeer = new RaftPeer("b3", "127.0.0.1", ports[3], false);

    final List<Member> members = new ArrayList<>();
    try {
      for (int i = 0; i < 3; i++) {
        members.add(Member.start(baseDir.resolve("node-" + i), voters.get(i), voters, ports[i]));
      }
      final Member leader = awaitLeader(members);
      assertNotNull(leader, "a leader must be elected");

      // Bring b3 up and add it as a learner (catches up from the leader).
      final Member learnerNode =
          Member.start(
              baseDir.resolve("node-3"), learnerPeer, concat(voters, learnerPeer), ports[3]);
      members.add(learnerNode);
      leader.partition.updateRaftMembership(concat(voters, learnerPeer)).join(15, SECONDS);
      assertTrue(awaitLearnerListed(leader, learnerPeer), "b3 must first be recorded as a learner");

      // Promote b3 from learner to voter (topology now lists all four as replicas). Before the fix
      // this threw "Invalid conf entry" because b3 was still in the old learner set while presented
      // as a new voter. The sequenced reconfiguration must accept it.
      final List<RaftPeer> allVoters =
          List.of(
              new RaftPeer("b0", "127.0.0.1", ports[0], true),
              new RaftPeer("b1", "127.0.0.1", ports[1], true),
              new RaftPeer("b2", "127.0.0.1", ports[2], true),
              new RaftPeer("b3", "127.0.0.1", ports[3], true));
      leader.partition.updateRaftMembership(allVoters).join(15, SECONDS);

      final var promoted = JRaftPeerMapper.toPeerId(learnerPeer);
      assertTrue(
          awaitVoterListed(leader, promoted), "b3 must be promoted into the leader's voter set");
      assertTrue(
          !leader.partition.node().listLearners().contains(promoted),
          "b3 must no longer be a learner after promotion");
    } finally {
      for (final Member member : members) {
        member.close();
      }
    }
  }

  @Test
  void failedPromotionRestoresLearnerMembership(@TempDir final Path baseDir) throws Exception {
    final int[] ports = {freePort(), freePort(), freePort(), freePort()};
    final List<RaftPeer> voters =
        List.of(
            new RaftPeer("b0", "127.0.0.1", ports[0], true),
            new RaftPeer("b1", "127.0.0.1", ports[1], true),
            new RaftPeer("b2", "127.0.0.1", ports[2], true));
    final RaftPeer learnerPeer = new RaftPeer("b3", "127.0.0.1", ports[3], false);

    final List<Member> members = new ArrayList<>();
    try {
      for (int i = 0; i < 3; i++) {
        members.add(Member.start(baseDir.resolve("node-" + i), voters.get(i), voters, ports[i]));
      }
      final Member leader = awaitLeader(members);
      assertNotNull(leader, "a leader must be elected");

      leader.partition.updateRaftMembership(concat(voters, learnerPeer)).join(15, SECONDS);
      assertTrue(
          awaitLearnerListed(leader, learnerPeer),
          "the unavailable peer must first be recorded as a learner");

      final List<RaftPeer> promotedMembership =
          List.of(
              voters.get(0),
              voters.get(1),
              voters.get(2),
              new RaftPeer("b3", "127.0.0.1", ports[3], true));
      assertThrows(
          RuntimeException.class,
          () -> leader.partition.updateRaftMembership(promotedMembership).join(15, SECONDS));

      assertTrue(
          awaitLearnerListed(leader, learnerPeer),
          "a failed promotion must restore the previous learner membership");
      assertFalse(
          leader.partition.node().listPeers().contains(JRaftPeerMapper.toPeerId(learnerPeer)),
          "the failed promotion must not leave the unreachable peer as a voter");
    } finally {
      for (final Member member : members) {
        member.close();
      }
    }
  }

  /** A non-leader must fail explicitly so PD cannot mistake a no-op for an applied change. */
  @Test
  void membershipUpdateOnNonLeaderFailsExplicitly(@TempDir final Path baseDir) throws Exception {
    final int[] ports = {freePort(), freePort(), freePort()};
    final List<RaftPeer> voters =
        List.of(
            new RaftPeer("b0", "127.0.0.1", ports[0], true),
            new RaftPeer("b1", "127.0.0.1", ports[1], true),
            new RaftPeer("b2", "127.0.0.1", ports[2], true));

    final List<Member> members = new ArrayList<>();
    try {
      for (int i = 0; i < 3; i++) {
        members.add(Member.start(baseDir.resolve("node-" + i), voters.get(i), voters, ports[i]));
      }
      final Member leader = awaitLeader(members);
      assertNotNull(leader, "a leader must be elected");
      final Member follower = members.stream().filter(m -> m != leader).findFirst().orElseThrow();

      // Observation is independent of membership execution, which requires the leader.
      final List<RaftPeer> reordered =
          List.of(
              new RaftPeer("b2", "127.0.0.1", ports[2], true),
              new RaftPeer("b0", "127.0.0.1", ports[0], true),
              new RaftPeer("b1", "127.0.0.1", ports[1], true));
      final RuntimeException rejected =
          assertThrows(
              RuntimeException.class,
              () -> follower.partition.updateRaftMembership(reordered).join(5, SECONDS));
      assertTrue(rejected.getMessage().contains("NOT_LEADER"));
      assertEquals(3, follower.partition.members().size());
    } finally {
      for (final Member member : members) {
        member.close();
      }
    }
  }

  @Test
  void resetMembershipRecoversSingleSurvivorAfterQuorumLoss(@TempDir final Path baseDir)
      throws Exception {
    final int[] ports = {freePort(), freePort(), freePort()};
    final List<RaftPeer> voters =
        List.of(
            new RaftPeer("b0", "127.0.0.1", ports[0], true, 100),
            new RaftPeer("b1", "127.0.0.1", ports[1], true, 10),
            new RaftPeer("b2", "127.0.0.1", ports[2], true, 10));
    final List<Member> members = new ArrayList<>();
    try {
      for (int index = 0; index < voters.size(); index++) {
        members.add(
            Member.start(
                baseDir.resolve("reset-node-" + index), voters.get(index), voters, ports[index]));
      }
      assertNotNull(awaitLeader(members), "the original cluster must elect a leader");
      final Member survivor = members.getFirst();
      members.get(1).close();
      members.get(2).close();

      survivor.partition.resetRaftMembership(List.of(voters.getFirst())).join(15, SECONDS);

      assertTrue(
          awaitSpecificLeader(survivor),
          "the PD-fenced single survivor must elect itself after resetPeers");
      assertEquals(
          List.of(JRaftPeerMapper.toPeerId(voters.getFirst(), voters.getFirst().priority())),
          survivor.partition.node().listPeers());
    } finally {
      for (final Member member : members) {
        member.close();
      }
    }
  }

  @Test
  void promotedLearnerUpdatesLiveElectionPriority(@TempDir final Path baseDir) throws Exception {
    final int[] ports = {freePort(), freePort()};
    final RaftPeer leaderPeer = new RaftPeer("b0", "127.0.0.1", ports[0], true, 100);
    final RaftPeer learnerPeer = new RaftPeer("b1", "127.0.0.1", ports[1], false, 0);
    final List<RaftPeer> initialMembership = List.of(leaderPeer, learnerPeer);
    final List<Member> members = new ArrayList<>();
    try {
      final Member leader =
          Member.startWithPriority(
              baseDir.resolve("priority-node-0"), leaderPeer, initialMembership, ports[0]);
      final Member learner =
          Member.startWithPriority(
              baseDir.resolve("priority-node-1"), learnerPeer, initialMembership, ports[1]);
      members.add(leader);
      members.add(learner);
      assertNotNull(awaitLeader(members), "the initial voter must become leader");

      final List<RaftPeer> promotedMembership =
          List.of(leaderPeer, new RaftPeer("b1", "127.0.0.1", ports[1], true, 100));
      learner.partition.observeTopology(promotedMembership);
      assertEquals(0, learner.partition.node().getOptions().getElectionPriority());
      leader.partition.updateRaftMembership(promotedMembership).join(15, SECONDS);
      for (int i = 0;
          i < 200 && learner.partition.node().getOptions().getElectionPriority() != 100;
          i++) {
        Thread.sleep(25);
      }

      assertEquals(
          100,
          learner.partition.node().getNodeId().getPeerId().getPriority(),
          "the promoted node's live election priority must no longer be zero");

      leader.close();
      members.remove(leader);
      learner.partition.resetRaftMembership(List.of(promotedMembership.get(1))).join(15, SECONDS);
      assertTrue(
          awaitSpecificLeader(learner),
          "the promoted survivor must become leader after the PD-fenced resetPeers");
    } finally {
      for (final Member member : members) {
        member.close();
      }
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

  private static boolean awaitSpecificLeader(final Member member) throws InterruptedException {
    for (int i = 0; i < 200; i++) {
      if (member.partition.role() == RaftRole.LEADER) {
        return true;
      }
      Thread.sleep(50);
    }
    return member.partition.role() == RaftRole.LEADER;
  }

  private static boolean awaitRole(final Member member, final RaftRole expected)
      throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      if (expected == member.observedRole.get()) {
        return true;
      }
      Thread.sleep(50);
    }
    return expected == member.observedRole.get();
  }

  private static boolean awaitLeaderId(final Member member, final String expected)
      throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      if (expected.equals(member.observedLeaderId.get())) {
        return true;
      }
      Thread.sleep(50);
    }
    return expected.equals(member.observedLeaderId.get());
  }

  private static boolean awaitLearnerListed(final Member member, final RaftPeer learner)
      throws InterruptedException {
    final var learnerPeer = JRaftPeerMapper.toPeerId(learner);
    for (int i = 0; i < 100; i++) {
      if (member.partition.node().listLearners().contains(learnerPeer)) {
        return true;
      }
      Thread.sleep(50);
    }
    return member.partition.node().listLearners().contains(learnerPeer);
  }

  private static boolean awaitVoterListed(final Member member, final PeerId voter)
      throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      if (member.partition.node().listPeers().contains(voter)) {
        return true;
      }
      Thread.sleep(50);
    }
    return member.partition.node().listPeers().contains(voter);
  }

  private static boolean awaitPriority(
      final Member member, final RaftPeer target, final int expectedPriority)
      throws InterruptedException {
    final var targetPeer = JRaftPeerMapper.toPeerId(target);
    for (int i = 0; i < 100; i++) {
      final boolean matched =
          member.partition.node().listPeers().stream()
              .anyMatch(peer -> peer.equals(targetPeer) && peer.getPriority() == expectedPriority);
      if (matched) {
        return true;
      }
      Thread.sleep(50);
    }
    return false;
  }

  /** Waits until {@code member.members()} reports {@code expected} configured members. */
  private static boolean awaitMemberCount(final Member member, final int expected)
      throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      final List<RaftMember> observed = member.partition.members();
      if (observed.size() == expected) {
        return true;
      }
      Thread.sleep(50);
    }
    return false;
  }

  private static boolean awaitMemberId(final Member member, final String memberId)
      throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      if (member.partition.members().stream()
          .anyMatch(value -> memberId.equals(value.memberId()))) {
        return true;
      }
      Thread.sleep(20);
    }
    return false;
  }

  private static final TimeUnit SECONDS = TimeUnit.SECONDS;

  private static int freePort() throws IOException {
    try (final ServerSocket socket = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"))) {
      return socket.getLocalPort();
    }
  }

  /** A cluster member whose role/leader listeners record the latest observed values. */
  private static final class Member implements AutoCloseable {
    private final JRaftServer server;
    private final JRaftPartition partition;
    private final String memberId;
    private final AtomicReference<RaftRole> observedRole = new AtomicReference<>();
    private final AtomicReference<String> observedLeaderId = new AtomicReference<>();

    private Member(
        final JRaftServer server, final JRaftPartition partition, final String memberId) {
      this.server = server;
      this.partition = partition;
      this.memberId = memberId;
    }

    static Member start(
        final Path dir, final RaftPeer self, final List<RaftPeer> peers, final int raftPort)
        throws IOException {
      return start(dir, self, peers, raftPort, false);
    }

    static Member startWithPriority(
        final Path dir, final RaftPeer self, final List<RaftPeer> peers, final int raftPort)
        throws IOException {
      return start(dir, self, peers, raftPort, true);
    }

    private static Member start(
        final Path dir,
        final RaftPeer self,
        final List<RaftPeer> peers,
        final int raftPort,
        final boolean priorityEnabled)
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
          priorityEnabled
              ? JRaftPartition.builder(config, server, groupDir)
                  .snapshotBackend(null)
                  .priorityFn(RaftPeer::priority)
                  .build()
              : JRaftPartition.builder(config, server, groupDir).build();
      final Member member = new Member(server, partition, self.brokerId());
      partition.addRoleChangeListener(member.observedRole::set);
      partition.addLeaderChangeListener(member.observedLeaderId::set);
      try {
        partition.start();
        server.start();
        return member;
      } catch (final Throwable failure) {
        try {
          server.close();
        } catch (final Throwable cleanupFailure) {
          failure.addSuppressed(cleanupFailure);
        }
        throw failure;
      }
    }

    @Override
    public void close() {
      server.close();
    }
  }
}
