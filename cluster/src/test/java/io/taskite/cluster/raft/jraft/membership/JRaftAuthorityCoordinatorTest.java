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
package io.taskite.cluster.raft.jraft.membership;

import static org.junit.jupiter.api.Assertions.*;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import io.taskite.cluster.raft.RaftPeer;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

final class JRaftAuthorityCoordinatorTest {
  @ParameterizedTest
  @EnumSource(Admission.class)
  void promotionWaitsForAppliedConfigurationAndRetainsAuthorityFencing(
      final Admission admission, @TempDir final Path directory) throws Exception {
    final var self = new RaftPeer("b0", "127.0.0.1", 26502, true, 100);
    final var learner = new RaftPeer("b1", "127.0.0.1", 27502, false, 0);
    final var selfId = JRaftPeerMapper.toPeerId(self);
    final var learnerId = JRaftPeerMapper.toPeerId(learner);
    final var voters = new AtomicReference<>(List.of(selfId));
    final var learners = new AtomicReference<>(List.of(learnerId));
    final var committed = new AtomicLong(10);
    final var applied = new AtomicLong(10);
    final var leader = new AtomicBoolean(true);
    final var promotions = new AtomicInteger();
    final var authority = new JRaftExecutionAuthority(directory);
    authority.apply(1, "promotion", 10);
    final var node =
        (Node)
            Proxy.newProxyInstance(
                Node.class.getClassLoader(),
                new Class<?>[] {Node.class},
                (proxy, method, args) ->
                    switch (method.getName()) {
                      case "isLeader" -> leader.get();
                      case "getLastCommittedIndex" -> committed.get();
                      case "getLastAppliedLogIndex" -> applied.get();
                      case "listPeers" -> voters.get();
                      case "listLearners" -> learners.get();
                      case "removeLearners" -> {
                        assertEquals(List.of(learnerId), args[0]);
                        learners.set(List.of());
                        committed.set(11);
                        if (admission == Admission.SETTLED
                            || admission == Admission.APPLYING_VOTER_CHANGE
                            || admission == Admission.ROLLBACK_APPLYING) applied.set(11);
                        // JRaft invokes the closure before publishing lastAppliedLogIndex.
                        ((Closure) args[1]).run(Status.OK());
                        yield null;
                      }
                      case "changePeers" -> {
                        assertTrue(applied.get() >= committed.get());
                        assertTrue(authority.permits(1, "promotion"));
                        assertTrue(leader.get());
                        promotions.incrementAndGet();
                        if (admission == Admission.ROLLBACK_APPLYING) {
                          committed.set(12);
                          ((Closure) args[1])
                              .run(new Status(RaftError.ECATCHUP, "promotion catch-up failed"));
                          yield null;
                        }
                        final var configuration = (Configuration) args[0];
                        voters.set(configuration.listPeers());
                        learners.set(configuration.listLearners());
                        if (admission == Admission.APPLYING_VOTER_CHANGE) committed.set(12);
                        ((Closure) args[1]).run(Status.OK());
                        yield null;
                      }
                      case "addLearners" -> {
                        assertTrue(authority.permits(1, "promotion"));
                        assertTrue(applied.get() >= committed.get());
                        assertEquals(List.of(learnerId), args[0]);
                        learners.set(List.of(learnerId));
                        ((Closure) args[1]).run(Status.OK());
                        yield null;
                      }
                      default ->
                          throw new AssertionError("Unexpected Node method: " + method.getName());
                    });
    final var view =
        new JRaftMemberView(
            "b0",
            selfId,
            new Configuration(List.of(selfId), List.of(learnerId)),
            List.of(self, learner),
            () -> node);
    final var membership = new JRaftMembershipManager("test", selfId, () -> node, view);
    final var coordinator =
        new JRaftAuthorityCoordinator(() -> node, () -> 1, membership, view, authority);
    final var result =
        membership.updateMembership(
            List.of(self, new RaftPeer("b1", learner.host(), learner.port(), true, 10)),
            () -> coordinator.authorized(1, "promotion"));

    switch (admission) {
      case CLOSED -> membership.quiesce(authority::closeAdmission);
      case REPLACED -> {
        membership.quiesce(authority::closeAdmission);
        authority.apply(2, "successor", 12);
        authority.openAdmission();
        committed.set(12);
      }
      case FOLLOWER -> leader.set(false);
      default -> {}
    }
    applied.set(committed.get());

    if (admission == Admission.ROLLBACK_APPLYING) {
      final var failure =
          assertThrows(ExecutionException.class, () -> result.get(5, TimeUnit.SECONDS));
      assertTrue(failure.getCause().getMessage().contains("ECATCHUP"));
      assertEquals(1, promotions.get());
      assertEquals(List.of(selfId), voters.get());
      assertEquals(List.of(learnerId), learners.get());
    } else if (admission == Admission.SETTLED
        || admission == Admission.APPLYING
        || admission == Admission.APPLYING_VOTER_CHANGE) {
      result.get(5, TimeUnit.SECONDS);
      assertEquals(1, promotions.get());
      assertEquals(List.of(selfId, PeerId.parsePeer("127.0.0.1:27502::10")), voters.get());
      assertTrue(learners.get().isEmpty());
    } else {
      final var failure =
          assertThrows(ExecutionException.class, () -> result.get(5, TimeUnit.SECONDS));
      assertTrue(failure.getCause().getMessage().contains("EXECUTION_SUPERSEDED"));
      assertEquals(0, promotions.get());
      assertEquals(List.of(selfId), voters.get());
    }
  }

  private enum Admission {
    SETTLED,
    APPLYING,
    APPLYING_VOTER_CHANGE,
    ROLLBACK_APPLYING,
    CLOSED,
    REPLACED,
    FOLLOWER
  }
}
