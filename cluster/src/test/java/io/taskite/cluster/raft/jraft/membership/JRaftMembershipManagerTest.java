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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

final class JRaftMembershipManagerTest {
  private static final RaftPeer SELF = new RaftPeer("b0", "127.0.0.1", 26502, true, 10);
  private static final RaftPeer LEARNER = new RaftPeer("b1", "127.0.0.1", 27502, false, 0);

  @ParameterizedTest
  @EnumSource(VoterChange.class)
  void voterChangesRetainStandbyInEveryIntermediateConfiguration(final VoterChange change)
      throws Exception {
    final var voter = new RaftPeer("b2", "127.0.0.1", 28502, true, 10);
    final var standby = new RaftPeer("b3", "127.0.0.1", 29502, false, 0);
    final var currentVoters =
        new AtomicReference<>(
            List.of(JRaftPeerMapper.toPeerId(SELF), JRaftPeerMapper.toPeerId(voter)));
    final var currentLearners =
        new AtomicReference<>(
            change == VoterChange.PROMOTE
                ? List.of(JRaftPeerMapper.toPeerId(LEARNER), JRaftPeerMapper.toPeerId(standby))
                : List.of(JRaftPeerMapper.toPeerId(standby)));
    final var configurations = new ArrayList<Configuration>();
    final var standbyId = JRaftPeerMapper.toPeerId(standby);
    final var node =
        (Node)
            Proxy.newProxyInstance(
                Node.class.getClassLoader(),
                new Class<?>[] {Node.class},
                (proxy, method, args) ->
                    switch (method.getName()) {
                      case "isLeader" -> true;
                      case "listPeers" -> currentVoters.get();
                      case "listLearners" -> currentLearners.get();
                      case "removeLearners" -> {
                        final var removed = (List<?>) args[0];
                        assertFalse(removed.contains(standbyId));
                        currentLearners.set(
                            currentLearners.get().stream()
                                .filter(p -> !removed.contains(p))
                                .toList());
                        ((Closure) args[1]).run(Status.OK());
                        yield null;
                      }
                      case "changePeers" -> {
                        final var configuration = (Configuration) args[0];
                        assertEquals(List.of(standbyId), configuration.listLearners());
                        assertTrue(
                            configuration.listLearners().stream()
                                .noneMatch(currentVoters.get()::contains),
                            "demoted voters must join as learners after the voter change settles");
                        configurations.add(configuration.copy());
                        currentVoters.set(List.copyOf(configuration.listPeers()));
                        currentLearners.set(List.copyOf(configuration.listLearners()));
                        ((Closure) args[1]).run(Status.OK());
                        yield null;
                      }
                      case "addLearners" -> {
                        @SuppressWarnings("unchecked")
                        final var added = (List<PeerId>) args[0];
                        assertFalse(added.contains(standbyId), "retained standby needs no re-add");
                        final var learners = new ArrayList<>(currentLearners.get());
                        learners.addAll(added);
                        currentLearners.set(List.copyOf(learners));
                        ((Closure) args[1]).run(Status.OK());
                        yield null;
                      }
                      default ->
                          throw new AssertionError("Unexpected Node method: " + method.getName());
                    });
    final List<RaftPeer> target =
        switch (change) {
          case PRIORITY ->
              List.of(SELF, new RaftPeer("b2", voter.host(), voter.port(), true, 100), standby);
          case PROMOTE ->
              List.of(
                  SELF,
                  voter,
                  new RaftPeer("b1", LEARNER.host(), LEARNER.port(), true, 10),
                  standby);
          case DEMOTE ->
              List.of(SELF, new RaftPeer("b2", voter.host(), voter.port(), false, 0), standby);
        };
    manager(node).updateMembership(target).get(5, TimeUnit.SECONDS);
    assertEquals(1, configurations.size());
    final var expected = JRaftPeerMapper.toConfiguration(target, RaftPeer::priority);
    assertEquals(expected.getLearners(), new HashSet<>(currentLearners.get()));
    assertEquals(expected.getPeerSet(), new HashSet<>(currentVoters.get()));
  }

  private enum VoterChange {
    PRIORITY,
    PROMOTE,
    DEMOTE
  }

  @Test
  void closingAuthorityStopsPromotionAfterLearnerRemoval() throws Exception {
    final var fixture = new Fixture(true, 0, false);
    fixture.withholdCompletion = true;
    final var allowed = new AtomicBoolean(true);
    final var membership = manager(fixture.node);
    final var promotion =
        membership.updateMembership(
            List.of(SELF, new RaftPeer("b1", LEARNER.host(), LEARNER.port(), true, 10)),
            allowed::get);
    assertEquals(1, fixture.attempts.get());

    membership.quiesce(() -> allowed.set(false));
    fixture.delayedCompletion.get().run(Status.OK());

    final var failure =
        assertThrows(ExecutionException.class, () -> promotion.get(5, TimeUnit.SECONDS));
    assertTrue(failure.getCause().getMessage().contains("EXECUTION_SUPERSEDED"));
    assertEquals(1, fixture.attempts.get());
  }

  @Test
  void staleAuthorityCannotStartAnyMembershipStage() {
    final var fixture = new Fixture(false, 0, false);
    final var failure =
        assertThrows(
            ExecutionException.class,
            () ->
                manager(fixture.node)
                    .updateMembership(List.of(SELF, LEARNER), () -> false)
                    .get(5, TimeUnit.SECONDS));
    assertTrue(failure.getCause().getMessage().contains("EXECUTION_SUPERSEDED"));
    assertEquals(0, fixture.attempts.get());
  }

  @Test
  void learnerAdmissionCannotReplaceOrDemoteCurrentVoters() throws Exception {
    final var fixture = new Fixture(false, 0, false);
    final var membership = manager(fixture.node);
    membership
        .admitLearners(
            List.of(new RaftPeer(SELF.brokerId(), SELF.host(), SELF.port(), false, 0), LEARNER))
        .get(5, TimeUnit.SECONDS);
    assertEquals(1, fixture.attempts.get());
    final var failure =
        assertThrows(
            ExecutionException.class,
            () -> membership.admitLearners(List.of(SELF)).get(5, TimeUnit.SECONDS));
    assertInstanceOf(IllegalArgumentException.class, failure.getCause());
    assertEquals(1, fixture.attempts.get());
  }

  @Test
  void removeLearnersRetriesBusyConfigurationBeforeCompleting() throws Exception {
    final var fixture = new Fixture(true, 2, false);
    manager(fixture.node).updateMembership(List.of(SELF)).get(5, TimeUnit.SECONDS);
    assertEquals(3, fixture.attempts.get());
  }

  @Test
  void addLearnersRetriesBusyConfigurationBeforeCompleting() throws Exception {
    final var fixture = new Fixture(false, 1, false);
    manager(fixture.node).updateMembership(List.of(SELF, LEARNER)).get(5, TimeUnit.SECONDS);
    assertEquals(2, fixture.attempts.get());
  }

  @Test
  void retryStopsWhenLeadershipWasLost() throws Exception {
    final var fixture = new Fixture(false, 1, true);
    final var failure =
        assertThrows(
            ExecutionException.class,
            () ->
                manager(fixture.node)
                    .updateMembership(List.of(SELF, LEARNER))
                    .get(5, TimeUnit.SECONDS));
    assertTrue(failure.getCause().getMessage().contains("NOT_LEADER"));
    assertEquals(1, fixture.attempts.get());
  }

  @Test
  void permanentlyBusyConfigurationHasBoundedRetries() {
    final var fixture = new Fixture(false, -1, false);
    final var failure =
        assertThrows(
            ExecutionException.class,
            () ->
                manager(fixture.node)
                    .updateMembership(List.of(SELF, LEARNER))
                    .get(5, TimeUnit.SECONDS));
    assertTrue(failure.getCause().getMessage().contains("EBUSY"));
    assertEquals(21, fixture.attempts.get());
  }

  @Test
  void missingCallbackTimesOutAndLateCallbackCannotAdvanceMembership() throws Exception {
    final var fixture = new Fixture(true, 0, false);
    fixture.withholdCompletion = true;
    final var membership = manager(fixture.node);
    final var promotion =
        membership.updateMembership(
            List.of(
                SELF, new RaftPeer(LEARNER.brokerId(), LEARNER.host(), LEARNER.port(), true, 10)));
    final var failure =
        assertThrows(ExecutionException.class, () -> promotion.get(15, TimeUnit.SECONDS));
    assertTrue(failure.getCause().getMessage().contains("CONFIGURATION_TIMEOUT"));
    // The proxy rejects changePeers: a late removeLearners ACK must not continue promotion.
    fixture.delayedCompletion.get().run(Status.OK());
    fixture.withholdCompletion = false;
    membership.updateMembership(List.of(SELF)).get(5, TimeUnit.SECONDS);
    assertEquals(2, fixture.attempts.get());
  }

  private static final class Fixture {
    private final AtomicInteger attempts = new AtomicInteger();
    private final AtomicBoolean leader = new AtomicBoolean(true);
    private final AtomicReference<Closure> delayedCompletion = new AtomicReference<>();
    private boolean withholdCompletion;
    private final Node node;

    private Fixture(final boolean remove, final int busyCount, final boolean loseLeadership) {
      node =
          (Node)
              Proxy.newProxyInstance(
                  Node.class.getClassLoader(),
                  new Class<?>[] {Node.class},
                  (proxy, method, args) ->
                      switch (method.getName()) {
                        case "isLeader" -> leader.get();
                        case "getLastAppliedLogIndex", "getLastCommittedIndex" -> 0L;
                        case "listPeers" -> List.of(JRaftPeerMapper.toPeerId(SELF, 10));
                        case "listLearners" ->
                            remove ? List.of(JRaftPeerMapper.toPeerId(LEARNER)) : List.of();
                        case "removeLearners", "addLearners" -> {
                          assertEquals(remove ? "removeLearners" : "addLearners", method.getName());
                          assertEquals(List.of(JRaftPeerMapper.toPeerId(LEARNER)), args[0]);
                          final boolean busy =
                              attempts.incrementAndGet() <= busyCount || busyCount < 0;
                          if (loseLeadership) {
                            leader.set(false);
                          }
                          if (withholdCompletion) {
                            delayedCompletion.set((Closure) args[1]);
                          } else {
                            ((Closure) args[1])
                                .run(
                                    busy
                                        ? new Status(RaftError.EBUSY, "configuration finishing")
                                        : Status.OK());
                          }
                          yield null;
                        }
                        default ->
                            throw new AssertionError("Unexpected Node method: " + method.getName());
                      });
    }
  }

  private static JRaftMembershipManager manager(final Node node) {
    final var self = JRaftPeerMapper.toPeerId(SELF, 10);
    final var view =
        new JRaftMemberView(
            "b0", self, new Configuration(List.of(self)), List.of(SELF, LEARNER), () -> node);
    return new JRaftMembershipManager("test", self, () -> node, view);
  }
}
