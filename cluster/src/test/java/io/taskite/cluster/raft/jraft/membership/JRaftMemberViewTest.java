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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class JRaftMemberViewTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void leaderProjectionUsesOneObservationDuringLeadershipChange(final boolean replaced) {
    final var server = PeerId.parsePeer("127.0.0.1:26502");
    final var next = PeerId.parsePeer("127.0.0.1:36502");
    final var observations = new AtomicInteger();
    final var node =
        (Node)
            Proxy.newProxyInstance(
                Node.class.getClassLoader(),
                new Class<?>[] {Node.class},
                (proxy, method, args) -> {
                  assertEquals("getLeaderId", method.getName());
                  return observations.getAndIncrement() == 0 ? server : replaced ? next : null;
                });
    final var view =
        new JRaftMemberView(
            "b0",
            server,
            new Configuration(List.of(server, next)),
            List.of(
                new RaftPeer("b0", "127.0.0.1", 26502, true),
                new RaftPeer("b1", "127.0.0.1", 36502, true)),
            () -> node);

    assertEquals("b0", view.leaderId());
  }

  @Test
  void parsedIpv6PeersRetainDomainMemberIdentity() {
    final var voter = new RaftPeer("b0", "2001:db8::1", 26502, true);
    final var learner = new RaftPeer("b1", "2001:db8::2", 26502, false);
    final var server = PeerId.parsePeer("[2001:db8::1]:26502");
    final var configuration =
        new Configuration(List.of(server), List.of(PeerId.parsePeer("[2001:db8::2]:26502")));
    final var view =
        new JRaftMemberView("b0", server, configuration, List.of(voter, learner), () -> null);

    assertEquals("b0", view.memberIdOf("[2001:db8::1]:26502"));
    assertEquals(
        List.of("b0", "b1"), view.members(configuration).stream().map(m -> m.memberId()).toList());
    assertEquals(RaftRole.LEARNER, view.members(configuration).get(1).role());
    assertEquals("[2001:db8::3]:26502", view.memberIdOf("[2001:db8::3]:26502"));
    assertEquals("invalid", view.memberIdOf("invalid"));
  }

  @Test
  void observedPeerReplacesIdentityAtTheSameEndpoint() {
    final var server = PeerId.parsePeer("127.0.0.1:26502");
    final var configuration = new Configuration(List.of(server));
    final var view =
        new JRaftMemberView(
            "old",
            server,
            configuration,
            List.of(new RaftPeer("old", "127.0.0.1", 26502, true)),
            () -> null);
    view.observePeerIdentities(List.of(new RaftPeer("new", "127.0.0.1", 26502, true)));
    assertEquals("new", view.memberIdOf(server));
    assertEquals("old", view.members().getFirst().memberId());
  }

  @Test
  void logicalProjectionSurvivesAddressChangesAndPortReuse() {
    final var mapper = JRaftPeerMapper.logical("demo");
    final var voter = new RaftPeer("b0", "127.0.0.1", 26502, true, 100);
    final var learner = new RaftPeer("b1", "::1", 36502, false, 0);
    final var configuration = mapper.configuration(List.of(voter, learner), RaftPeer::priority);
    final var view =
        new JRaftMemberView(
            "b0", mapper.peerId(voter), configuration, List.of(voter, learner), () -> null, mapper);
    view.observeTopology(
        List.of(
            new RaftPeer("b0", "::1", 46502, true, 100),
            new RaftPeer("b1", "127.0.0.1", 26502, false, 0)));

    assertEquals("b0", view.memberIdOf(mapper.peerId(voter)));
    assertEquals("b1", view.memberIdOf(mapper.peerId(learner)));
    assertEquals(
        List.of("b0", "b1"),
        view.members(configuration).stream().map(member -> member.memberId()).toList());
    assertEquals(RaftRole.LEARNER, view.members(configuration).get(1).role());
  }
}
