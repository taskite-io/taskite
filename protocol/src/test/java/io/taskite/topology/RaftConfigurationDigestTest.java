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
package io.taskite.topology;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.topology.RaftConfigurationDigest.Member;
import io.taskite.util.HostPort;
import io.taskite.util.RaftMemberIdentity;
import java.util.List;
import org.junit.jupiter.api.Test;

final class RaftConfigurationDigestTest {
  @Test
  void registrationAndJRaftEndpointRepresentationsHaveSameDigest() {
    final var registered =
        List.of(
            new Member("broker-0", "2001:db8::1", 10101, true, 100),
            new Member("broker-1", "::1", 10583, false, 0),
            new Member("broker-2", "fe80::1%eth0", 26502, true, 10));
    final var jraft =
        registered.stream()
            .map(
                member ->
                    new Member(
                        member.brokerId(),
                        "[" + member.host() + "]",
                        member.port(),
                        member.voter(),
                        member.priority()))
            .toList();

    assertEquals(RaftConfigurationDigest.of(registered), RaftConfigurationDigest.of(jraft));
  }

  @Test
  void endpointNormalizationPreservesMembershipFences() {
    final var source = new Member("broker-0", "::1", 26502, true, 100);
    final String digest = RaftConfigurationDigest.of(List.of(source));
    for (final var changed :
        List.of(
            new Member("broker-1", "::1", 26502, true, 100),
            new Member("broker-0", "::2", 26502, true, 100),
            new Member("broker-0", "::1", 26503, true, 100),
            new Member("broker-0", "::1", 26502, false, 0),
            new Member("broker-0", "::1", 26502, true, 10),
            new Member("broker-0", "fe80::1%eth1", 26502, true, 100))) {
      assertNotEquals(digest, RaftConfigurationDigest.of(List.of(changed)));
    }
  }

  @Test
  void ipv4DigestRemainsCompatibleAndMemberOrderIsIrrelevant() {
    assertEquals(
        "f3e75282bc52893f01f6d7a3341a1bb2d86e035c6c69e22a308a72888b6ba8bc",
        RaftConfigurationDigest.of(List.of(new Member("broker-0", "127.0.0.1", 26502, true, 100))));
    final var voter = new Member("voter", "broker-0.internal", 26502, true, 100);
    final var learner = new Member("learner", "127.0.0.1", 26503, false, 0);
    assertEquals(
        RaftConfigurationDigest.of(List.of(voter, learner)),
        RaftConfigurationDigest.of(List.of(learner, voter)));
  }

  @Test
  void brokerDigestMatchesCommittedLogicalPeersAndFencesEveryMembershipField() {
    final var voter = new BrokerPartitionMember("broker-0", true, 100);
    final var learner = new BrokerPartitionMember("broker-1", false, 0);
    final var members = List.of(voter, learner);
    final String digest = RaftConfigurationDigest.ofBroker("demo", members);
    final var committed =
        members.stream()
            .map(
                member -> {
                  final var identity =
                      HostPort.parse(RaftMemberIdentity.of("demo", member.brokerId()));
                  return new Member(
                      member.brokerId(),
                      identity.host(),
                      identity.port(),
                      member.voter(),
                      member.priority());
                })
            .toList();
    assertEquals(RaftConfigurationDigest.of(committed), digest);
    assertEquals(digest, RaftConfigurationDigest.ofBroker("demo", List.of(learner, voter)));
    assertNotEquals(digest, RaftConfigurationDigest.ofBroker("other", members));
    for (final var changed :
        List.of(
            new BrokerPartitionMember("broker-2", true, 100),
            new BrokerPartitionMember("broker-0", false, 0),
            new BrokerPartitionMember("broker-0", true, 10))) {
      assertNotEquals(digest, RaftConfigurationDigest.ofBroker("demo", List.of(changed, learner)));
    }
  }
}
