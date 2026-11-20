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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.jraft.log.JRaftLogEntryCodec;
import io.taskite.cluster.raft.jraft.membership.JRaftPeerMapper;
import io.taskite.cluster.raft.log.PersistedRaftRecord;
import io.taskite.util.RaftMemberIdentity;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for the two pieces of real (non-skeleton) Phase 1 logic: the {@link RaftPeer} ↔
 * jraft {@link PeerId}/{@link Configuration} mapping (D6) and the position-span framing carried in
 * each jraft entry payload (D1).
 */
final class JRaftPeerMappingAndLogCodecTest {

  @Test
  void votersBecomePeersAndNonVotersBecomeLearners() {
    final List<RaftPeer> peers =
        List.of(
            new RaftPeer("b0", "10.0.0.1", 7000, true),
            new RaftPeer("b1", "10.0.0.2", 7000, true),
            new RaftPeer("b2", "10.0.0.3", 7000, false));

    final Configuration conf = JRaftPeerMapper.toConfiguration(peers);

    assertEquals(2, conf.getPeers().size(), "voters populate the peer set");
    assertEquals(1, conf.getLearners().size(), "non-voters populate the learner set");
    assertEquals(new PeerId("10.0.0.1", 7000), conf.getPeers().get(0));
    assertEquals(new PeerId("10.0.0.3", 7000), conf.getLearners().iterator().next());
  }

  @Test
  void peerMapsToEndpointPeerId() {
    final PeerId peerId = JRaftPeerMapper.toPeerId(new RaftPeer("b0", "192.168.1.5", 9000, true));
    assertEquals("192.168.1.5", peerId.getIp());
    assertEquals(9000, peerId.getPort());
  }

  @Test
  void peerIdentityRoundTripsWithAndWithoutPriority() {
    for (final String host :
        List.of("192.168.1.5", "broker.example", "2001:db8::1", "[2001:db8::1]", "::1")) {
      final RaftPeer peer = new RaftPeer("b0", host, 26502, true);
      for (final PeerId mapped :
          List.of(JRaftPeerMapper.toPeerId(peer), JRaftPeerMapper.toPeerId(peer, 10))) {
        final PeerId restored = new PeerId();
        assertTrue(restored.parse(mapped.toString()), "cannot restore " + mapped);
        assertEquals(mapped, restored);
        assertEquals(mapped.getPriority(), restored.getPriority());
      }
    }
  }

  @Test
  void groupIdCombinesClusterAndPartition() {
    assertEquals("cluster-a-partition-3", JRaftPeerMapper.groupId("cluster-a", 3));
  }

  @Test
  void logicalMemberIdentitySurvivesAddressChangesAndPreservesPriority() {
    final String identity = RaftMemberIdentity.of("cluster-a", "broker-0");
    final var mapper = JRaftPeerMapper.logical("cluster-a");
    final var original = new RaftPeer("broker-0", "127.0.0.1", 26502, true, 100);
    final var moved = new RaftPeer("broker-0", "::1", 36502, true, 100);
    assertEquals(mapper.peerId(original, 100), mapper.peerId(moved, 100));
    assertEquals(100, mapper.peerId(moved, 100).getPriority());
    assertEquals(
        mapper.configuration(List.of(original), RaftPeer::priority),
        mapper.configuration(List.of(moved), RaftPeer::priority));
    assertNotEquals(identity, RaftMemberIdentity.of("cluster-b", "broker-0"));
  }

  @Test
  void logicalMapperRejectsMissingClusterAndPhysicalMapperPreservesEndpoint() {
    assertThrows(NullPointerException.class, () -> JRaftPeerMapper.logical(null));
    assertThrows(IllegalArgumentException.class, () -> JRaftPeerMapper.logical(" "));
    final var peer = new RaftPeer("broker-0", "::1", 26502, true, 100);
    assertEquals(JRaftPeerMapper.toPeerId(peer, 100), JRaftPeerMapper.physical().peerId(peer, 100));
  }

  @Test
  void logicalIdentitySeparatesClustersBrokersAndAmbiguousConcatenations() {
    final String identity = RaftMemberIdentity.of("a", "bc");
    assertNotEquals(identity, RaftMemberIdentity.of("ab", "c"));
    assertNotEquals(identity, RaftMemberIdentity.of("a", "bd"));
    assertThrows(IllegalArgumentException.class, () -> RaftMemberIdentity.of("", "broker-0"));
    assertThrows(IllegalArgumentException.class, () -> RaftMemberIdentity.of("demo", " "));
    assertThrows(NullPointerException.class, () -> RaftMemberIdentity.of(null, "broker-0"));
  }

  @Test
  void positionSpanRoundTripsThroughPayloadFraming() {
    final byte[] payload = "record-bytes".getBytes(StandardCharsets.UTF_8);
    final ByteBuffer framed = JRaftLogEntryCodec.encode(5, 8, payload);

    assertEquals(5, JRaftLogEntryCodec.lowestPosition(framed));
    assertEquals(8, JRaftLogEntryCodec.highestPosition(framed));
    assertArrayEquals(payload, JRaftLogEntryCodec.payload(framed));
  }

  @Test
  void decodeMarriesJraftTermIndexWithCarriedSpan() {
    final byte[] payload = "batch".getBytes(StandardCharsets.UTF_8);
    final ByteBuffer framed = JRaftLogEntryCodec.encode(100, 103, payload);

    // jraft assigns term=2, index=42; the position span comes from the payload header.
    final PersistedRaftRecord record = JRaftLogEntryCodec.decode(framed, 2, 42);

    assertEquals(2, record.term());
    assertEquals(42, record.index());
    assertEquals(100, record.lowestPosition());
    assertEquals(103, record.highestPosition());
    assertArrayEquals(payload, record.data());
  }

  @Test
  void encodeHandlesEmptyPayload() {
    final ByteBuffer framed = JRaftLogEntryCodec.encode(1, 1, new byte[0]);
    assertEquals(JRaftLogEntryCodec.HEADER_LENGTH, framed.remaining());
    assertEquals(1, JRaftLogEntryCodec.lowestPosition(framed));
    assertArrayEquals(new byte[0], JRaftLogEntryCodec.payload(framed));
  }
}
