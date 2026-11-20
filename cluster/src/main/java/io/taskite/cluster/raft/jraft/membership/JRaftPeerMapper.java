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

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.util.HostPort;
import io.taskite.util.RaftMemberIdentity;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.ToIntFunction;

/**
 * Maps immutable consensus identities to JRaft peers, preserving voters and election priorities.
 */
public final class JRaftPeerMapper {
  private static final JRaftPeerMapper PHYSICAL = new JRaftPeerMapper(null);
  private final String clusterId;

  private JRaftPeerMapper(final String clusterId) {
    this.clusterId = clusterId;
  }

  public static JRaftPeerMapper physical() {
    return PHYSICAL;
  }

  /** Broker consensus identity is always derived from the owning cluster and broker id. */
  public static JRaftPeerMapper logical(final String clusterId) {
    Objects.requireNonNull(clusterId, "clusterId");
    if (clusterId.isBlank()) {
      throw new IllegalArgumentException("clusterId must not be blank");
    }
    return new JRaftPeerMapper(clusterId);
  }

  /** Maps a single peer to its jraft {@link PeerId} (endpoint only; priority untouched). */
  public static PeerId toPeerId(final RaftPeer peer) {
    return PHYSICAL.peerId(peer);
  }

  public PeerId peerId(final RaftPeer peer) {
    Objects.requireNonNull(peer, "peer");
    final PeerId result = new PeerId();
    final String memberId =
        clusterId == null
            ? HostPort.format(peer.host(), peer.port())
            : RaftMemberIdentity.of(clusterId, peer.brokerId());
    if (!result.parse(memberId)) {
      throw new IllegalArgumentException("Invalid Raft member identity: " + memberId);
    }
    return result;
  }

  /**
   * Maps a peer to its jraft {@link PeerId} with an explicit election {@code priority} (ZEP006
   * priority election): jraft campaigns only while a node's own priority is {@code >=} the max
   * priority in the configuration, so encoding priorities into the shared {@link Configuration}
   * lets every node compute the same {@code targetPriority}. {@code priority <=
   * ElectionPriority.Disabled (-1)} disables priority for that peer.
   */
  public static PeerId toPeerId(final RaftPeer peer, final int priority) {
    return PHYSICAL.peerId(peer, priority);
  }

  public PeerId peerId(final RaftPeer peer, final int priority) {
    Objects.requireNonNull(peer, "peer");
    final PeerId result = peerId(peer);
    result.setPriority(priority);
    return result;
  }

  /**
   * Builds a jraft {@link Configuration} from the peer set: voters become configuration peers,
   * non-voters become learners.
   */
  public static Configuration toConfiguration(final List<RaftPeer> peers) {
    return toConfiguration(peers, null);
  }

  /**
   * Builds a jraft {@link Configuration} whose voter peers carry the election priorities computed
   * by {@code priorityFn} (ZEP006). When {@code priorityFn} is {@code null}, priorities are left
   * untouched (priority election disabled). Learners never lead, so they are not assigned a
   * priority.
   */
  public static Configuration toConfiguration(
      final List<RaftPeer> peers, final ToIntFunction<RaftPeer> priorityFn) {
    return PHYSICAL.configuration(peers, priorityFn);
  }

  public Configuration configuration(
      final List<RaftPeer> peers, final ToIntFunction<RaftPeer> priorityFn) {
    Objects.requireNonNull(peers, "peers");
    final List<PeerId> voters = new ArrayList<>();
    final List<PeerId> learners = new ArrayList<>();
    for (final RaftPeer peer : peers) {
      if (peer.voter()) {
        if (priorityFn != null && priorityFn.applyAsInt(peer) <= 0) {
          throw new IllegalArgumentException("Voter election priority must be positive");
        }
        voters.add(priorityFn == null ? peerId(peer) : peerId(peer, priorityFn.applyAsInt(peer)));
      } else {
        learners.add(peerId(peer));
      }
    }
    if (voters.isEmpty()) {
      throw new IllegalArgumentException("Raft membership must contain at least one voter");
    }
    return new Configuration(voters, learners);
  }

  /** The jraft group id for a partition, e.g. {@code "<clusterId>-partition-<id>"} (D6). */
  public static String groupId(final String clusterId, final int partitionId) {
    Objects.requireNonNull(clusterId, "clusterId");
    return clusterId + "-partition-" + partitionId;
  }
}
