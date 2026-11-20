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

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import io.taskite.cluster.raft.RaftMember;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.util.HostPort;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/** Maintains endpoint identity mappings and projects JRaft membership into domain member ids. */
public final class JRaftMemberView {
  private final String localMemberId;
  private final PeerId serverId;
  private final Configuration initialConfiguration;
  private final Supplier<Node> nodeSupplier;
  private final JRaftPeerMapper peerMapper;
  private final ConcurrentHashMap<String, RaftPeer> knownPeers = new ConcurrentHashMap<>();

  public JRaftMemberView(
      final String localMemberId,
      final PeerId serverId,
      final Configuration initialConfiguration,
      final List<RaftPeer> initialPeers,
      final Supplier<Node> nodeSupplier) {
    this(
        localMemberId,
        serverId,
        initialConfiguration,
        initialPeers,
        nodeSupplier,
        JRaftPeerMapper.physical());
  }

  public JRaftMemberView(
      final String localMemberId,
      final PeerId serverId,
      final Configuration initialConfiguration,
      final List<RaftPeer> initialPeers,
      final Supplier<Node> nodeSupplier,
      final JRaftPeerMapper peerMapper) {
    this.localMemberId = Objects.requireNonNull(localMemberId, "localMemberId");
    this.serverId = Objects.requireNonNull(serverId, "serverId");
    this.initialConfiguration =
        Objects.requireNonNull(initialConfiguration, "initialConfiguration");
    this.nodeSupplier = Objects.requireNonNull(nodeSupplier, "nodeSupplier");
    this.peerMapper = Objects.requireNonNull(peerMapper, "peerMapper");
    rememberPeers(initialPeers);
  }

  public String leaderId() {
    final Node node = nodeSupplier.get();
    final PeerId leader = node == null ? null : node.getLeaderId();
    return leader == null ? null : memberIdOf(leader);
  }

  public List<RaftMember> members() {
    final Node node = nodeSupplier.get();
    if (node == null) {
      return List.of(new RaftMember(localMemberId, RaftRole.FOLLOWER));
    }
    final Configuration configuration = currentConfiguration(node);
    return members(configuration);
  }

  public List<RaftMember> members(final Configuration configuration) {
    final String leaderId = leaderId();
    final List<RaftMember> members = new ArrayList<>();
    for (final PeerId peer : configuration.listPeers()) {
      final String memberId = memberIdOf(peer);
      members.add(
          new RaftMember(
              memberId, memberId.equals(leaderId) ? RaftRole.LEADER : RaftRole.FOLLOWER));
    }
    for (final PeerId learner : configuration.listLearners()) {
      members.add(new RaftMember(memberIdOf(learner), RaftRole.LEARNER));
    }
    members.sort(Comparator.comparing(RaftMember::memberId));
    return List.copyOf(members);
  }

  public void observeTopology(final List<RaftPeer> peers) {
    rememberPeers(peers);
  }

  public void observePeerIdentities(final List<RaftPeer> peers) {
    rememberPeers(peers);
  }

  public String memberIdOf(final String peerIdString) {
    final PeerId peerId = new PeerId();
    if (!peerId.parse(peerIdString)) {
      return peerIdString;
    }
    return memberIdOf(peerId);
  }

  public String memberIdOf(final PeerId peerId) {
    final String host = peerId.getEndpoint().getIp();
    final int port = peerId.getEndpoint().getPort();
    final RaftPeer peer = knownPeers.get(endpointKey(host, port));
    return peer == null ? peerId.toString() : peer.brokerId();
  }

  public Configuration currentConfiguration(final Node node) {
    return node instanceof NodeImpl nodeImpl ? nodeImpl.getCurrentConf() : initialConfiguration;
  }

  JRaftPeerMapper peerMapper() {
    return peerMapper;
  }

  private void rememberPeers(final List<RaftPeer> peers) {
    for (final RaftPeer peer : peers) {
      final var identity = peerMapper.peerId(peer).getEndpoint();
      knownPeers.put(endpointKey(identity.getIp(), identity.getPort()), peer);
    }
  }

  private static String endpointKey(final String host, final int port) {
    return HostPort.format(host, port);
  }
}
