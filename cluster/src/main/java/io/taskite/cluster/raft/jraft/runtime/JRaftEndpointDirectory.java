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
package io.taskite.cluster.raft.jraft.runtime;

import com.alipay.sofa.jraft.util.Endpoint;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.jraft.membership.JRaftPeerMapper;
import io.taskite.util.HostPort;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/** One group's immutable member identities and current physical transport addresses. */
public final class JRaftEndpointDirectory {
  private final String groupId;
  private final JRaftPeerMapper peerMapper;
  private volatile Map<Endpoint, Endpoint> endpoints = Map.of();
  private final CopyOnWriteArrayList<Consumer<Endpoint>> invalidations =
      new CopyOnWriteArrayList<>();

  public JRaftEndpointDirectory(
      final String groupId, final List<RaftPeer> peers, final JRaftPeerMapper peerMapper) {
    this.groupId = groupId;
    this.peerMapper = peerMapper;
    update(peers);
  }

  public synchronized void update(final List<RaftPeer> peers) {
    final Map<Endpoint, Endpoint> updated = new LinkedHashMap<>(endpoints);
    for (final RaftPeer peer : peers) {
      final Endpoint member = peerMapper.peerId(peer).getEndpoint();
      final Endpoint physical = endpoint(peer.host(), peer.port());
      final Endpoint previous = updated.put(member, physical);
      if (previous != null && !previous.equals(physical)) {
        // Publish the new target before invalidating the old channel. Subsequent RPCs reconnect
        // with the original peer identity in every protobuf request.
        endpoints = Map.copyOf(updated);
        invalidations.forEach(listener -> listener.accept(previous));
      }
    }
    endpoints = Map.copyOf(updated);
  }

  /** Frozen operation configurations can introduce a member but cannot regress a newer address. */
  public synchronized void remember(final List<RaftPeer> peers) {
    final Map<Endpoint, Endpoint> updated = new LinkedHashMap<>(endpoints);
    for (final RaftPeer peer : peers) {
      updated.putIfAbsent(
          peerMapper.peerId(peer).getEndpoint(), endpoint(peer.host(), peer.port()));
    }
    endpoints = Map.copyOf(updated);
  }

  public Endpoint resolve(final Endpoint member) {
    final Endpoint target = endpoints.get(endpoint(member.getIp(), member.getPort()));
    if (target == null) {
      throw new IllegalStateException(
          "Unknown Raft member in group directory: group=" + groupId + ", peer=" + member);
    }
    return target;
  }

  public List<Endpoint> identities(final Endpoint physical) {
    final List<Endpoint> identities = new ArrayList<>();
    final var target = endpoint(physical.getIp(), physical.getPort());
    endpoints.forEach(
        (identity, address) -> {
          if (address.equals(target)) {
            identities.add(identity);
          }
        });
    return List.copyOf(identities);
  }

  public void onInvalidation(final Consumer<Endpoint> listener) {
    invalidations.add(listener);
  }

  public void removeInvalidation(final Consumer<Endpoint> listener) {
    invalidations.remove(listener);
  }

  private static Endpoint endpoint(final String host, final int port) {
    return new Endpoint(HostPort.formatHost(host), port);
  }
}
