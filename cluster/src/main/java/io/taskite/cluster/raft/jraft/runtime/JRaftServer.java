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

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.StateMachine;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.Endpoint;
import io.taskite.cluster.raft.storage.RaftStorageLayout;
import io.taskite.util.HostPort;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns the single, shared gRPC {@link RpcServer} for a node and hosts every partition's jraft group
 * on it (migration decision D6: "broker 一进程 N 分区 → N 个 jraft group，共享一个 RpcServer"). Raft rides its
 * own endpoint (a dedicated port), separate from the business gRPC API.
 *
 * <p>The shared-server lifecycle mirrors jraft's multi-raft-group pattern (validated in the Phase 0
 * vertical-slice PoC): each group is created with {@link RaftGroupService#start(boolean)} passing
 * {@code false}, and the {@link RpcServer} is started exactly once via {@link #start()}. Groups
 * added after {@link #start()} start their own already-running server view.
 */
public final class JRaftServer implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(JRaftServer.class);

  private final Endpoint endpoint;
  private final RpcServer rpcServer;
  private final Map<String, RaftGroupService> groups = new ConcurrentHashMap<>();
  private final Map<String, Node> nodes = new ConcurrentHashMap<>();
  private volatile boolean started;

  /** Creates the shared raft RPC server bound to {@code raftEndpoint} (a dedicated raft port). */
  public JRaftServer(final Endpoint raftEndpoint) {
    Objects.requireNonNull(raftEndpoint, "raftEndpoint");
    endpoint = new Endpoint(HostPort.formatHost(raftEndpoint.getIp()), raftEndpoint.getPort());
    rpcServer = RaftRpcServerFactory.createRaftRpcServer(endpoint);
  }

  /**
   * Registers a jraft group on the shared server without starting the RPC server yet. Returns the
   * live {@link Node}. Call {@link #start()} once after all groups are registered.
   */
  public synchronized Node addGroup(
      final String groupId,
      final PeerId serverId,
      final Configuration initialConf,
      final StateMachine fsm,
      final RaftStorageLayout layout,
      final NodeOptions template) {
    Objects.requireNonNull(groupId, "groupId");
    if (groups.containsKey(groupId)) {
      throw new IllegalStateException("group already registered: " + groupId);
    }
    final NodeOptions source = template == null ? new NodeOptions() : template;
    final NodeOptions opts = source.copy();
    // NodeOptions.copy() in JRaft 1.4.1 does not copy the service factory.
    opts.setServiceFactory(source.getServiceFactory());
    opts.setFsm(fsm);
    opts.setInitialConf(initialConf);
    opts.setLogUri(layout.logDir().toString());
    opts.setRaftMetaUri(layout.metaDir().toString());
    opts.setSnapshotUri(layout.snapshotDir().toString());
    // Hoist the snapshot temp dir to the top-level pending/ sibling. jraft's LocalSnapshotStorage
    // honours a snapshotTempUri as long as it is not nested under the snapshot uri; a sibling dir
    // passes that check. Left unset, jraft would default temp to <snapshotUri>/temp.
    opts.setSnapshotTempUri(layout.tempDir().toString());

    // shareRpcServer=true so the group does not own the RpcServer's lifecycle.
    final RaftGroupService service = new RaftGroupService(groupId, serverId, opts, rpcServer, true);
    // startRpcServer=false: the shared server is started once in start().
    final Node node = service.start(false);
    groups.put(groupId, service);
    nodes.put(groupId, node);
    LOG.info("Registered jraft group {} on shared raft server {}", groupId, endpoint);
    return node;
  }

  /** Starts the shared RPC server once, after all initial groups are registered. */
  public synchronized void start() {
    if (started) {
      return;
    }
    final boolean initialized = rpcServer.init(null);
    final boolean rpcStarted = rpcServer.isStarted();
    final int boundPort = rpcServer.boundPort();
    if (!initialized || !rpcStarted || boundPort != endpoint.getPort()) {
      rpcServer.shutdown();
      throw new IllegalStateException(
          "Failed to start shared Raft RPC server on "
              + endpoint
              + ": initialized="
              + initialized
              + ", started="
              + rpcStarted
              + ", boundPort="
              + boundPort);
    }
    started = true;
    LOG.info(
        "Started shared raft RPC server on {} (boundPort={}) hosting {} group(s)",
        endpoint,
        boundPort,
        groups.size());
  }

  public boolean isStarted() {
    return started;
  }

  public Endpoint endpoint() {
    return endpoint;
  }

  /** Shuts down a single group (leaving the shared server and other groups running). */
  public void removeGroup(final String groupId) {
    final RaftGroupService service;
    final Node node;
    synchronized (this) {
      service = groups.remove(groupId);
      node = nodes.remove(groupId);
      if (service != null) {
        service.shutdown();
      }
    }
    // RaftGroupService.shutdown() is asynchronous; join the node so its threads/timers/log storage
    // are fully released before we return. Otherwise, in a process that creates many short-lived
    // servers (tests), the accumulating jraft resources starve the machine and stall appends.
    // The join runs OUTSIDE the monitor so a slow teardown never blocks other groups' add/remove
    // (C1: never hold a shared lock across a blocking wait).
    joinQuietly(node);
  }

  @Override
  public void close() {
    final List<Node> toJoin;
    synchronized (this) {
      for (final RaftGroupService service : groups.values()) {
        service.shutdown();
      }
      toJoin = new ArrayList<>(nodes.values());
      groups.clear();
      nodes.clear();
      rpcServer.shutdown();
      started = false;
    }
    // Join every node outside the monitor (see removeGroup).
    for (final Node node : toJoin) {
      joinQuietly(node);
    }
  }

  private static void joinQuietly(final Node node) {
    if (node == null) {
      return;
    }
    try {
      node.join();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
