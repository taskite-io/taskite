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

import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.InvokeContext;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileRequest;
import com.alipay.sofa.jraft.util.Endpoint;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves only the transport destination; protobuf PeerIds and persisted configurations stay
 * intact.
 */
final class JRaftResolvingRpcClient implements RpcClient {
  private static final Logger LOG = LoggerFactory.getLogger(JRaftResolvingRpcClient.class);
  private final RpcClient transport;
  private final JRaftEndpointDirectory directory;
  private final Consumer<Endpoint> invalidation;

  JRaftResolvingRpcClient(final RpcClient transport, final JRaftEndpointDirectory directory) {
    this.transport = transport;
    this.directory = directory;
    invalidation = transport::closeConnection;
    directory.onInvalidation(invalidation);
  }

  @Override
  public boolean init(final RpcOptions options) {
    return transport.init(options);
  }

  @Override
  public void shutdown() {
    directory.removeInvalidation(invalidation);
    transport.shutdown();
  }

  @Override
  public boolean checkConnection(final Endpoint member) {
    return checkConnection(member, false);
  }

  @Override
  public boolean checkConnection(final Endpoint member, final boolean create) {
    final Endpoint target;
    try {
      target = directory.resolve(member);
    } catch (final IllegalStateException unresolved) {
      LOG.debug("Raft connection unavailable: {}", unresolved.getMessage());
      return false;
    }
    return transport.checkConnection(target, create);
  }

  @Override
  public void closeConnection(final Endpoint member) {
    final Endpoint target;
    try {
      target = directory.resolve(member);
    } catch (final IllegalStateException unresolved) {
      return;
    }
    transport.closeConnection(target);
  }

  @Override
  public Object invokeSync(
      final Endpoint member, final Object request, final InvokeContext context, final long timeout)
      throws InterruptedException, RemotingException {
    return transport.invokeSync(target(member, request), request, context, timeout);
  }

  @Override
  public void invokeAsync(
      final Endpoint member,
      final Object request,
      final InvokeContext context,
      final InvokeCallback callback,
      final long timeout)
      throws InterruptedException, RemotingException {
    transport.invokeAsync(target(member, request), request, context, callback, timeout);
  }

  private Endpoint target(final Endpoint member, final Object request) throws RemotingException {
    // Snapshot URIs name a physical file server, potentially reusing another member's old port.
    if (request instanceof GetFileRequest) {
      return member;
    }
    try {
      return directory.resolve(member);
    } catch (final IllegalStateException unresolved) {
      throw new RemotingException(unresolved.getMessage(), unresolved);
    }
  }

  @Override
  public void registerConnectEventListener(final ReplicatorGroup group) {
    transport.registerConnectEventListener(new JRaftConnectionListener(group, directory));
  }
}
