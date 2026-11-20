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

import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.rpc.impl.core.DefaultRaftClientService;
import com.alipay.sofa.jraft.util.Endpoint;
import com.google.protobuf.Message;
import io.taskite.util.HostPort;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

/** Physical snapshot transport, with local aliases for JRaft's IPv4-only URI parser. */
final class JRaftSnapshotFileClient extends DefaultRaftClientService {
  private final Map<Endpoint, Endpoint> destinations = new ConcurrentHashMap<>();

  JRaftSnapshotFileClient(final NodeOptions options) {
    super(null, options.getAppendEntriesExecutors());
  }

  synchronized Endpoint register(final HostPort address) {
    final Endpoint physical = new Endpoint(HostPort.formatHost(address.host()), address.port());
    for (final var entry : destinations.entrySet()) {
      if (entry.getValue().equals(physical)) {
        return entry.getKey();
      }
    }
    // Only the built-in copier parses this alias. Neither DNS nor consensus sees it.
    final Endpoint alias = new Endpoint("snapshot-" + destinations.size() + ".taskite.invalid", 1);
    destinations.put(alias, physical);
    return alias;
  }

  @Override
  public boolean connect(final Endpoint alias) {
    return super.connect(physical(alias));
  }

  @Override
  public Future<Message> getFile(
      final Endpoint alias,
      final GetFileRequest request,
      final int timeout,
      final RpcResponseClosure<GetFileResponse> done) {
    return super.getFile(physical(alias), request, timeout, done);
  }

  Endpoint physical(final Endpoint alias) {
    final Endpoint target = destinations.get(alias);
    if (target == null) {
      throw new IllegalStateException("Unknown snapshot file server: " + alias);
    }
    return target;
  }

  @Override
  public void shutdown() {
    super.shutdown();
    clearDestinations();
  }

  void clearDestinations() {
    destinations.clear();
  }
}
