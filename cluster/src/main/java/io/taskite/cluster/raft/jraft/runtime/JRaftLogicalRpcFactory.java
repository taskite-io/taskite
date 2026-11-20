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

import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.impl.GrpcRaftRpcFactory;
import com.alipay.sofa.jraft.util.SPI;
import java.util.Objects;
import java.util.function.Supplier;

/** Public JRaft SPI; PD clients retain the standard gRPC transport outside a Broker group scope. */
@SPI(priority = 100)
public final class JRaftLogicalRpcFactory extends GrpcRaftRpcFactory {
  private static final ThreadLocal<JRaftEndpointDirectory> DIRECTORY = new ThreadLocal<>();

  public static <T> T inGroup(
      final JRaftEndpointDirectory directory, final Supplier<T> construction) {
    final var previous = DIRECTORY.get();
    DIRECTORY.set(Objects.requireNonNull(directory, "directory"));
    try {
      return construction.get();
    } finally {
      if (previous == null) {
        DIRECTORY.remove();
      } else {
        DIRECTORY.set(previous);
      }
    }
  }

  @Override
  public RpcClient createRpcClient(final ConfigHelper<RpcClient> helper) {
    final RpcClient transport = super.createRpcClient(helper);
    final var directory = DIRECTORY.get();
    return directory == null ? transport : new JRaftResolvingRpcClient(transport, directory);
  }

  public static <T> T withoutGroup(final Supplier<T> construction) {
    final var previous = DIRECTORY.get();
    DIRECTORY.remove();
    try {
      return construction.get();
    } finally {
      if (previous != null) {
        DIRECTORY.set(previous);
      }
    }
  }
}
