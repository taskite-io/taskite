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
package io.taskite.gateway.api.grpc;

import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Owns the public Gateway gRPC server lifecycle. */
public final class GrpcGatewayServer implements AutoCloseable {
  private final String host;
  private final int configuredPort;
  private final GatewayCommandApi commandApi;
  private final Duration minKeepAliveInterval;
  private final List<ServerInterceptor> interceptors;
  private Server server;

  public GrpcGatewayServer(
      final String host,
      final int configuredPort,
      final GatewayCommandApi commandApi,
      final Duration minKeepAliveInterval,
      final List<ServerInterceptor> interceptors) {
    this.host = Objects.requireNonNull(host, "host");
    this.configuredPort = configuredPort;
    this.commandApi = Objects.requireNonNull(commandApi, "commandApi");
    this.minKeepAliveInterval =
        Objects.requireNonNull(minKeepAliveInterval, "minKeepAliveInterval");
    this.interceptors = List.copyOf(interceptors);
  }

  public void start() throws Exception {
    if (server != null) {
      return;
    }
    server =
        NettyServerBuilder.forAddress(new InetSocketAddress(host, configuredPort))
            .permitKeepAliveTime(minKeepAliveInterval.toMillis(), TimeUnit.MILLISECONDS)
            .addService(ServerInterceptors.interceptForward(commandApi, interceptors))
            .build()
            .start();
  }

  public int port() {
    return server == null ? configuredPort : server.getPort();
  }

  @Override
  public void close() throws Exception {
    if (server == null) {
      return;
    }
    server.shutdown();
    if (!server.awaitTermination(1, TimeUnit.SECONDS)) {
      server.shutdownNow();
      server.awaitTermination(5, TimeUnit.SECONDS);
    }
    server = null;
  }
}
