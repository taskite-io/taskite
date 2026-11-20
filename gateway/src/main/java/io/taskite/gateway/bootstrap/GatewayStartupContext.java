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
package io.taskite.gateway.bootstrap;

import io.taskite.broker.client.BrokerClient;
import io.taskite.gateway.GatewayConfig;
import io.taskite.gateway.api.grpc.GrpcGatewayServer;
import io.taskite.gateway.broker.GatewayBrokerCommandDispatcher;
import io.taskite.gateway.interceptor.GatewayInterceptorRepository;
import io.taskite.gateway.routing.GatewayBrokerRouter;
import io.taskite.gateway.routing.GatewayTopologyCache;
import io.taskite.gateway.routing.GatewayTopologyManager;
import io.taskite.gateway.runtime.GatewayRuntime;
import io.taskite.placementdriver.client.Client;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Resource registry for ordered Gateway startup and reverse-order shutdown. */
public final class GatewayStartupContext implements AutoCloseable {
  private final GatewayConfig config;
  private final GatewayRuntime runtime;
  private final Map<Class<?>, Object> resources = new LinkedHashMap<>();
  private final Deque<RegisteredResource> closeOrder = new ArrayDeque<>();
  private boolean closed;

  public GatewayStartupContext(final GatewayConfig config) {
    this.config = Objects.requireNonNull(config, "config");
    runtime = new GatewayRuntime(config.threads().management());
  }

  public GatewayConfig config() {
    return config;
  }

  public GatewayRuntime runtime() {
    return runtime;
  }

  public Client placementDriverClient() {
    return requireResource(Client.class);
  }

  void placementDriverClient(final Client client) {
    registerResource(Client.class, client, client);
  }

  public BrokerClient brokerClient() {
    return requireResource(BrokerClient.class);
  }

  void brokerClient(final BrokerClient client) {
    registerResource(BrokerClient.class, client, client);
  }

  public GatewayTopologyCache topologyCache() {
    return requireResource(GatewayTopologyCache.class);
  }

  void topologyCache(final GatewayTopologyCache topologyCache) {
    registerValue(GatewayTopologyCache.class, topologyCache);
  }

  public GatewayBrokerRouter brokerRouter() {
    return requireResource(GatewayBrokerRouter.class);
  }

  void brokerRouter(final GatewayBrokerRouter brokerRouter) {
    registerValue(GatewayBrokerRouter.class, brokerRouter);
  }

  public GatewayTopologyManager topologyManager() {
    return requireResource(GatewayTopologyManager.class);
  }

  void topologyManager(final GatewayTopologyManager topologyManager) {
    registerResource(GatewayTopologyManager.class, topologyManager, topologyManager);
  }

  public GatewayBrokerCommandDispatcher brokerDispatcher() {
    return requireResource(GatewayBrokerCommandDispatcher.class);
  }

  void brokerDispatcher(final GatewayBrokerCommandDispatcher brokerDispatcher) {
    registerValue(GatewayBrokerCommandDispatcher.class, brokerDispatcher);
  }

  public GatewayInterceptorRepository interceptorRepository() {
    return requireResource(GatewayInterceptorRepository.class);
  }

  void interceptorRepository(final GatewayInterceptorRepository repository) {
    registerResource(GatewayInterceptorRepository.class, repository, repository);
  }

  void server(final GrpcGatewayServer server) {
    registerResource(GrpcGatewayServer.class, server, server);
  }

  public int serverPort() {
    return findResource(GrpcGatewayServer.class)
        .map(GrpcGatewayServer::port)
        .orElse(config.network().port());
  }

  public <T> void registerResource(
      final Class<T> type, final T resource, final AutoCloseable closeAction) {
    register(type, resource);
    closeOrder.push(
        new RegisteredResource(type, Objects.requireNonNull(closeAction, "closeAction")));
  }

  public <T> T requireResource(final Class<T> type) {
    return findResource(type)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Gateway startup resource is not initialized: " + type.getSimpleName()));
  }

  private <T> void registerValue(final Class<T> type, final T value) {
    register(type, value);
  }

  private <T> void register(final Class<T> type, final T value) {
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(value, "value");
    if (closed) {
      throw new IllegalStateException("Gateway startup context is already closed");
    }
    if (resources.putIfAbsent(type, value) != null) {
      throw new IllegalStateException(
          "Gateway startup resource is already initialized: " + type.getSimpleName());
    }
  }

  private <T> Optional<T> findResource(final Class<T> type) {
    return Optional.ofNullable(resources.get(type)).map(type::cast);
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    RuntimeException failure = null;
    while (!closeOrder.isEmpty()) {
      final RegisteredResource registered = closeOrder.pop();
      try {
        registered.closeAction().close();
      } catch (final Exception error) {
        if (failure == null) {
          failure =
              new IllegalStateException(
                  "Failed to close Gateway resource " + registered.type().getSimpleName(), error);
        } else {
          failure.addSuppressed(error);
        }
      }
    }
    resources.clear();
    if (failure != null) {
      throw failure;
    }
  }

  private record RegisteredResource(Class<?> type, AutoCloseable closeAction) {}
}
