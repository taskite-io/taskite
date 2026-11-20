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
package io.taskite.dist;

import io.taskite.gateway.GatewayConfig;
import io.taskite.gateway.interceptor.GatewayInterceptorConfig;
import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.ConstructorBinding;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** Immutable Spring configuration bound from the {@code taskite.gateway} namespace. */
@ConfigurationProperties(prefix = "taskite.gateway", ignoreUnknownFields = false)
public final class GatewayConfiguration {
  private final Network network;
  private final PlacementDriver placementDriver;
  private final Broker broker;
  private final Threads threads;
  private final MultiTenancy multiTenancy;
  private final List<Interceptor> interceptors;

  @ConstructorBinding
  public GatewayConfiguration(
      final Network network,
      final PlacementDriver placementDriver,
      final Broker broker,
      final Threads threads,
      final MultiTenancy multiTenancy,
      final List<Interceptor> interceptors) {
    this.network = network == null ? new Network(null, 26500, null) : network;
    this.placementDriver =
        placementDriver == null ? new PlacementDriver(null, null, null) : placementDriver;
    this.broker = broker == null ? new Broker(null, GatewayConfig.DEFAULT_MAX_RETRIES) : broker;
    this.threads =
        threads == null ? new Threads(GatewayConfig.DEFAULT_MANAGEMENT_THREADS) : threads;
    this.multiTenancy = multiTenancy == null ? new MultiTenancy(false) : multiTenancy;
    this.interceptors = List.copyOf(interceptors == null ? List.of() : interceptors);
  }

  public Network network() {
    return network;
  }

  public PlacementDriver placementDriver() {
    return placementDriver;
  }

  public Broker broker() {
    return broker;
  }

  public Threads threads() {
    return threads;
  }

  public MultiTenancy multiTenancy() {
    return multiTenancy;
  }

  public List<Interceptor> interceptors() {
    return interceptors;
  }

  public GatewayConfig toGatewayConfig() {
    return new GatewayConfig(
        new GatewayConfig.Network(network.host(), network.port(), network.minKeepAliveInterval()),
        new GatewayConfig.PlacementDriver(
            placementDriver.endpoints(),
            placementDriver.requestTimeout(),
            placementDriver.topologyRefreshInterval()),
        new GatewayConfig.Broker(broker.requestTimeout(), broker.maxRetries()),
        new GatewayConfig.Threads(threads.management()),
        new GatewayConfig.MultiTenancy(
            multiTenancy.enabled(),
            interceptors.stream()
                .filter(Interceptor::configured)
                .map(
                    interceptor ->
                        new GatewayInterceptorConfig(
                            interceptor.id(), interceptor.className(), interceptor.jarPath()))
                .toList()));
  }

  public record Network(String host, int port, Duration minKeepAliveInterval) {
    public Network(
        @DefaultValue("0.0.0.0") final String host,
        @DefaultValue("26500") final int port,
        @DefaultValue("30s") final Duration minKeepAliveInterval) {
      this.host = host == null ? "0.0.0.0" : host;
      this.port = port;
      this.minKeepAliveInterval =
          minKeepAliveInterval == null ? Duration.ofSeconds(30) : minKeepAliveInterval;
    }
  }

  public record PlacementDriver(
      String endpoints, Duration requestTimeout, Duration topologyRefreshInterval) {
    public PlacementDriver(
        @DefaultValue("127.0.0.1:26501") final String endpoints,
        @DefaultValue("10s") final Duration requestTimeout,
        @DefaultValue("5s") final Duration topologyRefreshInterval) {
      this.endpoints = endpoints == null ? "127.0.0.1:26501" : endpoints;
      this.requestTimeout =
          requestTimeout == null ? GatewayConfig.DEFAULT_PD_REQUEST_TIMEOUT : requestTimeout;
      this.topologyRefreshInterval =
          topologyRefreshInterval == null
              ? GatewayConfig.DEFAULT_TOPOLOGY_REFRESH_INTERVAL
              : topologyRefreshInterval;
    }
  }

  public record Broker(Duration requestTimeout, int maxRetries) {
    public Broker(
        @DefaultValue("30s") final Duration requestTimeout,
        @DefaultValue("3") final int maxRetries) {
      this.requestTimeout =
          requestTimeout == null ? GatewayConfig.DEFAULT_REQUEST_TIMEOUT : requestTimeout;
      this.maxRetries = maxRetries;
    }
  }

  public record Threads(int management) {
    public Threads(@DefaultValue("20") final int management) {
      this.management = management;
    }
  }

  public record MultiTenancy(boolean enabled) {
    public MultiTenancy(@DefaultValue("false") final boolean enabled) {
      this.enabled = enabled;
    }
  }

  public record Interceptor(String id, String className, String jarPath) {
    public Interceptor {
      id = id == null ? "" : id;
      className = className == null ? "" : className;
      jarPath = jarPath == null ? "" : jarPath;
    }

    private boolean configured() {
      return !id.isBlank() || !className.isBlank() || !jarPath.isBlank();
    }
  }
}
