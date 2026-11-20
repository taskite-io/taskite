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
package io.taskite.gateway;

import io.taskite.gateway.interceptor.GatewayInterceptorConfig;
import io.taskite.placementdriver.client.Endpoint;
import io.taskite.util.HostPort;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public record GatewayConfig(
    Network network,
    PlacementDriver placementDriver,
    Broker broker,
    Threads threads,
    MultiTenancy multiTenancy) {
  public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);
  public static final Duration DEFAULT_PD_REQUEST_TIMEOUT = Duration.ofSeconds(10);
  public static final int DEFAULT_MAX_RETRIES = 3;
  public static final Duration DEFAULT_MIN_KEEP_ALIVE_INTERVAL = Duration.ofSeconds(30);
  public static final Duration DEFAULT_TOPOLOGY_REFRESH_INTERVAL = Duration.ofSeconds(5);
  public static final int DEFAULT_MANAGEMENT_THREADS = 20;

  public GatewayConfig {
    Objects.requireNonNull(network, "network");
    Objects.requireNonNull(placementDriver, "placementDriver");
    Objects.requireNonNull(broker, "broker");
    Objects.requireNonNull(threads, "threads");
    Objects.requireNonNull(multiTenancy, "multiTenancy");
    if (multiTenancy.enabled() && multiTenancy.interceptors().isEmpty()) {
      throw new IllegalArgumentException(
          "gateway multi-tenancy requires at least one configured interceptor");
    }
  }

  public record Network(String host, int port, Duration minKeepAliveInterval) {
    public Network {
      host = host == null ? "" : HostPort.normalizeHost(host);
      if (host.isBlank()) {
        throw new IllegalArgumentException("gateway host must not be blank");
      }
      if (port < 0 || port > 65_535) {
        throw new IllegalArgumentException("gateway port must be in range 0..65535");
      }
      requirePositive(minKeepAliveInterval, "gateway minimum keepalive interval");
    }
  }

  public record PlacementDriver(
      String endpoints, Duration requestTimeout, Duration topologyRefreshInterval) {
    public PlacementDriver {
      endpoints = endpoints == null ? "" : endpoints.trim();
      validateEndpoints(endpoints);
      requirePositive(requestTimeout, "gateway placement-driver request timeout");
      requirePositive(topologyRefreshInterval, "gateway topology refresh interval");
      if (topologyRefreshInterval.compareTo(Duration.ofSeconds(1)) < 0) {
        throw new IllegalArgumentException("gateway topology refresh interval must be at least 1s");
      }
    }
  }

  public record Broker(Duration requestTimeout, int maxRetries) {
    public Broker {
      requirePositive(requestTimeout, "gateway broker request timeout");
      if (maxRetries < 0) {
        throw new IllegalArgumentException("gateway broker max retries must not be negative");
      }
    }
  }

  public record Threads(int management) {
    public Threads {
      if (management <= 0) {
        throw new IllegalArgumentException("gateway management threads must be positive");
      }
    }
  }

  public record MultiTenancy(boolean enabled, List<GatewayInterceptorConfig> interceptors) {
    public MultiTenancy {
      interceptors = List.copyOf(interceptors == null ? List.of() : interceptors);
    }
  }

  private static void requirePositive(final Duration value, final String name) {
    Objects.requireNonNull(value, name);
    if (value.isNegative() || value.toMillis() < 1) {
      throw new IllegalArgumentException(name + " must be at least 1ms");
    }
  }

  private static void validateEndpoints(final String placementDrivers) {
    final List<Endpoint> endpoints = new ArrayList<>();
    for (final String raw : placementDrivers.split(",")) {
      if (!raw.isBlank()) {
        endpoints.add(Endpoint.parse(raw));
      }
    }
    if (endpoints.isEmpty()) {
      throw new IllegalArgumentException("gateway requires at least one placement-driver endpoint");
    }
    for (final Endpoint endpoint : endpoints) {
      if (endpoint.host().isBlank() || endpoint.port() <= 0 || endpoint.port() > 65_535) {
        throw new IllegalArgumentException(
            "invalid placement-driver endpoint: " + endpoint.host() + ":" + endpoint.port());
      }
    }
  }
}
