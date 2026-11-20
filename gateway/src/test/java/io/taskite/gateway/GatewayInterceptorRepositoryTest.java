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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.taskite.gateway.interceptor.GatewayInterceptorConfig;
import io.taskite.gateway.interceptor.GatewayInterceptorLoadException;
import io.taskite.gateway.interceptor.GatewayInterceptorRepository;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class GatewayInterceptorRepositoryTest {
  private static final List<String> CLOSED = new ArrayList<>();

  @BeforeEach
  void clearClosed() {
    CLOSED.clear();
  }

  @Test
  void loadsInConfigurationOrderAndClosesInReverseOrder() {
    final GatewayInterceptorRepository repository =
        new GatewayInterceptorRepository(
            List.of(
                config("first", FirstInterceptor.class),
                config("second", SecondInterceptor.class)));

    assertInstanceOf(FirstInterceptor.class, repository.interceptors().get(0));
    assertInstanceOf(SecondInterceptor.class, repository.interceptors().get(1));

    repository.close();
    assertEquals(List.of("second", "first"), CLOSED);
  }

  @Test
  void rejectsDuplicateIdsAndInvalidTypes() {
    assertThrows(
        GatewayInterceptorLoadException.class,
        () ->
            new GatewayInterceptorRepository(
                List.of(
                    config("duplicate", FirstInterceptor.class),
                    config("duplicate", SecondInterceptor.class))));
    assertThrows(
        GatewayInterceptorLoadException.class,
        () ->
            new GatewayInterceptorRepository(
                List.of(new GatewayInterceptorConfig("invalid", String.class.getName(), ""))));
  }

  @Test
  void requiresInterceptorWhenMultiTenancyIsEnabled() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new GatewayConfig(
                new GatewayConfig.Network("127.0.0.1", 26500, Duration.ofSeconds(30)),
                new GatewayConfig.PlacementDriver(
                    "127.0.0.1:26501", Duration.ofSeconds(10), Duration.ofSeconds(1)),
                new GatewayConfig.Broker(Duration.ofSeconds(5), 3),
                new GatewayConfig.Threads(GatewayConfig.DEFAULT_MANAGEMENT_THREADS),
                new GatewayConfig.MultiTenancy(true, List.of())));
  }

  private static GatewayInterceptorConfig config(
      final String id, final Class<? extends ServerInterceptor> type) {
    return new GatewayInterceptorConfig(id, type.getName(), "");
  }

  public static final class FirstInterceptor implements ServerInterceptor, AutoCloseable {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        final ServerCall<ReqT, RespT> call,
        final Metadata headers,
        final ServerCallHandler<ReqT, RespT> next) {
      return next.startCall(call, headers);
    }

    @Override
    public void close() {
      CLOSED.add("first");
    }
  }

  public static final class SecondInterceptor implements ServerInterceptor, AutoCloseable {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        final ServerCall<ReqT, RespT> call,
        final Metadata headers,
        final ServerCallHandler<ReqT, RespT> next) {
      return next.startCall(call, headers);
    }

    @Override
    public void close() {
      CLOSED.add("second");
    }
  }
}
