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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.grpc.Attributes;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.taskite.gateway.api.grpc.GatewayClusterHeaderInterceptor;
import io.taskite.gateway.api.grpc.GatewayInterceptorUtil;
import io.taskite.gateway.api.grpc.GatewayTenantInterceptor;
import io.taskite.protocol.record.value.broker.TenantOwned;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

final class GatewayInterceptorsTest {
  @Test
  void parsesExactlyOneValidClusterHeader() {
    final Metadata headers = new Metadata();
    headers.put(GatewayInterceptorUtil.CLUSTER_ID_METADATA_KEY, "cluster-a");
    final CapturingServerCall call = new CapturingServerCall();
    final CapturingHandler handler = new CapturingHandler();

    new GatewayClusterHeaderInterceptor().interceptCall(call, headers, handler);

    assertNull(call.status);
    assertEquals("cluster-a", handler.clusterId);
  }

  @Test
  void rejectsMissingDuplicateAndInvalidClusterHeaders() {
    assertRejected(new Metadata());

    final Metadata duplicates = new Metadata();
    duplicates.put(GatewayInterceptorUtil.CLUSTER_ID_METADATA_KEY, "cluster-a");
    duplicates.put(GatewayInterceptorUtil.CLUSTER_ID_METADATA_KEY, "cluster-b");
    assertRejected(duplicates);

    final Metadata invalid = new Metadata();
    invalid.put(GatewayInterceptorUtil.CLUSTER_ID_METADATA_KEY, "cluster a");
    assertRejected(invalid);
  }

  @Test
  void injectsDefaultTenantWhenMultiTenancyIsDisabled() {
    final CapturingServerCall call = new CapturingServerCall();
    final CapturingHandler handler = new CapturingHandler();

    new GatewayTenantInterceptor(false).interceptCall(call, new Metadata(), handler);

    assertNull(call.status);
    assertEquals(TenantOwned.DEFAULT_TENANT_IDENTIFIER, handler.tenantId);
  }

  @Test
  void requiresConfiguredInterceptorToProvideTenantWhenMultiTenancyIsEnabled() {
    final CapturingServerCall missingTenantCall = new CapturingServerCall();
    final CapturingHandler missingTenantHandler = new CapturingHandler();
    new GatewayTenantInterceptor(true)
        .interceptCall(missingTenantCall, new Metadata(), missingTenantHandler);

    assertEquals(Status.Code.UNAUTHENTICATED, missingTenantCall.status.getCode());
    assertFalse(missingTenantHandler.called);

    final CapturingServerCall authenticatedCall = new CapturingServerCall();
    final CapturingHandler authenticatedHandler = new CapturingHandler();
    final Context context = GatewayInterceptorUtil.withTenantId("tenant-a");
    final Context previous = context.attach();
    try {
      new GatewayTenantInterceptor(true)
          .interceptCall(authenticatedCall, new Metadata(), authenticatedHandler);
    } finally {
      context.detach(previous);
    }

    assertNull(authenticatedCall.status);
    assertEquals("tenant-a", authenticatedHandler.tenantId);
  }

  @Test
  void executesConfiguredInterceptorsBetweenClusterAndTenantInterceptors() {
    final List<String> observed = new ArrayList<>();
    final ServerInterceptor authentication =
        new ServerInterceptor() {
          @Override
          public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
              final ServerCall<ReqT, RespT> call,
              final Metadata headers,
              final ServerCallHandler<ReqT, RespT> next) {
            observed.add(GatewayInterceptorUtil.getClusterId());
            return Contexts.interceptCall(
                GatewayInterceptorUtil.withTenantId("tenant-a"), call, headers, next);
          }
        };
    final CapturingHandler handler = new CapturingHandler();
    final ServerServiceDefinition service =
        ServerServiceDefinition.builder("test.Service")
            .addMethod(CapturingServerCall.METHOD, handler)
            .build();
    final ServerServiceDefinition intercepted =
        ServerInterceptors.interceptForward(
            service,
            List.of(
                new GatewayClusterHeaderInterceptor(),
                authentication,
                new GatewayTenantInterceptor(true)));
    final Metadata headers = new Metadata();
    headers.put(GatewayInterceptorUtil.CLUSTER_ID_METADATA_KEY, "cluster-a");

    interceptedHandler(intercepted).startCall(new CapturingServerCall(), headers);

    assertEquals(List.of("cluster-a"), observed);
    assertEquals("cluster-a", handler.clusterId);
    assertEquals("tenant-a", handler.tenantId);
  }

  @SuppressWarnings("unchecked")
  private static ServerCallHandler<String, String> interceptedHandler(
      final ServerServiceDefinition service) {
    return (ServerCallHandler<String, String>)
        (ServerCallHandler<?, ?>)
            service
                .getMethod(CapturingServerCall.METHOD.getFullMethodName())
                .getServerCallHandler();
  }

  private static void assertRejected(final Metadata headers) {
    final CapturingServerCall call = new CapturingServerCall();
    final CapturingHandler handler = new CapturingHandler();
    new GatewayClusterHeaderInterceptor().interceptCall(call, headers, handler);
    assertEquals(Status.Code.INVALID_ARGUMENT, call.status.getCode());
    assertFalse(handler.called);
  }

  private static final class CapturingHandler implements ServerCallHandler<String, String> {
    private boolean called;
    private String clusterId;
    private String tenantId;

    @Override
    public ServerCall.Listener<String> startCall(
        final ServerCall<String, String> call, final Metadata headers) {
      called = true;
      clusterId = GatewayInterceptorUtil.getClusterId();
      tenantId = GatewayInterceptorUtil.getTenantId();
      return new ServerCall.Listener<>() {};
    }
  }

  private static final class CapturingServerCall extends ServerCall<String, String> {
    private static final MethodDescriptor.Marshaller<String> MARSHALLER =
        new MethodDescriptor.Marshaller<>() {
          @Override
          public InputStream stream(final String value) {
            return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
          }

          @Override
          public String parse(final InputStream stream) {
            try {
              return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            } catch (final Exception error) {
              throw new IllegalStateException(error);
            }
          }
        };
    static final MethodDescriptor<String, String> METHOD =
        MethodDescriptor.<String, String>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName("test.Service/Call")
            .setRequestMarshaller(MARSHALLER)
            .setResponseMarshaller(MARSHALLER)
            .build();

    private Status status;

    @Override
    public void request(final int numMessages) {}

    @Override
    public void sendHeaders(final Metadata headers) {}

    @Override
    public void sendMessage(final String message) {}

    @Override
    public void close(final Status status, final Metadata trailers) {
      this.status = status;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public MethodDescriptor<String, String> getMethodDescriptor() {
      return METHOD;
    }

    @Override
    public Attributes getAttributes() {
      return Attributes.EMPTY;
    }
  }
}
