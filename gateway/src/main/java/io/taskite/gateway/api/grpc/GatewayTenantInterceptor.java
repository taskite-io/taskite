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

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.taskite.protocol.record.value.broker.TenantOwned;

/** Resolves the tenant after configured authentication interceptors have run. */
public final class GatewayTenantInterceptor implements ServerInterceptor {
  private static final Metadata.Key<String> JOB_TENANT =
      Metadata.Key.of("taskite-tenant-id", Metadata.ASCII_STRING_MARSHALLER);
  private final boolean multiTenancyEnabled;

  public GatewayTenantInterceptor(final boolean multiTenancyEnabled) {
    this.multiTenancyEnabled = multiTenancyEnabled;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {
    Context context;
    if (multiTenancyEnabled) {
      final String tenantId = GatewayInterceptorUtil.getTenantId();
      if (tenantId == null || tenantId.isBlank()) {
        call.close(
            Status.UNAUTHENTICATED.withDescription(
                "tenant identity was not provided by an authentication interceptor"),
            new Metadata());
        return new ServerCall.Listener<>() {};
      }
      context = Context.current();
    } else {
      context = GatewayInterceptorUtil.withTenantId(TenantOwned.DEFAULT_TENANT_IDENTIFIER);
    }
    final String requestedTenant = headers.get(JOB_TENANT);
    if (requestedTenant != null) {
      if (requestedTenant.isBlank()
          || !GatewayInterceptorUtil.getAuthorizedTenantIds(context).contains(requestedTenant)) {
        call.close(
            Status.PERMISSION_DENIED.withDescription("requested job tenant is not authorized"),
            new Metadata());
        return new ServerCall.Listener<>() {};
      }
      context = GatewayInterceptorUtil.selectTenant(context, requestedTenant);
    }
    return Contexts.interceptCall(context, call, headers, next);
  }
}
