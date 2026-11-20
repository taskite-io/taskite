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
import io.grpc.Metadata;
import java.util.List;
import java.util.Objects;

/** Shared metadata and gRPC Context accessors for built-in and configured interceptors. */
public final class GatewayInterceptorUtil {
  public static final String CLUSTER_ID_HEADER = "x-cluster-id";
  public static final Metadata.Key<String> CLUSTER_ID_METADATA_KEY =
      Metadata.Key.of(CLUSTER_ID_HEADER, Metadata.ASCII_STRING_MARSHALLER);

  private static final Context.Key<String> CLUSTER_ID_CONTEXT_KEY =
      Context.key("gateway-cluster-id");
  private static final Context.Key<String> TENANT_ID_CONTEXT_KEY = Context.key("gateway-tenant-id");
  private static final Context.Key<List<String>> AUTHORIZED_TENANTS_CONTEXT_KEY =
      Context.key("gateway-authorized-tenants");

  private GatewayInterceptorUtil() {}

  public static String getClusterId() {
    return getClusterId(Context.current());
  }

  public static String getClusterId(final Context context) {
    return CLUSTER_ID_CONTEXT_KEY.get(Objects.requireNonNull(context, "context"));
  }

  public static String getTenantId() {
    return getTenantId(Context.current());
  }

  public static String getTenantId(final Context context) {
    return TENANT_ID_CONTEXT_KEY.get(Objects.requireNonNull(context, "context"));
  }

  public static Context withTenantId(final String tenantId) {
    if (tenantId == null || tenantId.isBlank()) {
      throw new IllegalArgumentException("tenantId must be present");
    }
    return Context.current()
        .withValues(
            TENANT_ID_CONTEXT_KEY, tenantId, AUTHORIZED_TENANTS_CONTEXT_KEY, List.of(tenantId));
  }

  static Context selectTenant(final Context context, final String tenantId) {
    return context.withValue(TENANT_ID_CONTEXT_KEY, tenantId);
  }

  /** Sets the authenticated tenant access; the first tenant remains the default for requests. */
  public static Context withAuthorizedTenantIds(final List<String> tenantIds) {
    if (tenantIds == null
        || tenantIds.isEmpty()
        || tenantIds.stream().anyMatch(id -> id == null || id.isBlank())) {
      throw new IllegalArgumentException("Authorized tenant IDs must be present and non-blank");
    }
    final var tenants = tenantIds.stream().distinct().toList();
    return Context.current()
        .withValues(
            TENANT_ID_CONTEXT_KEY, tenants.getFirst(), AUTHORIZED_TENANTS_CONTEXT_KEY, tenants);
  }

  public static List<String> getAuthorizedTenantIds(final Context context) {
    final var tenants = AUTHORIZED_TENANTS_CONTEXT_KEY.get(context);
    return tenants == null ? List.of() : tenants;
  }

  static Context withClusterId(final String clusterId) {
    return Context.current().withValue(CLUSTER_ID_CONTEXT_KEY, clusterId);
  }
}
