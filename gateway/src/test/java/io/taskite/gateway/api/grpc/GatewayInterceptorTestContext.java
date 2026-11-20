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

/** Creates request contexts for tests that invoke Gateway endpoints without a gRPC server. */
public final class GatewayInterceptorTestContext {
  private GatewayInterceptorTestContext() {}

  public static void run(
      final String clusterId, final String tenantId, final Runnable requestAction) {
    final Context clusterContext = GatewayInterceptorUtil.withClusterId(clusterId);
    final Context previous = clusterContext.attach();
    try {
      GatewayInterceptorUtil.withTenantId(tenantId).run(requestAction);
    } finally {
      clusterContext.detach(previous);
    }
  }
}
