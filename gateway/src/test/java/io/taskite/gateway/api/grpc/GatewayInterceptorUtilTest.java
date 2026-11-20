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

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.grpc.Context;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

final class GatewayInterceptorUtilTest {
  @Test
  void capturesIndependentRequestContextsAcrossConcurrentCalls() throws Exception {
    final CyclicBarrier barrier = new CyclicBarrier(2);
    try (var executor = Executors.newFixedThreadPool(2)) {
      final Future<CapturedContext> first =
          executor.submit(capture("cluster-a", "tenant-a", barrier));
      final Future<CapturedContext> second =
          executor.submit(capture("cluster-b", "tenant-b", barrier));

      assertEquals(new CapturedContext("cluster-a", "tenant-a"), first.get());
      assertEquals(new CapturedContext("cluster-b", "tenant-b"), second.get());
    }
  }

  private static Callable<CapturedContext> capture(
      final String clusterId, final String tenantId, final CyclicBarrier barrier) {
    return () -> {
      final Context clusterContext = GatewayInterceptorUtil.withClusterId(clusterId);
      final Context previousCluster = clusterContext.attach();
      try {
        final Context tenantContext = GatewayInterceptorUtil.withTenantId(tenantId);
        final Context previousTenant = tenantContext.attach();
        try {
          barrier.await();
          final Context captured = Context.current();
          barrier.await();
          return new CapturedContext(
              GatewayInterceptorUtil.getClusterId(captured),
              GatewayInterceptorUtil.getTenantId(captured));
        } finally {
          tenantContext.detach(previousTenant);
        }
      } finally {
        clusterContext.detach(previousCluster);
      }
    };
  }

  private record CapturedContext(String clusterId, String tenantId) {}
}
