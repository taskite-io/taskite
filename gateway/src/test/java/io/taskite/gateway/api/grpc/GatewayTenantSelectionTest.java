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

import static org.junit.jupiter.api.Assertions.*;

import io.grpc.*;
import io.taskite.protocol.record.value.broker.TenantOwned;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

final class GatewayTenantSelectionTest {
  private static final Metadata.Key<String> TENANT =
      Metadata.Key.of("taskite-tenant-id", Metadata.ASCII_STRING_MARSHALLER);

  @Test
  void jobTenantSelectsAuthorizedContextAndDoesNotChangeOuterIdentity() {
    var outer = GatewayInterceptorUtil.withAuthorizedTenantIds(List.of("a", "b"));
    outer.run(
        () -> {
          var call = new Call();
          var headers = new Metadata();
          headers.put(TENANT, "b");
          var selected = new AtomicReference<String>();
          var listener =
              new GatewayTenantInterceptor(true)
                  .interceptCall(
                      call,
                      headers,
                      (ignored, metadata) -> {
                        selected.set(GatewayInterceptorUtil.getTenantId());
                        assertEquals(
                            List.of("a", "b"),
                            GatewayInterceptorUtil.getAuthorizedTenantIds(Context.current()));
                        return new ServerCall.Listener<>() {
                          public void onMessage(Object message) {
                            selected.set(GatewayInterceptorUtil.getTenantId());
                          }
                        };
                      });
          assertEquals("b", selected.get());
          assertNull(call.status);
          assertEquals("a", GatewayInterceptorUtil.getTenantId());
          selected.set(null);
          listener.onMessage(new Object());
          assertEquals("b", selected.get());
          assertEquals("a", GatewayInterceptorUtil.getTenantId());
        });
  }

  @Test
  void unauthorizedAndBlankTenantNeverReachService() {
    GatewayInterceptorUtil.withAuthorizedTenantIds(List.of("a", "b"))
        .run(
            () -> {
              for (String tenant : List.of("c", "", " ")) {
                var call = new Call();
                var headers = new Metadata();
                headers.put(TENANT, tenant);
                new GatewayTenantInterceptor(true)
                    .interceptCall(
                        call,
                        headers,
                        (ignored, metadata) -> {
                          fail("Unauthorized tenant reached the service");
                          return null;
                        });
                assertEquals(Status.Code.PERMISSION_DENIED, call.status.getCode());
              }
            });
  }

  @Test
  void tenantHeaderCannotProvideAuthenticationAndDisabledModeUsesDefaultTenant() {
    var call = new Call();
    var headers = new Metadata();
    headers.put(TENANT, "a");
    new GatewayTenantInterceptor(true)
        .interceptCall(
            call,
            headers,
            (ignored, metadata) -> {
              fail("Tenant header supplied authentication");
              return null;
            });
    assertEquals(Status.Code.UNAUTHENTICATED, call.status.getCode());
    new GatewayTenantInterceptor(false)
        .interceptCall(
            new Call(),
            new Metadata(),
            (ignored, metadata) -> {
              assertEquals(
                  TenantOwned.DEFAULT_TENANT_IDENTIFIER, GatewayInterceptorUtil.getTenantId());
              return new ServerCall.Listener<>() {};
            });
  }

  private static final class Call extends ServerCall<Object, Object> {
    Status status;

    public void request(int count) {}

    public void sendHeaders(Metadata headers) {}

    public void sendMessage(Object response) {}

    public void close(Status status, Metadata trailers) {
      this.status = status;
    }

    public boolean isCancelled() {
      return false;
    }

    public MethodDescriptor<Object, Object> getMethodDescriptor() {
      return null;
    }
  }
}
