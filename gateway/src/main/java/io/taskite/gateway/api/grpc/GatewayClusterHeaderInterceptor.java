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

import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.Iterator;
import java.util.regex.Pattern;

/** Validates {@code x-cluster-id} and publishes it to the current gRPC Context. */
public final class GatewayClusterHeaderInterceptor implements ServerInterceptor {
  private static final int MAX_CLUSTER_ID_LENGTH = 128;
  private static final Pattern CLUSTER_ID = Pattern.compile("[A-Za-z0-9._-]+");

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {
    final Iterable<String> values = headers.getAll(GatewayInterceptorUtil.CLUSTER_ID_METADATA_KEY);
    if (values == null) {
      return reject(call, "x-cluster-id metadata must be provided exactly once");
    }

    final Iterator<String> iterator = values.iterator();
    if (!iterator.hasNext()) {
      return reject(call, "x-cluster-id metadata must be provided exactly once");
    }
    final String clusterId = iterator.next();
    if (iterator.hasNext()) {
      return reject(call, "x-cluster-id metadata must be provided exactly once");
    }
    if (clusterId == null
        || clusterId.isBlank()
        || clusterId.length() > MAX_CLUSTER_ID_LENGTH
        || !CLUSTER_ID.matcher(clusterId).matches()) {
      return reject(
          call,
          "x-cluster-id must match [A-Za-z0-9._-]+ and contain at most "
              + MAX_CLUSTER_ID_LENGTH
              + " characters");
    }

    return Contexts.interceptCall(
        GatewayInterceptorUtil.withClusterId(clusterId), call, headers, next);
  }

  private static <ReqT, RespT> ServerCall.Listener<ReqT> reject(
      final ServerCall<ReqT, RespT> call, final String description) {
    call.close(Status.INVALID_ARGUMENT.withDescription(description), new Metadata());
    return new ServerCall.Listener<>() {};
  }
}
