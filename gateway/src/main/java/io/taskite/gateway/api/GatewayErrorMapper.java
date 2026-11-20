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
package io.taskite.gateway.api;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.taskite.broker.client.response.BrokerError;
import io.taskite.broker.client.response.BrokerErrorException;
import io.taskite.broker.client.response.BrokerRejectionException;
import io.taskite.broker.client.response.BrokerRequestRejectedBeforeAppendException;
import io.taskite.broker.client.response.PartitionNotFoundException;
import io.taskite.broker.client.response.PartitionNotReadyException;
import io.taskite.gateway.common.ClusterTopologyUnavailableException;
import io.taskite.gateway.common.UnknownClusterException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/** Maps domain and transport failures to the public Gateway gRPC status contract. */
public final class GatewayErrorMapper {
  private GatewayErrorMapper() {}

  public static StatusRuntimeException toStatusException(final Throwable error) {
    final Throwable unwrapped = unwrap(error);
    if (unwrapped instanceof UnknownClusterException unknownCluster) {
      return Status.NOT_FOUND
          .withDescription(unknownCluster.getMessage())
          .withCause(unwrapped)
          .asRuntimeException();
    }
    if (unwrapped instanceof StatusRuntimeException statusRuntimeException) {
      return statusRuntimeException;
    }
    if (unwrapped instanceof StatusException statusException) {
      return statusException
          .getStatus()
          .withCause(unwrapped)
          .asRuntimeException(statusException.getTrailers());
    }
    return toStatus(unwrapped).asRuntimeException();
  }

  public static Status toStatus(final Throwable error) {
    final Throwable unwrapped = unwrap(error);
    if (unwrapped instanceof ClusterTopologyUnavailableException) {
      return Status.UNAVAILABLE.withDescription(messageOf(unwrapped)).withCause(unwrapped);
    }
    if (unwrapped instanceof UnknownClusterException) {
      return Status.NOT_FOUND.withDescription(messageOf(unwrapped)).withCause(unwrapped);
    }
    if (unwrapped instanceof CancellationException) {
      return Status.CANCELLED.withDescription(messageOf(unwrapped)).withCause(unwrapped);
    }
    if (unwrapped instanceof BrokerErrorException brokerErrorException) {
      return toStatus(brokerErrorException.getError());
    }
    if (unwrapped instanceof PartitionNotFoundException
        || unwrapped instanceof PartitionNotReadyException
        || unwrapped instanceof BrokerRequestRejectedBeforeAppendException) {
      return Status.UNAVAILABLE.withDescription(messageOf(unwrapped)).withCause(unwrapped);
    }
    if (unwrapped instanceof BrokerRejectionException brokerRejectionException) {
      final Status status =
          switch (brokerRejectionException.getRejection().type()) {
            case INVALID_ARGUMENT -> Status.INVALID_ARGUMENT;
            case NOT_FOUND -> Status.NOT_FOUND;
            case ALREADY_EXISTS -> Status.ALREADY_EXISTS;
            case INVALID_STATE, STALE_WORKER_SESSION, FEATURE_DISABLED ->
                Status.FAILED_PRECONDITION;
            case EXCEEDED_BATCH_RECORD_SIZE -> Status.RESOURCE_EXHAUSTED;
            case WORKER_SESSION_NOT_READY -> Status.UNAVAILABLE;
            case PROCESSING_ERROR, NULL_VAL, SBE_UNKNOWN -> Status.INTERNAL;
          };
      return status.withDescription(brokerRejectionException.getMessage()).withCause(unwrapped);
    }
    if (unwrapped instanceof IllegalArgumentException) {
      return Status.INVALID_ARGUMENT.withDescription(messageOf(unwrapped)).withCause(unwrapped);
    }
    if (unwrapped instanceof IllegalStateException) {
      return Status.FAILED_PRECONDITION.withDescription(messageOf(unwrapped)).withCause(unwrapped);
    }
    if (unwrapped instanceof TimeoutException) {
      return Status.DEADLINE_EXCEEDED.withDescription(messageOf(unwrapped)).withCause(unwrapped);
    }
    if (unwrapped instanceof StatusException statusException) {
      return statusException.getStatus().withCause(unwrapped);
    }
    if (unwrapped instanceof StatusRuntimeException statusRuntimeException) {
      return statusRuntimeException.getStatus().withCause(unwrapped);
    }
    return Status.INTERNAL.withDescription(messageOf(unwrapped)).withCause(unwrapped);
  }

  private static Status toStatus(final BrokerError error) {
    final Status status =
        switch (error.code()) {
          case PARTITION_LEADER_MISMATCH -> Status.FAILED_PRECONDITION;
          case RESOURCE_EXHAUSTED -> Status.RESOURCE_EXHAUSTED;
          case INTERNAL_ERROR, NULL_VAL, SBE_UNKNOWN -> Status.INTERNAL;
        };
    return status.withDescription(error.message());
  }

  private static Throwable unwrap(final Throwable error) {
    Throwable current = error;
    while ((current instanceof CompletionException || current instanceof ExecutionException)
        && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  private static String messageOf(final Throwable error) {
    final String message = error.getMessage();
    return message == null || message.isBlank() ? error.getClass().getSimpleName() : message;
  }
}
