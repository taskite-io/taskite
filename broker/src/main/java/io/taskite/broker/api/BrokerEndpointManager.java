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
package io.taskite.broker.api;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.taskite.broker.partition.BrokerPartitionChangeExecutor;
import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.encoding.ExecuteCommandRequest;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.record.ExecuteAdminRequestDecoder;
import io.taskite.protocol.record.ExecuteCommandRequestDecoder;
import io.taskite.protocol.record.MessageHeaderDecoder;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.streamplatform.api.backpressure.BackpressureException;
import io.taskite.streamplatform.api.exception.ExceededBatchRecordSizeException;
import io.taskite.streamplatform.command.CommandRequestHandler;
import java.util.Objects;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Single application entry point shared by the Broker's transport adapters. */
public final class BrokerEndpointManager {
  private static final Logger LOG = LoggerFactory.getLogger(BrokerEndpointManager.class);
  private static final String KEY_IDENTITY_MISMATCH =
      "Broker command key %d does not belong to cluster '%s' (clusterKey %d), partition %d";
  private static final String INVALID_PARTITION =
      "Broker command partitionId must be in [1, %d], got %d";

  private final String clusterId;
  private final long clusterKey;
  private final CommandRequestHandler commandRequestHandler;
  private final BrokerAdminRequestHandler adminRequestHandler;

  public BrokerEndpointManager(
      final String clusterId,
      final long clusterKey,
      final CommandRequestHandler commandRequestHandler,
      final BrokerPartitionChangeExecutor partitionChangeExecutor) {
    this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
    Protocol.validateClusterKey(clusterKey);
    this.clusterKey = clusterKey;
    this.commandRequestHandler =
        Objects.requireNonNull(commandRequestHandler, "commandRequestHandler");
    adminRequestHandler = new BrokerAdminRequestHandler(clusterId, partitionChangeExecutor);
  }

  public void sendRequest(final byte[] request, final StreamObserver<byte[]> responseObserver) {
    final MessageHeaderDecoder header;
    try {
      if (request.length < MessageHeaderDecoder.ENCODED_LENGTH) {
        throw new IllegalArgumentException("truncated broker request header");
      }
      header = new MessageHeaderDecoder().wrap(new UnsafeBuffer(request), 0);
      if (header.schemaId() != ExecuteCommandRequestDecoder.SCHEMA_ID
          || header.version() > ExecuteCommandRequestDecoder.SCHEMA_VERSION) {
        throw new IllegalArgumentException("unsupported broker request schema or version");
      }
    } catch (final RuntimeException error) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(messageOf(error))
              .withCause(error)
              .asRuntimeException());
      return;
    }
    if (header.templateId() == ExecuteCommandRequestDecoder.TEMPLATE_ID) {
      executeCommand(request, responseObserver);
    } else if (header.templateId() == ExecuteAdminRequestDecoder.TEMPLATE_ID) {
      adminRequestHandler.handleRequest(request, responseObserver);
    } else {
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription("unsupported broker request template: " + header.templateId())
              .asRuntimeException());
    }
  }

  private void executeCommand(final byte[] request, final StreamObserver<byte[]> responseObserver) {
    final ExecuteCommandRequest command = new ExecuteCommandRequest();
    try {
      command.wrap(new UnsafeBuffer(request), 0, request.length);
      if (command.getValueType() == ValueType.BROKER_PARTITION_CONFIGURATION
          || command.getValueType() == ValueType.BROKER_PARTITION_OPERATION) {
        throw new IllegalArgumentException(
            "administration values require the administration request template");
      }
      if (command.getIntent() == JobIntent.YIELD) {
        throw new IllegalArgumentException("job yield is an internal partition command");
      }
      if (command.getPartitionId() < 1 || command.getPartitionId() > Protocol.MAXIMUM_PARTITIONS) {
        throw new IllegalArgumentException(
            INVALID_PARTITION.formatted(Protocol.MAXIMUM_PARTITIONS, command.getPartitionId()));
      }
      if (command.getKey() < 0) {
        throw new IllegalArgumentException(
            KEY_IDENTITY_MISMATCH.formatted(
                command.getKey(), clusterId, clusterKey, command.getPartitionId()));
      }
      if (command.getValueType() != ValueType.COMMAND_DISTRIBUTION
          && command.getKey() > 0
          && (Protocol.decodeClusterKey(command.getKey()) != clusterKey
              || Protocol.decodePartitionId(command.getKey()) != command.getPartitionId())) {
        throw new IllegalArgumentException(
            KEY_IDENTITY_MISMATCH.formatted(
                command.getKey(), clusterId, clusterKey, command.getPartitionId()));
      }
    } catch (final RuntimeException error) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription("Expected to decode broker command: " + messageOf(error))
              .withCause(error)
              .asRuntimeException());
      return;
    }
    if (command.getValueType() == ValueType.JOB_YIELD) {
      try {
        validateWorkerControl(command);
      } catch (final RuntimeException error) {
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription(messageOf(error))
                .withCause(error)
                .asRuntimeException());
        return;
      }
    }
    if (command.getClusterId().isBlank()) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription("clusterId must be present")
              .asRuntimeException());
      return;
    }
    if (!clusterId.equals(command.getClusterId())) {
      responseObserver.onError(
          Status.FAILED_PRECONDITION
              .withDescription(
                  "broker belongs to cluster `"
                      + clusterId
                      + "`, but request targets cluster `"
                      + command.getClusterId()
                      + "`")
              .asRuntimeException());
      return;
    }
    commandRequestHandler
        .handleRequest(request)
        .whenComplete(
            (response, error) -> {
              if (error != null) {
                final var backpressure = backpressureCause(error);
                if (backpressure == null) {
                  LOG.warn("broker command request failed: {}", error.getMessage(), error);
                } else {
                  LOG.debug("broker command rejected: {}", backpressure.getMessage());
                }
                final Status status =
                    ExceededBatchRecordSizeException.causedBy(error) || backpressure != null
                        ? Status.RESOURCE_EXHAUSTED
                        : Status.UNAVAILABLE;
                responseObserver.onError(
                    status
                        .withDescription(
                            backpressure == null ? messageOf(error) : backpressure.getMessage())
                        .withCause(error)
                        .asRuntimeException());
                return;
              }
              responseObserver.onNext(response);
              responseObserver.onCompleted();
            });
  }

  private static void validateWorkerControl(final ExecuteCommandRequest command) {
    if (command.getIntent() != JobYieldIntent.FENCE_SESSION
        && command.getIntent() != JobYieldIntent.SYNC_SESSIONS) {
      throw new IllegalArgumentException("unsupported external Worker control intent");
    }
    final var value = new JobYieldRecord();
    value.wrap(command.getValue());
    if (command.getPartitionId() <= 0
        || command.getPartitionId() > Protocol.MAXIMUM_PARTITIONS
        || !Objects.equals(command.getClusterId(), value.getClusterId())
        || command.getPartitionId() != value.getPartitionId()
        || (command.getKey() > 0
            && Protocol.decodePartitionId(command.getKey()) != command.getPartitionId())) {
      throw new IllegalArgumentException("Worker control envelope identity mismatch");
    }
    if (command.getIntent() == JobYieldIntent.FENCE_SESSION) {
      validateWorkerIdentity(value);
    } else {
      if (value.getPreviousSyncCursor() < 0
          || value.getSyncCursor() < value.getPreviousSyncCursor()
          || value.getSessions().size() > 100) {
        throw new IllegalArgumentException("Invalid worker session synchronization");
      }
      for (final var session : value.getSessions()) {
        validateWorkerIdentity(session);
        if (!Objects.equals(value.getClusterId(), session.getClusterId())
            || value.getPartitionId() != session.getPartitionId()) {
          throw new IllegalArgumentException("worker session synchronization identity mismatch");
        }
      }
    }
  }

  private static void validateWorkerIdentity(final JobYieldRecord value) {
    if (value.getYieldId() <= 0
        || value.getWorkerLeaseToken() <= 0
        || value.getWorkerId() == null
        || value.getWorkerId().isBlank()
        || value.getTenantId() == null
        || value.getTenantId().isBlank()
        || value.getReason() == null) {
      throw new IllegalArgumentException("Invalid Job yield identity");
    }
  }

  private static String messageOf(final Throwable error) {
    final String message = error.getMessage();
    return message == null || message.isBlank() ? error.getClass().getSimpleName() : message;
  }

  private static BackpressureException backpressureCause(final Throwable error) {
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      if (cause instanceof BackpressureException backpressure) {
        return backpressure;
      }
    }
    return null;
  }
}
