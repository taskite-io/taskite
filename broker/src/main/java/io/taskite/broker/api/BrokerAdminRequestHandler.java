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
import io.taskite.broker.partition.*;
import io.taskite.protocol.impl.encoding.ExecuteAdminRequest;
import io.taskite.protocol.impl.encoding.ExecuteAdminResponse;
import io.taskite.protocol.impl.record.value.broker.*;
import io.taskite.protocol.record.*;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.broker.BrokerPartitionConfigurationIntent;
import io.taskite.scheduler.future.ActorFuture;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import org.agrona.concurrent.UnsafeBuffer;

/** Cluster and partition administration; never submits administration to the engine queue. */
public final class BrokerAdminRequestHandler {
  private final String clusterId;
  private final BrokerPartitionChangeExecutor executor;

  public BrokerAdminRequestHandler(
      final String clusterId, final BrokerPartitionChangeExecutor executor) {
    this.clusterId = Objects.requireNonNull(clusterId);
    this.executor = Objects.requireNonNull(executor);
  }

  public void handleRequest(final byte[] bytes, final StreamObserver<byte[]> observer) {
    try {
      final var request = new ExecuteAdminRequest();
      request.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
      if (!clusterId.equals(request.getClusterId())) {
        observer.onError(
            Status.FAILED_PRECONDITION
                .withDescription("broker cluster identity mismatch")
                .asRuntimeException());
        return;
      }
      final ActorFuture<? extends UnifiedRecordValue> result;
      if (request.getValueType() == ValueType.BROKER_PARTITION_CONFIGURATION) {
        final var value = new BrokerPartitionConfigurationRecord();
        decodeValue(value, request);
        validateIdentity(request, value.getClusterId(), value.getPartitionId());
        if (request.getIntent() == BrokerPartitionConfigurationIntent.QUERY) {
          result =
              executor
                  .readConfiguration(clusterId, request.getPartitionId())
                  .thenApply(BrokerAdminRequestHandler::configurationValue);
        } else {
          if (value.getExpectedGeneration() < 0
              || value.getGeneration() <= value.getExpectedGeneration()
              || value.getOwnerChangeId().isBlank()
              || value.getExpectedRecoveryEpoch() < 0) {
            throw new IllegalArgumentException("invalid execution authority");
          }
          result =
              executor
                  .installAuthority(
                      clusterId,
                      request.getPartitionId(),
                      value.getExpectedGeneration(),
                      value.getGeneration(),
                      value.getOwnerChangeId(),
                      value.getExpectedRecoveryEpoch())
                  .thenApply(BrokerAdminRequestHandler::configurationValue);
        }
      } else {
        final var value = new BrokerPartitionOperationRecord();
        value.wrap(request.getValue());
        validateIdentity(request, value.getClusterId(), value.getPartitionId());
        if (value.getOpId().isBlank()) {
          throw new IllegalArgumentException("operation identity must be present");
        }
        final var operation = BrokerPartitionOperation.fromRecord(value, request.getIntent());
        result = executor.apply(operation).thenApply(ignored -> value.setApplied(true));
      }
      result.onComplete(
          (value, error) -> {
            if (error != null) {
              fail(observer, error);
              return;
            }
            try {
              final String cluster =
                  value instanceof BrokerPartitionConfigurationRecord configuration
                      ? configuration.getClusterId()
                      : ((BrokerPartitionOperationRecord) value).getClusterId();
              final int partition =
                  value instanceof BrokerPartitionConfigurationRecord configuration
                      ? configuration.getPartitionId()
                      : ((BrokerPartitionOperationRecord) value).getPartitionId();
              validateIdentity(request, cluster, partition);
              final var serialized = new UnsafeBuffer(new byte[value.getLength()]);
              value.write(serialized, 0);
              final var response =
                  new ExecuteAdminResponse()
                      .setClusterId(clusterId)
                      .setPartitionId(partition)
                      .setValueType(request.getValueType())
                      .setIntent(
                          Intent.fromProtocolValue(
                              request.getValueType(), (short) (request.getIntent().value() + 1)))
                      .setRecordType(RecordType.EVENT)
                      .setValue(serialized, 0, serialized.capacity());
              final var buffer = new UnsafeBuffer(new byte[response.getLength()]);
              response.write(buffer, 0);
              observer.onNext(buffer.byteArray());
              observer.onCompleted();
            } catch (final RuntimeException failure) {
              fail(observer, failure);
            }
          },
          Runnable::run);
    } catch (final RuntimeException error) {
      fail(observer, error);
    }
  }

  private static void decodeValue(
      final UnifiedRecordValue value, final ExecuteAdminRequest request) {
    try {
      value.wrap(request.getValue());
    } catch (final RuntimeException error) {
      throw new IllegalArgumentException("invalid administration record value", error);
    }
  }

  private static void validateIdentity(
      final ExecuteAdminRequest request, final String clusterId, final int partitionId) {
    if (!request.getClusterId().equals(clusterId) || request.getPartitionId() != partitionId) {
      throw new IllegalArgumentException("administration envelope and value identity mismatch");
    }
  }

  private static BrokerPartitionConfigurationRecord configurationValue(
      final BrokerPartitionConfiguration value) {
    return new BrokerPartitionConfigurationRecord(
        value.clusterId(),
        value.partitionId(),
        value.leaderId(),
        value.leaderTerm(),
        value.configurationIndex(),
        value.configurationDigest(),
        value.members(),
        value.recoveryEpoch(),
        value.executionGeneration(),
        value.ownerChangeId(),
        value.authorityIndex());
  }

  private static void fail(final StreamObserver<byte[]> observer, final Throwable error) {
    final Throwable cause =
        error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
    final Status status =
        cause instanceof IllegalArgumentException || cause instanceof IndexOutOfBoundsException
            ? Status.INVALID_ARGUMENT
            : Status.FAILED_PRECONDITION;
    observer.onError(
        status.withDescription(cause.getMessage()).withCause(cause).asRuntimeException());
  }
}
