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
package io.taskite.broker.api.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.taskite.broker.api.BrokerEndpointManager;
import io.taskite.broker.client.transport.BrokerRequestGrpc;
import io.taskite.broker.partition.BrokerPartitionChangeExecutor;
import io.taskite.broker.partition.BrokerPartitionOperation;
import io.taskite.engine.Engine;
import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.encoding.ExecuteAdminRequest;
import io.taskite.protocol.impl.encoding.ExecuteAdminResponse;
import io.taskite.protocol.impl.encoding.ExecuteCommandRequest;
import io.taskite.protocol.impl.record.value.broker.BrokerPartitionOperationRecord;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.TopologyOperationType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.BrokerPartitionOperationIntent;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.streamplatform.api.backpressure.BackpressureException;
import io.taskite.streamplatform.api.exception.ExceededBatchRecordSizeException;
import io.taskite.streamplatform.command.CommandRequestHandler;
import io.taskite.topology.RaftConfigurationReference;
import io.taskite.topology.RecoveryFence;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class BrokerTopologyOperationGrpcTest {
  @Test
  void rejectsForeignClusterAndPartitionKeysEvenWhenTheClusterIdMatches() {
    final var submitted = new AtomicBoolean();
    final var handler =
        new CommandRequestHandler(
            command -> {
              submitted.set(true);
              return CompletableActorFuture.completed(command);
            },
            Engine.eventRegistry(),
            3);
    final var endpoint =
        new BrokerEndpointManager("demo", 3, handler, executor(new AtomicReference<>()));
    for (final long key : List.of(Protocol.encodeKey(2, 9, 1), Protocol.encodeKey(3, 10, 1))) {
      final var request =
          new ExecuteCommandRequest()
              .setKey(key)
              .setPartitionId(9)
              .setValueType(ValueType.JOB)
              .setIntent(JobIntent.COMPLETE);
      request.setClusterId("demo");
      final var bytes = new byte[request.getLength()];
      request.write(new UnsafeBuffer(bytes), 0);
      final var observer = new CapturingObserver();
      endpoint.sendRequest(bytes, observer);
      assertEquals(Status.Code.INVALID_ARGUMENT, observer.error.getStatus().getCode());
      assertFalse(submitted.get());
    }
  }

  @Test
  void backpressureRetainsRetryableStatusAndPrefixAcrossGrpc() throws Exception {
    for (final Throwable failure :
        List.of(
            new BackpressureException(1), new CompletionException(new BackpressureException(1)))) {
      final var handler =
          new CommandRequestHandler(
              ignored -> CompletableActorFuture.completedExceptionally(failure),
              Engine.eventRegistry(),
              0);
      final var server =
          new GrpcBrokerServer(
              0,
              new BrokerEndpointManager(
                  "cluster-a", 0, handler, executor(new AtomicReference<>())));
      server.start();
      final var channel =
          ManagedChannelBuilder.forAddress("127.0.0.1", server.port()).usePlaintext().build();
      try {
        final var value = new JobRecord().setJobType("test").setTenantId("tenant");
        final var buffer = new UnsafeBuffer(new byte[value.getLength()]);
        value.write(buffer, 0);
        final var request =
            new ExecuteCommandRequest()
                .setPartitionId(1)
                .setValueType(ValueType.JOB)
                .setIntent(JobIntent.COMPLETE);
        request.setClusterId("cluster-a");
        request.setValue(buffer, 0, buffer.capacity());
        final var bytes = new byte[request.getLength()];
        request.write(new UnsafeBuffer(bytes), 0);
        final var error =
            assertThrows(
                Exception.class,
                () ->
                    BrokerRequestGrpc.sendRequest(channel, bytes, Duration.ofSeconds(5))
                        .join(5, TimeUnit.SECONDS));
        final var status = Status.fromThrowable(error);
        assertEquals(Status.Code.RESOURCE_EXHAUSTED, status.getCode());
        assertTrue(status.getDescription().startsWith("BACKPRESSURE:"));
      } finally {
        channel.shutdownNow();
        server.close();
      }
    }
  }

  @Test
  void rejectsCommandForAnotherClusterBeforeSubmittingIt() {
    final AtomicBoolean submitted = new AtomicBoolean();
    final CommandRequestHandler commandHandler =
        new CommandRequestHandler(
            ignored -> {
              submitted.set(true);
              return CompletableActorFuture.completed(ignored);
            },
            Engine.eventRegistry(),
            0);
    final BrokerEndpointManager endpointManager =
        new BrokerEndpointManager(
            "cluster-a", 0, commandHandler, executor(new AtomicReference<>()));
    final ExecuteCommandRequest request =
        new ExecuteCommandRequest()
            .setPartitionId(1)
            .setValueType(ValueType.JOB)
            .setIntent(JobIntent.COMPLETE);
    request.setClusterId("cluster-b");
    final byte[] requestBytes = new byte[request.getLength()];
    request.write(new UnsafeBuffer(requestBytes), 0);
    final CapturingObserver observer = new CapturingObserver();

    endpointManager.sendRequest(requestBytes, observer);

    assertEquals(Status.Code.FAILED_PRECONDITION, observer.error.getStatus().getCode());
    assertTrue(observer.error.getStatus().getDescription().contains("cluster-b"));
    assertFalse(submitted.get());
  }

  @Test
  void mapsOversizedCommandPayloadToResourceExhausted() {
    final CommandRequestHandler commandHandler =
        new CommandRequestHandler(
            ignored ->
                CompletableActorFuture.completedExceptionally(
                    new ExceededBatchRecordSizeException(1025, 1024)),
            Engine.eventRegistry(),
            0);
    final BrokerEndpointManager endpointManager =
        new BrokerEndpointManager(
            "cluster-a", 0, commandHandler, executor(new AtomicReference<>()));
    final ExecuteCommandRequest request =
        new ExecuteCommandRequest()
            .setPartitionId(1)
            .setValueType(ValueType.JOB)
            .setIntent(JobIntent.COMPLETE);
    request.setClusterId("cluster-a");
    final JobRecord value = new JobRecord().setJobType("test").setTenantId("tenant");
    final UnsafeBuffer valueBuffer = new UnsafeBuffer(new byte[value.getLength()]);
    value.write(valueBuffer, 0);
    request.setValue(valueBuffer, 0, valueBuffer.capacity());
    final byte[] requestBytes = new byte[request.getLength()];
    request.write(new UnsafeBuffer(requestBytes), 0);
    final CapturingObserver observer = new CapturingObserver();

    endpointManager.sendRequest(requestBytes, observer);

    assertEquals(Status.Code.RESOURCE_EXHAUSTED, observer.error.getStatus().getCode());
  }

  @Test
  void dispatchesSbeOperationToExecutor() throws Exception {
    final AtomicReference<BrokerPartitionOperation> applied = new AtomicReference<>();
    final BrokerPartitionChangeExecutor executor = executor(applied);
    final CommandRequestHandler commandHandler =
        new CommandRequestHandler(
            ignored ->
                CompletableActorFuture.completedExceptionally(
                    new UnsupportedOperationException("not used")),
            Engine.eventRegistry(),
            0);
    final GrpcBrokerServer server =
        new GrpcBrokerServer(0, new BrokerEndpointManager("demo", 0, commandHandler, executor));
    server.start();
    final ManagedChannel channel =
        ManagedChannelBuilder.forAddress("127.0.0.1", server.port()).usePlaintext().build();
    try {
      final BrokerPartitionOperation request =
          new BrokerPartitionOperation(
              new BrokerPartitionOperation.Partition("demo", 1),
              new BrokerPartitionOperation.Operation(
                  "1:JOIN:broker-2:0", TopologyOperationType.JOIN, "broker-2", 0),
              new BrokerPartitionOperation.Configuration(
                  List.of(
                      new BrokerPartitionMember("broker-0", true),
                      new BrokerPartitionMember("broker-2", false)),
                  new RaftConfigurationReference(0, ""),
                  new RecoveryFence(0, 0, "")));

      final var value =
          new BrokerPartitionOperationRecord()
              .setClusterId(request.clusterId())
              .setPartitionId(request.partitionId())
              .setOpId(request.opId())
              .setTargetBroker(request.targetBroker())
              .setPriority(request.priority())
              .setMembers(request.members())
              .setExpectedMembershipEpoch(request.expectedMembershipEpoch())
              .setExpectedConfigurationDigest(request.expectedConfigurationDigest())
              .setExpectedRecoveryEpoch(request.expectedRecoveryEpoch())
              .setTargetRecoveryEpoch(request.targetRecoveryEpoch())
              .setFencingProofId(request.fencingProofId())
              .setExecutionGeneration(request.authority().generation())
              .setOwnerChangeId(request.authority().owner());
      final var payload = new UnsafeBuffer(new byte[value.getLength()]);
      value.write(payload, 0);
      final var wire =
          new ExecuteAdminRequest()
              .setClusterId(request.clusterId())
              .setPartitionId(request.partitionId())
              .setValueType(ValueType.BROKER_PARTITION_OPERATION)
              .setIntent(BrokerPartitionOperationIntent.JOIN)
              .setValue(payload, 0, payload.capacity());
      final var bytes = new UnsafeBuffer(new byte[wire.getLength()]);
      wire.write(bytes, 0);
      final byte[] responseBytes =
          BrokerRequestGrpc.sendRequest(channel, bytes.byteArray(), Duration.ofSeconds(5))
              .join(5, TimeUnit.SECONDS);
      final var envelope = new ExecuteAdminResponse();
      envelope.wrap(new UnsafeBuffer(responseBytes), 0, responseBytes.length);
      final var response = new BrokerPartitionOperationRecord();
      response.wrap(envelope.getValue());
      assertEquals(BrokerPartitionOperationIntent.JOINED, envelope.getIntent());
      assertEquals(request, applied.get());
      assertEquals(request.opId(), response.opId());
      assertTrue(response.applied());
    } finally {
      channel.shutdownNow();
      server.close();
    }
  }

  private static BrokerPartitionChangeExecutor executor(
      final AtomicReference<BrokerPartitionOperation> applied) {
    return new BrokerPartitionChangeExecutor() {
      @Override
      public ActorFuture<Void> add(final BrokerPartitionOperation request) {
        return complete(applied, request);
      }

      @Override
      public ActorFuture<Void> join(final BrokerPartitionOperation request) {
        return complete(applied, request);
      }

      @Override
      public ActorFuture<Void> promoteLearner(final BrokerPartitionOperation request) {
        return complete(applied, request);
      }

      @Override
      public ActorFuture<Void> leave(final BrokerPartitionOperation request) {
        return complete(applied, request);
      }
    };
  }

  private static ActorFuture<Void> complete(
      final AtomicReference<BrokerPartitionOperation> applied,
      final BrokerPartitionOperation request) {
    applied.set(request);
    return CompletableActorFuture.completed(null);
  }

  private static final class CapturingObserver implements StreamObserver<byte[]> {
    private StatusRuntimeException error;

    @Override
    public void onNext(final byte[] value) {}

    @Override
    public void onError(final Throwable error) {
      this.error = (StatusRuntimeException) error;
    }

    @Override
    public void onCompleted() {}
  }
}
