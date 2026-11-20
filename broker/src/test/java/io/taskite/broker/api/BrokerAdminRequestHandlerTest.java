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

import static org.junit.jupiter.api.Assertions.*;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.taskite.broker.partition.*;
import io.taskite.engine.Engine;
import io.taskite.protocol.impl.encoding.*;
import io.taskite.protocol.impl.record.value.broker.*;
import io.taskite.protocol.record.*;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.broker.*;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.streamplatform.command.CommandRequestHandler;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class BrokerAdminRequestHandlerTest {
  @Test
  void dispatchesReadAndInstallationWithoutSubmittingEngineCommands() {
    final var executor = new Executor();
    final var businessCalls = new AtomicInteger();
    final var manager = manager(executor, businessCalls);
    for (final var intent :
        List.of(
            BrokerPartitionConfigurationIntent.QUERY,
            BrokerPartitionConfigurationIntent.INSTALL_AUTHORITY)) {
      final var value = configuration();
      final var observer = new Observer();
      manager.sendRequest(
          frame(ValueType.BROKER_PARTITION_CONFIGURATION, intent, "demo", 7, value), observer);
      assertNull(observer.error);
      assertEquals(1, observer.completed);
      final var response = new ExecuteAdminResponse();
      response.wrap(new UnsafeBuffer(observer.response), 0, observer.response.length);
      assertEquals(intent.value() + 1, response.getIntent().value());
      final var decoded = new BrokerPartitionConfigurationRecord();
      decoded.wrap(response.getValue());
      assertEquals(executor.configuration.configurationIndex(), decoded.getConfigurationIndex());
      assertEquals(executor.configuration.authorityIndex(), decoded.getAuthorityIndex());
      assertEquals(executor.configuration.members(), decoded.getMembers());
    }
    assertEquals(1, executor.reads);
    assertEquals(1, executor.installs);
    assertEquals(0, businessCalls.get());
  }

  @Test
  void mapsAllEightOperationIntentsToImmutableExecutorParameters() {
    final var executor = new Executor();
    final var business = new AtomicInteger();
    final var manager = manager(executor, business);
    for (final var intent : BrokerPartitionOperationIntent.values()) {
      if (intent.isEvent()) {
        continue;
      }
      final var value =
          new BrokerPartitionOperationRecord()
              .setClusterId("demo")
              .setPartitionId(7)
              .setOpId("op-" + intent)
              .setTargetBroker("b2")
              .setPriority(100)
              .setMembers(List.of(new BrokerPartitionMember("b1", true, 100)))
              .setExpectedMembershipEpoch(9007199254740993L)
              .setExpectedConfigurationDigest("source")
              .setExpectedRecoveryEpoch(3)
              .setTargetRecoveryEpoch(4)
              .setFencingProofId("proof")
              .setExecutionGeneration(5)
              .setOwnerChangeId("owner");
      final var observer = new Observer();
      manager.sendRequest(
          frame(ValueType.BROKER_PARTITION_OPERATION, intent, "demo", 7, value), observer);
      assertNull(observer.error);
      assertEquals(TopologyOperationType.valueOf(intent.name()), executor.last.type());
      assertEquals(value.getMembers(), executor.last.members());
      assertEquals(value.getExpectedMembershipEpoch(), executor.last.expectedMembershipEpoch());
      assertEquals("source", executor.last.expectedConfigurationDigest());
      assertEquals(3, executor.last.expectedRecoveryEpoch());
      assertEquals(4, executor.last.targetRecoveryEpoch());
      assertEquals("proof", executor.last.fencingProofId());
      assertEquals(5, executor.last.authority().generation());
      assertEquals("owner", executor.last.authority().owner());
      assertEquals("b2", executor.last.targetBroker());
      assertEquals(100, executor.last.priority());
      final var response = new ExecuteAdminResponse();
      response.wrap(new UnsafeBuffer(observer.response), 0, observer.response.length);
      assertEquals(intent.value() + 1, response.getIntent().value());
      final var result = new BrokerPartitionOperationRecord();
      result.wrap(response.getValue());
      assertTrue(result.getApplied());
      assertEquals(value.getOpId(), result.getOpId());
    }
    assertEquals(8, executor.operations);
    assertEquals(0, business.get());
  }

  @Test
  void rejectsWrongClusterAndInnerIdentityBeforeExecuting() {
    final var executor = new Executor();
    final var handler = new BrokerAdminRequestHandler("demo", executor);
    final var wrong = new Observer();
    handler.handleRequest(
        frame(
            ValueType.BROKER_PARTITION_CONFIGURATION,
            BrokerPartitionConfigurationIntent.QUERY,
            "other",
            7,
            configuration().setClusterId("other")),
        wrong);
    assertEquals(Status.Code.FAILED_PRECONDITION, wrong.error.getStatus().getCode());
    for (final var value :
        List.of(configuration().setClusterId("other"), configuration().setPartitionId(8))) {
      final var observer = new Observer();
      handler.handleRequest(
          frame(
              ValueType.BROKER_PARTITION_CONFIGURATION,
              BrokerPartitionConfigurationIntent.QUERY,
              "demo",
              7,
              value),
          observer);
      assertEquals(Status.Code.INVALID_ARGUMENT, observer.error.getStatus().getCode());
      assertEquals(0, observer.completed);
    }
    assertEquals(0, executor.reads);
  }

  @Test
  void propagatesAsyncAndSynchronousFailuresWithoutAcknowledgingSuccess() {
    final var executor = new Executor();
    final var handler = new BrokerAdminRequestHandler("demo", executor);
    for (final boolean synchronous : List.of(false, true)) {
      executor.synchronous = synchronous;
      executor.fail = true;
      final var observer = new Observer();
      handler.handleRequest(
          frame(
              ValueType.BROKER_PARTITION_CONFIGURATION,
              BrokerPartitionConfigurationIntent.QUERY,
              "demo",
              7,
              configuration()),
          observer);
      assertEquals(Status.Code.FAILED_PRECONDITION, observer.error.getStatus().getCode());
      assertEquals("NO_QUORUM", observer.error.getStatus().getDescription());
      assertNull(observer.response);
      assertEquals(0, observer.completed);
    }
  }

  @Test
  void rejectsUnknownTruncatedFramesAndMalformedValuesAtCommonEntry() {
    final var executor = new Executor();
    final var manager = manager(executor, new AtomicInteger());
    final var request =
        frame(
            ValueType.BROKER_PARTITION_CONFIGURATION,
            BrokerPartitionConfigurationIntent.QUERY,
            "demo",
            7,
            configuration());
    final var unknown = request.clone();
    new MessageHeaderEncoder().wrap(new UnsafeBuffer(unknown), 0).templateId(999);
    for (final var bytes :
        List.of(new byte[0], unknown, Arrays.copyOf(request, request.length - 1))) {
      final var observer = new Observer();
      manager.sendRequest(bytes, observer);
      assertEquals(Status.Code.INVALID_ARGUMENT, observer.error.getStatus().getCode());
    }
    assertEquals(0, executor.reads);
    final var invalid =
        new ExecuteAdminRequest()
            .setClusterId("demo")
            .setPartitionId(7)
            .setValueType(ValueType.BROKER_PARTITION_CONFIGURATION)
            .setIntent(BrokerPartitionConfigurationIntent.QUERY)
            .setValue(new UnsafeBuffer(new byte[] {(byte) '{'}), 0, 1);
    final var buffer = new UnsafeBuffer(new byte[invalid.getLength()]);
    invalid.write(buffer, 0);
    final var observer = new Observer();
    manager.sendRequest(buffer.byteArray(), observer);
    assertEquals(Status.Code.INVALID_ARGUMENT, observer.error.getStatus().getCode());
    assertEquals(0, executor.reads);
  }

  @Test
  void rejectsInvalidInstallationBeforeMutatingAuthority() {
    final var executor = new Executor();
    final var handler = new BrokerAdminRequestHandler("demo", executor);
    for (final var value :
        List.of(
            configuration().setGeneration(4),
            configuration().setExpectedGeneration(-1),
            configuration().setOwnerChangeId(""),
            configuration().setExpectedRecoveryEpoch(-1))) {
      final var observer = new Observer();
      handler.handleRequest(
          frame(
              ValueType.BROKER_PARTITION_CONFIGURATION,
              BrokerPartitionConfigurationIntent.INSTALL_AUTHORITY,
              "demo",
              7,
              value),
          observer);
      assertEquals(Status.Code.INVALID_ARGUMENT, observer.error.getStatus().getCode());
    }
    assertEquals(0, executor.installs);
  }

  @Test
  void rejectsAdminValueInBusinessCommandBeforeEngineQueue() {
    final var executor = new Executor();
    final var business = new AtomicInteger();
    final var manager = manager(executor, business);
    final var request =
        new ExecuteCommandRequest()
            .setPartitionId(7)
            .setValueType(ValueType.BROKER_PARTITION_CONFIGURATION)
            .setIntent(BrokerPartitionConfigurationIntent.QUERY);
    request.setClusterId("demo");
    final var bytes = new byte[request.getLength()];
    request.write(new UnsafeBuffer(bytes), 0);
    final var observer = new Observer();
    manager.sendRequest(bytes, observer);
    assertEquals(Status.Code.INVALID_ARGUMENT, observer.error.getStatus().getCode());
    assertEquals(0, business.get());
  }

  private static BrokerPartitionConfigurationRecord configuration() {
    return new BrokerPartitionConfigurationRecord()
        .setClusterId("demo")
        .setPartitionId(7)
        .setExpectedGeneration(4)
        .setGeneration(5)
        .setOwnerChangeId("owner")
        .setExpectedRecoveryEpoch(3);
  }

  private static BrokerEndpointManager manager(final Executor executor, final AtomicInteger calls) {
    return new BrokerEndpointManager(
        "demo",
        0,
        new CommandRequestHandler(
            bytes -> {
              calls.incrementAndGet();
              return CompletableActorFuture.completed(bytes);
            },
            Engine.eventRegistry(),
            0),
        executor);
  }

  private static byte[] frame(
      final ValueType type,
      final Intent intent,
      final String cluster,
      final int partition,
      final UnifiedRecordValue value) {
    final var payload = new UnsafeBuffer(new byte[value.getLength()]);
    value.write(payload, 0);
    final var request =
        new ExecuteAdminRequest()
            .setClusterId(cluster)
            .setPartitionId(partition)
            .setValueType(type)
            .setIntent(intent)
            .setValue(payload, 0, payload.capacity());
    final var buffer = new UnsafeBuffer(new byte[request.getLength()]);
    request.write(buffer, 0);
    return buffer.byteArray();
  }

  private static final class Observer implements StreamObserver<byte[]> {
    private byte[] response;
    private StatusRuntimeException error;
    private int completed;

    @Override
    public void onNext(final byte[] response) {
      this.response = response;
    }

    @Override
    public void onError(final Throwable error) {
      this.error = (StatusRuntimeException) error;
    }

    @Override
    public void onCompleted() {
      completed++;
    }
  }

  private static final class Executor implements BrokerPartitionChangeExecutor {
    private final BrokerPartitionConfiguration configuration =
        new BrokerPartitionConfiguration(
            "demo", 7, "broker-1", 2, 9, "digest", List.of(), 3, 4, "owner", 11);
    private int reads, installs, operations;
    private BrokerPartitionOperation last;
    private boolean fail, synchronous;

    @Override
    public ActorFuture<BrokerPartitionConfiguration> readConfiguration(
        final String cluster, final int partition) {
      assertEquals("demo", cluster);
      assertEquals(7, partition);
      reads++;
      if (fail && synchronous) {
        throw new IllegalStateException("NO_QUORUM");
      }
      return fail
          ? CompletableActorFuture.completedExceptionally(new IllegalStateException("NO_QUORUM"))
          : CompletableActorFuture.completed(configuration);
    }

    @Override
    public ActorFuture<BrokerPartitionConfiguration> installAuthority(
        final String cluster,
        final int partition,
        final long expected,
        final long generation,
        final String owner,
        final long recovery) {
      assertEquals("demo", cluster);
      assertEquals(7, partition);
      assertEquals(4, expected);
      assertEquals(5, generation);
      assertEquals("owner", owner);
      assertEquals(3, recovery);
      installs++;
      return CompletableActorFuture.completed(configuration);
    }

    @Override
    public ActorFuture<Void> apply(final BrokerPartitionOperation request) {
      operations++;
      last = request;
      return CompletableActorFuture.completed(null);
    }

    @Override
    public ActorFuture<Void> add(final BrokerPartitionOperation request) {
      operations++;
      last = request;
      return CompletableActorFuture.completed(null);
    }

    @Override
    public ActorFuture<Void> join(final BrokerPartitionOperation request) {
      operations++;
      last = request;
      return CompletableActorFuture.completed(null);
    }

    @Override
    public ActorFuture<Void> promoteLearner(final BrokerPartitionOperation request) {
      operations++;
      last = request;
      return CompletableActorFuture.completed(null);
    }

    @Override
    public ActorFuture<Void> leave(final BrokerPartitionOperation request) {
      operations++;
      last = request;
      return CompletableActorFuture.completed(null);
    }
  }
}
