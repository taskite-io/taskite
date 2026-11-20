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

import static org.junit.jupiter.api.Assertions.*;

import io.grpc.*;
import io.taskite.broker.api.BrokerEndpointManager;
import io.taskite.broker.client.impl.BrokerClientImpl;
import io.taskite.broker.client.request.BrokerExecuteCommand;
import io.taskite.broker.client.transport.GrpcBrokerClientTransport;
import io.taskite.broker.partition.*;
import io.taskite.engine.Engine;
import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.protocol.record.value.broker.JobYieldStatus;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.streamplatform.command.CommandRequestHandler;
import io.taskite.topology.BrokerEndpoint;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

final class BrokerJobYieldGrpcTest {
  @Test
  void commonCommandsNeedNoExtraCredentialsAndResponsesPreserveFullSessionIdentity()
      throws Exception {
    var captured = new AtomicReference<JobYieldRecord>();
    var handler =
        new CommandRequestHandler(
            command -> {
              captured.set(((JobYieldRecord) command.value()).copy());
              command.metadata().recordType(RecordType.EVENT).intent(JobYieldIntent.SESSION_FENCED);
              return CompletableActorFuture.completed(command);
            },
            Engine.eventRegistry(),
            0);
    var endpoint = new BrokerEndpointManager("demo", 0, handler, unsupportedExecutor());
    try (var server = new GrpcBrokerServer(0, endpoint);
        var client = client()) {
      server.start();
      var endpointAddress = new BrokerEndpoint("broker", "127.0.0.1", server.port(), 26502);
      var request = new WorkerCommand(identity(), JobYieldIntent.FENCE_SESSION);
      var response =
          client.sendRequest(endpointAddress, request).get(5, TimeUnit.SECONDS).getResponse();
      assertTrue(identity().sameIdentity(response));
      assertEquals(9007199254740995L, response.getWorkerLeaseToken());
      assertEquals("worker", captured.get().getWorkerId());
      assertEquals(JobYieldStatus.PENDING, response.getStatus());
      captured.set(null);
      assertStatus(
          Status.Code.FAILED_PRECONDITION,
          () ->
              client
                  .sendRequest(
                      endpointAddress,
                      new WorkerCommand(
                          identity().setClusterId("other"), JobYieldIntent.FENCE_SESSION))
                  .get(5, TimeUnit.SECONDS));
      assertNull(captured.get());
    }
  }

  @Test
  void ordinaryJobCommandsUseTheSameRequestService() throws Exception {
    var captured = new AtomicReference<JobRecord>();
    var handler =
        new CommandRequestHandler(
            command -> {
              captured.set(((JobRecord) command.value()).copy());
              command.metadata().recordType(RecordType.EVENT).intent(JobIntent.COMPLETED);
              return CompletableActorFuture.completed(command);
            },
            Engine.eventRegistry(),
            0);
    try (var server =
            new GrpcBrokerServer(
                0, new BrokerEndpointManager("demo", 0, handler, unsupportedExecutor()));
        var client = client()) {
      server.start();
      var response =
          client
              .sendRequest(
                  new BrokerEndpoint("broker", "127.0.0.1", server.port(), 26502),
                  new JobCommand(JobIntent.COMPLETE))
              .get(5, TimeUnit.SECONDS);
      assertEquals("a", response.getResponse().getTenantId());
      assertEquals("test", captured.get().getType());
    }
  }

  @Test
  void rejectsEnvelopeMismatchInvalidSessionsAndInternalIntentsBeforeSubmission() throws Exception {
    var captured = new AtomicReference<JobYieldRecord>();
    var handler =
        new CommandRequestHandler(
            command -> {
              captured.set((JobYieldRecord) command.value());
              return CompletableActorFuture.completed(command);
            },
            Engine.eventRegistry(),
            0);
    try (var server =
            new GrpcBrokerServer(
                0, new BrokerEndpointManager("demo", 0, handler, unsupportedExecutor()));
        var client = client()) {
      server.start();
      var address = new BrokerEndpoint("broker", "127.0.0.1", server.port(), 26502);
      var mismatch = new WorkerCommand(identity(), JobYieldIntent.FENCE_SESSION);
      mismatch.setPartitionId(2);
      var clusterMismatch = new WorkerCommand(identity(), JobYieldIntent.FENCE_SESSION);
      clusterMismatch.setClusterId("other");
      var keyMismatch = new WorkerCommand(identity(), JobYieldIntent.FENCE_SESSION);
      keyMismatch.setKey(Protocol.encodePartitionId(2, 0));
      for (var command :
          List.of(
              mismatch,
              clusterMismatch,
              keyMismatch,
              new WorkerCommand(identity().setPartitionId(0), JobYieldIntent.FENCE_SESSION),
              new WorkerCommand(identity().setWorkerLeaseToken(0), JobYieldIntent.FENCE_SESSION),
              new WorkerCommand(identity().setWorkerId(""), JobYieldIntent.FENCE_SESSION),
              new WorkerCommand(identity().setTenantId(""), JobYieldIntent.FENCE_SESSION),
              new WorkerCommand(identity().setYieldId(0), JobYieldIntent.FENCE_SESSION))) {
        assertStatus(
            Status.Code.INVALID_ARGUMENT,
            () -> client.sendRequest(address, command).get(5, TimeUnit.SECONDS));
      }
      for (var intent : JobYieldIntent.values()) {
        if (intent == JobYieldIntent.FENCE_SESSION || intent == JobYieldIntent.SYNC_SESSIONS) {
          continue;
        }
        assertStatus(
            Status.Code.INVALID_ARGUMENT,
            () ->
                client
                    .sendRequest(address, new WorkerCommand(identity(), intent))
                    .get(5, TimeUnit.SECONDS));
      }
      assertStatus(
          Status.Code.INVALID_ARGUMENT,
          () ->
              client
                  .sendRequest(address, new JobCommand(JobIntent.YIELD))
                  .get(5, TimeUnit.SECONDS));
      assertNull(captured.get());
    }
  }

  @Test
  void sessionPagesUseTheCommonCommandAndValidatePageBounds() throws Exception {
    var captured = new AtomicReference<JobYieldRecord>();
    var handler =
        new CommandRequestHandler(
            command -> {
              captured.set(((JobYieldRecord) command.value()).copy());
              command
                  .metadata()
                  .recordType(RecordType.EVENT)
                  .intent(JobYieldIntent.SESSIONS_SYNCED);
              return CompletableActorFuture.completed(command);
            },
            Engine.eventRegistry(),
            0);
    try (var server =
            new GrpcBrokerServer(
                0, new BrokerEndpointManager("demo", 0, handler, unsupportedExecutor()));
        var client = client()) {
      server.start();
      var address = new BrokerEndpoint("broker", "127.0.0.1", server.port(), 26502);
      var page =
          new JobYieldRecord()
              .setClusterId("demo")
              .setPartitionId(1)
              .setSyncCursor(identity().getYieldId())
              .setSyncComplete(true)
              .setSessions(List.of(identity()));
      var response =
          client
              .sendRequest(address, new WorkerCommand(page, JobYieldIntent.SYNC_SESSIONS))
              .get(5, TimeUnit.SECONDS)
              .getResponse();
      assertEquals(page.getSyncCursor(), response.getSyncCursor());
      assertTrue(response.isSyncComplete());
      assertTrue(identity().sameIdentity(captured.get().getSessions().getFirst()));
      captured.set(null);
      for (var invalid :
          List.of(
              page.copy().setPreviousSyncCursor(-1),
              page.copy().setPreviousSyncCursor(page.getSyncCursor() + 1),
              page.copy().setSessions(Collections.nCopies(101, identity())),
              page.copy().setSessions(List.of(identity().setReason(null))),
              page.copy().setSessions(List.of(identity().setClusterId("other"))))) {
        assertStatus(
            Status.Code.INVALID_ARGUMENT,
            () ->
                client
                    .sendRequest(address, new WorkerCommand(invalid, JobYieldIntent.SYNC_SESSIONS))
                    .get(5, TimeUnit.SECONDS));
      }
      assertNull(captured.get());
    }
  }

  private static JobYieldRecord identity() {
    return new JobYieldRecord()
        .setClusterId("demo")
        .setPartitionId(1)
        .setYieldId(9007199254740993L)
        .setTenantId("a")
        .setWorkerId("worker")
        .setWorkerLeaseToken(9007199254740995L);
  }

  private static BrokerClientImpl client() {
    return new BrokerClientImpl(new GrpcBrokerClientTransport(), Duration.ofSeconds(3), 0);
  }

  private static void assertStatus(Status.Code expected, Executable invocation) {
    var error = assertThrows(ExecutionException.class, invocation);
    assertEquals(expected, Status.fromThrowable(error).getCode());
  }

  private static final class WorkerCommand extends BrokerExecuteCommand<JobYieldRecord> {
    private final JobYieldRecord value;

    WorkerCommand(JobYieldRecord value, JobYieldIntent intent) {
      super(ValueType.JOB_YIELD, intent);
      this.value = value;
      setClusterId(value.getClusterId());
      setPartitionId(value.getPartitionId());
    }

    @Override
    public JobYieldRecord getRequestWriter() {
      return value;
    }

    @Override
    protected JobYieldRecord toResponseDto(DirectBuffer buffer) {
      var result = new JobYieldRecord();
      result.wrap(buffer);
      return result;
    }
  }

  private static final class JobCommand extends BrokerExecuteCommand<JobRecord> {
    JobCommand(JobIntent intent) {
      super(ValueType.JOB, intent);
      setClusterId("demo");
      setPartitionId(1);
    }

    @Override
    public JobRecord getRequestWriter() {
      return new JobRecord().setTenantId("a").setJobType("test");
    }

    @Override
    protected JobRecord toResponseDto(DirectBuffer buffer) {
      var result = new JobRecord();
      result.wrap(buffer);
      return result;
    }
  }

  private static BrokerPartitionChangeExecutor unsupportedExecutor() {
    return new BrokerPartitionChangeExecutor() {
      public ActorFuture<Void> add(BrokerPartitionOperation request) {
        throw new UnsupportedOperationException();
      }

      public ActorFuture<Void> join(BrokerPartitionOperation request) {
        throw new UnsupportedOperationException();
      }

      public ActorFuture<Void> promoteLearner(BrokerPartitionOperation request) {
        throw new UnsupportedOperationException();
      }

      public ActorFuture<Void> leave(BrokerPartitionOperation request) {
        throw new UnsupportedOperationException();
      }
    };
  }
}
