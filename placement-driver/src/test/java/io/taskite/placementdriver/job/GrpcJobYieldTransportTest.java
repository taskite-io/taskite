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
package io.taskite.placementdriver.job;

import static org.junit.jupiter.api.Assertions.*;

import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.taskite.broker.client.response.BrokerErrorException;
import io.taskite.broker.client.response.BrokerRejectionException;
import io.taskite.broker.client.transport.BrokerRequestService;
import io.taskite.placementdriver.broker.request.BrokerSyncWorkerSessionsRequest;
import io.taskite.placementdriver.broker.request.BrokerYieldJobsRequest;
import io.taskite.protocol.impl.encoding.ErrorResponse;
import io.taskite.protocol.impl.encoding.ExecuteCommandRequest;
import io.taskite.protocol.impl.encoding.ExecuteCommandResponse;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.record.*;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.protocol.record.value.broker.JobYieldStatus;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.util.BufferUtil;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class GrpcJobYieldTransportTest {
  @Test
  void bothControlsUseCommonRpcAndOnlySendSessionIdentity() throws Exception {
    final var captured = new AtomicReference<JobYieldRecord>();
    final var service =
        new BrokerRequestService() {
          @Override
          protected void sendRequest(final byte[] bytes, final StreamObserver<byte[]> observer) {
            final var command = new ExecuteCommandRequest();
            command.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
            assertEquals("demo", command.getClusterId());
            assertEquals(1, command.getPartitionId());
            assertEquals(ValueType.JOB_YIELD, command.getValueType());
            final var value = new JobYieldRecord();
            value.wrap(command.getValue());
            captured.set(value);
            observer.onNext(
                reply(
                    value,
                    command.getIntent() == JobYieldIntent.FENCE_SESSION
                        ? JobYieldIntent.SESSION_FENCED
                        : JobYieldIntent.SESSIONS_SYNCED));
            observer.onCompleted();
          }
        };
    final var server = NettyServerBuilder.forPort(0).addService(service).build().start();
    try (final var transport = new GrpcJobYieldTransport(Duration.ofSeconds(3))) {
      final var address = address(server);
      final var request =
          identity()
              .setAttempts(8)
              .setReleasedJobCount(100)
              .setStatus(JobYieldStatus.COMPLETED)
              .setLastError("PD_ONLY");
      final var response = transport.yieldJobs(address, request).get(5, TimeUnit.SECONDS);
      assertTrue(identity().sameIdentity(response));
      assertEquals(0, captured.get().getAttempts());
      assertEquals(0, captured.get().getReleasedJobCount());
      assertEquals("", captured.get().getLastError());
      assertEquals(JobYieldStatus.PENDING, captured.get().getStatus());
      final var page =
          new JobYieldRecord()
              .setClusterId("demo")
              .setPartitionId(1)
              .setPreviousSyncCursor(7)
              .setSyncCursor(identity().getYieldId())
              .setSyncComplete(true)
              .setSessions(List.of(request));
      final var synced = transport.syncWorkerSessions(address, page).get(5, TimeUnit.SECONDS);
      assertEquals(page.getSyncCursor(), synced.getSyncCursor());
      assertEquals(7, captured.get().getPreviousSyncCursor());
      assertTrue(synced.isSyncComplete());
      assertTrue(identity().sameIdentity(captured.get().getSessions().getFirst()));
      assertEquals(0, captured.get().getSessions().getFirst().getReleasedJobCount());
      assertFalse(new BrokerYieldJobsRequest(request).allowsTransportRetry());
      assertFalse(new BrokerSyncWorkerSessionsRequest(page).allowsTransportRetry());
    } finally {
      server.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
    }
  }

  @Test
  void brokerRejectionsKeepTheirTypes() throws Exception {
    final var requests = new AtomicInteger();
    final var server =
        NettyServerBuilder.forPort(0)
            .addService(
                new BrokerRequestService() {
                  @Override
                  protected void sendRequest(
                      final byte[] bytes, final StreamObserver<byte[]> observer) {
                    final var reason = BufferUtil.wrapString("FEATURE_DISABLED: secret detail");
                    final var rejection =
                        new ExecuteCommandResponse()
                            .setPartitionId(1)
                            .setRecordType(RecordType.COMMAND_REJECTION)
                            .setValueType(ValueType.JOB_YIELD)
                            .setIntent(JobYieldIntent.FENCE_SESSION)
                            .setRejectionType(RejectionType.FEATURE_DISABLED)
                            .setRejectionReason(reason, 0, reason.capacity());
                    final var buffer = new UnsafeBuffer(new byte[rejection.getLength()]);
                    rejection.write(buffer, 0);
                    requests.incrementAndGet();
                    observer.onNext(buffer.byteArray());
                    observer.onCompleted();
                  }
                })
            .build()
            .start();
    try (final var transport = new GrpcJobYieldTransport(Duration.ofSeconds(2))) {
      final var error =
          assertThrows(
              ExecutionException.class,
              () -> transport.yieldJobs(address(server), identity()).get(3, TimeUnit.SECONDS));
      assertEquals(
          RejectionType.FEATURE_DISABLED,
          assertInstanceOf(BrokerRejectionException.class, error.getCause()).getRejection().type());
      assertEquals(1, requests.get());
    } finally {
      server.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
    }
  }

  @Test
  void commonErrorResponsesAndUnknownGrpcFailuresAreNotRetried() throws Exception {
    final var requests = new AtomicInteger();
    final var server =
        NettyServerBuilder.forPort(0)
            .addService(
                new BrokerRequestService() {
                  @Override
                  protected void sendRequest(
                      final byte[] bytes, final StreamObserver<byte[]> observer) {
                    if (requests.incrementAndGet() == 1) {
                      final var error =
                          new ErrorResponse().setErrorCode(ErrorCode.RESOURCE_EXHAUSTED);
                      final var buffer = new UnsafeBuffer(new byte[error.getLength()]);
                      error.write(buffer, 0);
                      observer.onNext(buffer.byteArray());
                      observer.onCompleted();
                    } else {
                      observer.onError(Status.UNAVAILABLE.asRuntimeException());
                    }
                  }
                })
            .build()
            .start();
    try (final var transport = new GrpcJobYieldTransport(Duration.ofSeconds(2))) {
      final var error =
          assertThrows(
              ExecutionException.class,
              () -> transport.yieldJobs(address(server), identity()).get(3, TimeUnit.SECONDS));
      assertEquals(
          ErrorCode.RESOURCE_EXHAUSTED,
          assertInstanceOf(BrokerErrorException.class, error.getCause()).getError().code());
      final var unavailable =
          assertThrows(
              ExecutionException.class,
              () ->
                  transport
                      .syncWorkerSessions(address(server), identity())
                      .get(3, TimeUnit.SECONDS));
      assertEquals(Status.Code.UNAVAILABLE, Status.fromThrowable(unavailable).getCode());
      assertEquals(2, requests.get());
    } finally {
      server.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
    }
  }

  @Test
  void requestDecodersRejectWrongIdentitiesStatusesAndEventIntents() {
    final var request = new BrokerYieldJobsRequest(identity());
    for (final var wrong :
        List.of(
            identity().setWorkerLeaseToken(1),
            identity().setClusterId("other"),
            identity().setPartitionId(2),
            identity().setReleasedJobCount(-1),
            identity().setStatus(JobYieldStatus.RETIRED))) {
      assertThrows(
          IllegalArgumentException.class,
          () -> request.getResponse(new UnsafeBuffer(reply(wrong, JobYieldIntent.SESSION_FENCED))));
    }
    assertThrows(
        IllegalArgumentException.class,
        () ->
            request.getResponse(
                new UnsafeBuffer(reply(identity(), JobYieldIntent.SESSIONS_SYNCED))));
    assertTrue(
        request
                .getResponse(
                    new UnsafeBuffer(
                        reply(
                            identity().setStatus(JobYieldStatus.COMPLETED).setReleasedJobCount(4),
                            JobYieldIntent.SESSION_FENCED)))
                .getResponse()
                .getStatus()
            == JobYieldStatus.COMPLETED);
    final var sync = new BrokerSyncWorkerSessionsRequest(identity());
    assertThrows(
        IllegalArgumentException.class,
        () -> sync.getResponse(new UnsafeBuffer(reply(identity(), JobYieldIntent.SESSION_FENCED))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            sync.getResponse(
                new UnsafeBuffer(
                    reply(identity().setSyncCursor(-1), JobYieldIntent.SESSIONS_SYNCED))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            sync.getResponse(
                new UnsafeBuffer(
                    reply(identity().setClusterId("other"), JobYieldIntent.SESSIONS_SYNCED))));
    // Broker can return its own committed cursor when a PD retry carries an outdated cursor.
    assertEquals(
        7,
        sync.getResponse(
                new UnsafeBuffer(
                    reply(identity().setSyncCursor(7), JobYieldIntent.SESSIONS_SYNCED)))
            .getResponse()
            .getSyncCursor());
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

  private static BrokerEndpoint address(final Server server) {
    return new BrokerEndpoint("broker", "127.0.0.1", server.getPort(), 26502);
  }

  private static byte[] reply(final JobYieldRecord value, final JobYieldIntent intent) {
    final var payload = new UnsafeBuffer(new byte[value.getLength()]);
    value.write(payload, 0);
    final var response =
        new ExecuteCommandResponse()
            .setPartitionId(ExecuteCommandResponseEncoder.partitionIdNullValue())
            .setRecordType(RecordType.EVENT)
            .setValueType(ValueType.JOB_YIELD)
            .setIntent(intent)
            .setValue(payload, 0, payload.capacity());
    final var bytes = new UnsafeBuffer(new byte[response.getLength()]);
    response.write(bytes, 0);
    return bytes.byteArray();
  }
}
