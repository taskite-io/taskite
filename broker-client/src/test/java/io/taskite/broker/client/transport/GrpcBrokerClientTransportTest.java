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
package io.taskite.broker.client.transport;

import static org.junit.jupiter.api.Assertions.*;

import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.taskite.broker.client.request.BrokerAdminRequest;
import io.taskite.broker.client.request.BrokerExecuteCommand;
import io.taskite.broker.client.response.BrokerRequestRejectedBeforeAppendException;
import io.taskite.protocol.impl.encoding.*;
import io.taskite.protocol.impl.record.value.broker.BrokerPartitionConfigurationRecord;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.ErrorCode;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.BrokerPartitionConfigurationIntent;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.topology.BrokerEndpoint;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class GrpcBrokerClientTransportTest {
  @Test
  void directCommandSendClassifiesPreAppendRejectionWithoutRetry() throws Exception {
    final var requests = new AtomicInteger();
    final var server =
        NettyServerBuilder.forPort(0)
            .addService(
                new BrokerRequestService() {
                  @Override
                  protected void sendRequest(byte[] bytes, StreamObserver<byte[]> observer) {
                    requests.incrementAndGet();
                    final var response =
                        new ErrorResponse().setErrorCode(ErrorCode.PARTITION_LEADER_MISMATCH);
                    final var buffer = new UnsafeBuffer(new byte[response.getLength()]);
                    response.write(buffer, 0);
                    observer.onNext(buffer.byteArray());
                    observer.onCompleted();
                  }
                })
            .build()
            .start();
    try (final var transport = new GrpcBrokerClientTransport()) {
      final var request = new CompleteRequest();
      request.transportRetry = false;
      request.serializeValue();
      assertFalse(request.allowsTransportRetry());
      final var error =
          assertThrows(
              ExecutionException.class,
              () ->
                  transport
                      .sendRequest(
                          new BrokerEndpoint("b1", "127.0.0.1", server.getPort(), 26502),
                          request,
                          Duration.ofSeconds(2))
                      .get(3, TimeUnit.SECONDS));
      assertInstanceOf(BrokerRequestRejectedBeforeAppendException.class, error.getCause());
      assertEquals(1, requests.get());
    } finally {
      server.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
    }
  }

  @Test
  void sendsAdminThroughTheCommonRpcAndDecodesTheResponse() throws Exception {
    final var requests = new AtomicInteger();
    final Server server = server(requests, false);
    try (final var transport = new GrpcBrokerClientTransport()) {
      final var response =
          transport
              .sendRequest(
                  new BrokerEndpoint("b1", "127.0.0.1", server.getPort(), 26502),
                  new ReadRequest(),
                  Duration.ofSeconds(2))
              .get(3, TimeUnit.SECONDS);
      assertEquals("configuration", response.getResponse().getLeaderId());
      assertEquals(7, response.getPartitionId());
      assertEquals(1, requests.get());
    } finally {
      server.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
    }
  }

  @Test
  void neverRetriesAnAdminOperationEvenWhenTheRetryEntryPointIsCalled() throws Exception {
    final var requests = new AtomicInteger();
    final Server server = server(requests, true);
    try (final var transport = new GrpcBrokerClientTransport()) {
      final var error =
          assertThrows(
              ExecutionException.class,
              () ->
                  transport
                      .sendRequestWithRetry(
                          new BrokerEndpoint("b1", "127.0.0.1", server.getPort(), 26502),
                          new ReadRequest(),
                          Duration.ofSeconds(2),
                          3,
                          ignored -> true)
                      .get(3, TimeUnit.SECONDS));
      assertEquals(
          Status.Code.FAILED_PRECONDITION, Status.fromThrowable(error.getCause()).getCode());
      assertEquals(1, requests.get());
    } finally {
      server.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
    }
  }

  @Test
  void commandTransportDoesNotRetryUnknownUnavailableTimeoutOrCancellation() throws Exception {
    for (final var status :
        new Status[] {Status.UNAVAILABLE, Status.DEADLINE_EXCEEDED, Status.CANCELLED}) {
      final var requests = new AtomicInteger();
      final var server =
          NettyServerBuilder.forPort(0)
              .addService(
                  new BrokerRequestService() {
                    @Override
                    protected void sendRequest(byte[] bytes, StreamObserver<byte[]> observer) {
                      requests.incrementAndGet();
                      observer.onError(status.asRuntimeException());
                    }
                  })
              .build()
              .start();
      try (final var transport = new GrpcBrokerClientTransport()) {
        final var request = new CompleteRequest();
        request.serializeValue();
        final var error =
            assertThrows(
                ExecutionException.class,
                () ->
                    transport
                        .sendRequestWithRetry(
                            new BrokerEndpoint("b1", "127.0.0.1", server.getPort(), 26502),
                            request,
                            Duration.ofSeconds(2),
                            3,
                            ignored -> true)
                        .get(3, TimeUnit.SECONDS));
        assertEquals(status.getCode(), Status.fromThrowable(error.getCause()).getCode());
        assertEquals(1, requests.get());
      } finally {
        server.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
      }
    }
  }

  private static final class CompleteRequest extends BrokerExecuteCommand<JobRecord> {
    private boolean transportRetry = true;

    private CompleteRequest() {
      super(ValueType.JOB, JobIntent.COMPLETE);
      setClusterId("demo");
      setPartitionId(7);
      setKey(1);
    }

    @Override
    public JobRecord getRequestWriter() {
      return new JobRecord();
    }

    @Override
    protected JobRecord toResponseDto(DirectBuffer buffer) {
      final var response = new JobRecord();
      response.wrap(buffer);
      return response;
    }

    @Override
    public boolean allowsTransportRetry() {
      return transportRetry;
    }
  }

  private static Server server(final AtomicInteger requests, final boolean fail) throws Exception {
    return NettyServerBuilder.forPort(0)
        .addService(
            new BrokerRequestService() {
              @Override
              protected void sendRequest(
                  final byte[] request, final StreamObserver<byte[]> observer) {
                requests.incrementAndGet();
                final var envelope = new ExecuteAdminRequest();
                envelope.wrap(new UnsafeBuffer(request), 0, request.length);
                assertEquals(BrokerPartitionConfigurationIntent.QUERY, envelope.getIntent());
                assertEquals("demo", envelope.getClusterId());
                assertEquals(7, envelope.getPartitionId());
                if (fail) {
                  observer.onError(
                      Status.FAILED_PRECONDITION.withDescription("NO_QUORUM").asRuntimeException());
                } else {
                  final var value =
                      new BrokerPartitionConfigurationRecord().setLeaderId("configuration");
                  final var payload = new UnsafeBuffer(new byte[value.getLength()]);
                  value.write(payload, 0);
                  final var response =
                      new ExecuteAdminResponse()
                          .setClusterId("demo")
                          .setPartitionId(7)
                          .setValueType(ValueType.BROKER_PARTITION_CONFIGURATION)
                          .setIntent(BrokerPartitionConfigurationIntent.QUERIED)
                          .setValue(payload, 0, payload.capacity());
                  final var bytes = new UnsafeBuffer(new byte[response.getLength()]);
                  response.write(bytes, 0);
                  observer.onNext(bytes.byteArray());
                  observer.onCompleted();
                }
              }
            })
        .build()
        .start();
  }

  static final class ReadRequest extends BrokerAdminRequest<BrokerPartitionConfigurationRecord> {
    ReadRequest() {
      super(ValueType.BROKER_PARTITION_CONFIGURATION, BrokerPartitionConfigurationIntent.QUERY);
      setClusterId("demo");
      setPartitionId(7);
    }

    @Override
    public BrokerPartitionConfigurationRecord getRequestWriter() {
      return new BrokerPartitionConfigurationRecord().setClusterId("demo").setPartitionId(7);
    }

    @Override
    protected BrokerPartitionConfigurationRecord toResponseDto(final DirectBuffer buffer) {
      final var value = new BrokerPartitionConfigurationRecord();
      value.wrap(buffer);
      return value;
    }
  }
}
