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
package io.taskite.broker.client.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Status;
import io.taskite.broker.client.request.BrokerAdminRequest;
import io.taskite.broker.client.request.BrokerRequest;
import io.taskite.broker.client.response.BrokerError;
import io.taskite.broker.client.response.BrokerErrorException;
import io.taskite.broker.client.response.BrokerErrorResponse;
import io.taskite.broker.client.response.BrokerRejection;
import io.taskite.broker.client.response.BrokerRejectionException;
import io.taskite.broker.client.response.BrokerRejectionResponse;
import io.taskite.broker.client.response.BrokerRequestRejectedBeforeAppendException;
import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.broker.client.transport.BrokerClientTransport;
import io.taskite.protocol.impl.record.value.broker.BrokerPartitionConfigurationRecord;
import io.taskite.protocol.record.ErrorCode;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.BrokerPartitionConfigurationIntent;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.util.BufferWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

final class BrokerRequestManagerTest {
  private static final BrokerEndpoint ENDPOINT =
      new BrokerEndpoint("broker-1", "localhost", 26501, 26601);
  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(1);

  @Test
  void shouldSendAdminRequestWithoutRetryForEveryOverload() throws Exception {
    final var response = new BrokerResponse<>("accepted");
    final var transport = new StubTransport(response);
    final var manager = new BrokerRequestManager(transport, REQUEST_TIMEOUT, 3);
    final var request = new AdminRequest();
    assertSame(response, manager.sendRequest(ENDPOINT, request).get());
    assertFalse(transport.retry);
    assertSame(response, manager.sendRequest(() -> ENDPOINT, request).get());
    assertFalse(transport.retry);
    assertSame(response, manager.sendRequestWithRetry(ENDPOINT, request).get());
    assertFalse(transport.retry);
  }

  static final class AdminRequest extends BrokerAdminRequest<BrokerPartitionConfigurationRecord> {
    AdminRequest() {
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

  @Test
  void shouldSerializeRequestBeforeSending() throws Exception {
    final var response = new BrokerResponse<>("accepted", 1, 42);
    final var transport = new StubTransport(response);
    final var request = new StubRequest(new StubWriter());
    final var manager = new BrokerRequestManager(transport, REQUEST_TIMEOUT, 3);

    final var result = manager.sendRequest(ENDPOINT, request).get();

    assertSame(response, result);
    assertTrue(request.serialized);
    assertTrue(transport.retry);
    assertEquals(3, transport.maxRetries);
    assertEquals(REQUEST_TIMEOUT, transport.requestTimeout);
  }

  @Test
  void shouldMapRejectionResponseToException() {
    final var rejection =
        new BrokerRejection(JobIntent.COMPLETE, 12L, RejectionType.INVALID_STATE, "not active");
    final var transport = new StubTransport(new BrokerRejectionResponse<>(rejection));
    final var manager = new BrokerRequestManager(transport, REQUEST_TIMEOUT, 0);

    final var error =
        assertThrows(
            ExecutionException.class, () -> manager.sendRequest(ENDPOINT, new StubRequest()).get());

    final var cause = assertInstanceOf(BrokerRejectionException.class, error.getCause());
    assertSame(rejection, cause.getRejection());
  }

  @Test
  void shouldMapErrorResponseToException() {
    final var brokerError = new BrokerError(ErrorCode.INTERNAL_ERROR, "boom");
    final var transport = new StubTransport(new BrokerErrorResponse<>(brokerError));
    final var manager = new BrokerRequestManager(transport, REQUEST_TIMEOUT, 0);

    final var error =
        assertThrows(
            ExecutionException.class, () -> manager.sendRequest(ENDPOINT, new StubRequest()).get());

    final var cause = assertInstanceOf(BrokerErrorException.class, error.getCause());
    assertSame(brokerError, cause.getError());
  }

  @Test
  void shouldSupportSendingWithoutRetry() throws Exception {
    final var response = new BrokerResponse<>("accepted");
    final var transport = new StubTransport(response);
    final var manager = new BrokerRequestManager(transport, REQUEST_TIMEOUT, 3);

    final var result = manager.sendRequest(ENDPOINT, new StubRequest(), REQUEST_TIMEOUT).get();

    assertSame(response, result);
    assertFalse(transport.retry);
  }

  @Test
  void shouldResolveEndpointAgainForEachRetry() throws Exception {
    final BrokerEndpoint newEndpoint = new BrokerEndpoint("broker-2", "localhost", 26502, 26602);
    final var transport = new RefreshingEndpointTransport();
    final var manager = new BrokerRequestManager(transport, REQUEST_TIMEOUT, 1);
    final AtomicInteger resolutions = new AtomicInteger();

    final var result =
        manager
            .sendRequest(
                () -> resolutions.getAndIncrement() == 0 ? ENDPOINT : newEndpoint,
                new StubRequest())
            .get();

    assertEquals("accepted", result.getResponse());
    assertEquals(List.of(ENDPOINT, newEndpoint), transport.endpoints);
  }

  @Test
  void shouldNotRetryCancelledRequests() {
    final var transport = new RefreshingEndpointTransport();
    final var manager = new BrokerRequestManager(transport, REQUEST_TIMEOUT, 3);
    final AtomicInteger resolutions = new AtomicInteger();

    final CancellationException error =
        assertThrows(
            CancellationException.class,
            () ->
                manager
                    .sendRequest(
                        () -> {
                          resolutions.incrementAndGet();
                          throw new CancellationException("cancelled");
                        },
                        new StubRequest())
                    .get());

    assertEquals("cancelled", error.getMessage());
    assertEquals(1, resolutions.get());
    assertEquals(List.of(), transport.endpoints);
  }

  @Test
  void shouldNotRetryTimedOutCommands() {
    final var transport =
        new RefreshingEndpointTransport(new TimeoutException("request timed out"));
    final var manager = new BrokerRequestManager(transport, REQUEST_TIMEOUT, 3);
    final AtomicInteger resolutions = new AtomicInteger();

    final var error =
        assertThrows(
            ExecutionException.class,
            () ->
                manager
                    .sendRequest(
                        () -> {
                          resolutions.incrementAndGet();
                          return ENDPOINT;
                        },
                        new StubRequest())
                    .get());

    assertInstanceOf(TimeoutException.class, error.getCause());
    assertEquals(1, resolutions.get());
    assertEquals(List.of(ENDPOINT), transport.endpoints);
  }

  @Test
  void shouldNotRetryUnavailableWithUnknownAppendOutcome() {
    final var failure = Status.UNAVAILABLE.asRuntimeException();
    final var transport = new RefreshingEndpointTransport(failure);
    final var manager = new BrokerRequestManager(transport, REQUEST_TIMEOUT, 3);
    final var error =
        assertThrows(
            ExecutionException.class,
            () -> manager.sendRequest(() -> ENDPOINT, new StubRequest()).get());
    assertSame(failure, error.getCause());
    assertEquals(List.of(ENDPOINT), transport.endpoints);
  }

  private static final class StubTransport implements BrokerClientTransport {
    private final BrokerResponse<?> response;
    private boolean retry;
    private int maxRetries;
    private Duration requestTimeout;

    private StubTransport(final BrokerResponse<?> response) {
      this.response = response;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ActorFuture<BrokerResponse<T>> sendRequest(
        final BrokerEndpoint endpoint,
        final BrokerRequest<T> request,
        final Duration requestTimeout) {
      retry = false;
      maxRetries = -1;
      this.requestTimeout = requestTimeout;
      return CompletableActorFuture.completed((BrokerResponse<T>) response);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ActorFuture<BrokerResponse<T>> sendRequestWithRetry(
        final BrokerEndpoint endpoint,
        final BrokerRequest<T> request,
        final Duration requestTimeout,
        final int maxRetries,
        final Predicate<DirectBuffer> retryable) {
      retry = true;
      this.maxRetries = maxRetries;
      this.requestTimeout = requestTimeout;
      return CompletableActorFuture.completed((BrokerResponse<T>) response);
    }
  }

  private static final class RefreshingEndpointTransport implements BrokerClientTransport {
    private final List<BrokerEndpoint> endpoints = new ArrayList<>();
    private final Throwable firstError;

    private RefreshingEndpointTransport() {
      this(new BrokerRequestRejectedBeforeAppendException());
    }

    private RefreshingEndpointTransport(final Throwable firstError) {
      this.firstError = firstError;
    }

    @Override
    public <T> ActorFuture<BrokerResponse<T>> sendRequest(
        final BrokerEndpoint endpoint,
        final BrokerRequest<T> request,
        final Duration requestTimeout) {
      return CompletableActorFuture.completedExceptionally(new UnsupportedOperationException());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ActorFuture<BrokerResponse<T>> sendRequestWithRetry(
        final BrokerEndpoint endpoint,
        final BrokerRequest<T> request,
        final Duration requestTimeout,
        final int maxRetries,
        final Predicate<DirectBuffer> retryable) {
      endpoints.add(endpoint);
      if (endpoints.size() == 1) {
        return CompletableActorFuture.completedExceptionally(firstError);
      }
      return CompletableActorFuture.completed((BrokerResponse<T>) new BrokerResponse<>("accepted"));
    }
  }

  private static final class StubRequest implements BrokerRequest<String> {
    private final BufferWriter writer;
    private boolean serialized;

    private StubRequest() {
      this(null);
    }

    private StubRequest(final BufferWriter writer) {
      this.writer = writer;
    }

    @Override
    public void setPartitionId(final int partitionId) {}

    @Override
    public void setClusterId(final String clusterId) {}

    @Override
    public String getClusterId() {
      return "cluster";
    }

    @Override
    public int getPartitionId() {
      return 1;
    }

    @Override
    public byte[] serializeRequest() {
      return new byte[0];
    }

    @Override
    public void serializeValue() {
      if (writer != null) {
        serialized = true;
      }
    }

    @Override
    public BrokerResponse<String> getResponse(final DirectBuffer responseBuffer) {
      return new BrokerResponse<>("unused");
    }

    private String toResponseDto(final DirectBuffer buffer) {
      return "unused";
    }

    @Override
    public BufferWriter getRequestWriter() {
      return writer;
    }

    @Override
    public BrokerResponse<String> mapResponse(
        final int partitionId, final long key, final UnifiedRecordValue value) {
      return new BrokerResponse<>("unused", partitionId, key);
    }
  }

  private static final class StubWriter implements BufferWriter {
    @Override
    public int getLength() {
      return 1;
    }

    @Override
    public void write(final MutableDirectBuffer buffer, final int offset) {
      buffer.putByte(offset, (byte) 1);
    }
  }
}
