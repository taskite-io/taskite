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

import io.taskite.broker.client.ErrorResponseHandler;
import io.taskite.broker.client.request.BrokerRequest;
import io.taskite.broker.client.response.BrokerErrorException;
import io.taskite.broker.client.response.BrokerRejectionException;
import io.taskite.broker.client.response.BrokerRequestRejectedBeforeAppendException;
import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.broker.client.response.BrokerResponseException;
import io.taskite.broker.client.transport.BrokerClientTransport;
import io.taskite.protocol.record.ErrorCode;
import io.taskite.protocol.record.MessageHeaderDecoder;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.topology.BrokerEndpoint;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import org.agrona.DirectBuffer;

public final class BrokerRequestManager implements AutoCloseable {
  private static final TransportRequestSender SENDER_WITH_RETRY =
      new TransportRequestSender() {
        @Override
        public <T> ActorFuture<BrokerResponse<T>> send(
            final BrokerClientTransport transport,
            final BrokerEndpoint endpoint,
            final BrokerRequest<T> request,
            final Duration requestTimeout,
            final int maxRetries) {
          return transport.sendRequestWithRetry(
              endpoint, request, requestTimeout, maxRetries, BrokerRequestManager::isRetryable);
        }
      };

  private static final TransportRequestSender SENDER_WITHOUT_RETRY =
      new TransportRequestSender() {
        @Override
        public <T> ActorFuture<BrokerResponse<T>> send(
            final BrokerClientTransport transport,
            final BrokerEndpoint endpoint,
            final BrokerRequest<T> request,
            final Duration requestTimeout,
            final int maxRetries) {
          return transport.sendRequest(endpoint, request, requestTimeout);
        }
      };

  private final BrokerClientTransport transport;
  private final Duration requestTimeout;
  private final int maxRetries;

  public BrokerRequestManager(
      final BrokerClientTransport transport, final Duration requestTimeout, final int maxRetries) {
    this.transport = Objects.requireNonNull(transport, "transport");
    this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
    this.maxRetries = maxRetries;
  }

  /**
   * Decides whether a raw broker response warrants a retry. Only {@link
   * ErrorCode#PARTITION_LEADER_MISMATCH} is retryable because a new attempt may reach the current
   * leader. Successes, rejections, and all other errors are returned to the caller unchanged.
   */
  private static boolean isRetryable(final DirectBuffer responseContent) {
    final ErrorResponseHandler errorHandler = new ErrorResponseHandler();
    final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    headerDecoder.wrap(responseContent, 0);

    if (!errorHandler.handlesResponse(headerDecoder)) {
      return false;
    }
    errorHandler.wrap(
        responseContent,
        headerDecoder.encodedLength(),
        headerDecoder.blockLength(),
        headerDecoder.version());
    return errorHandler.getErrorCode() == ErrorCode.PARTITION_LEADER_MISMATCH;
  }

  public <T> ActorFuture<BrokerResponse<T>> sendRequest(
      final BrokerEndpoint endpoint, final BrokerRequest<T> request) {
    return sendRequestWithRetry(endpoint, request, requestTimeout);
  }

  public <T> ActorFuture<BrokerResponse<T>> sendRequest(
      final Supplier<BrokerEndpoint> endpointSupplier, final BrokerRequest<T> request) {
    final Supplier<BrokerEndpoint> normalizedSupplier =
        Objects.requireNonNull(endpointSupplier, "endpointSupplier");
    final BrokerRequest<T> normalizedRequest = Objects.requireNonNull(request, "request");
    if (!normalizedRequest.allowsTransportRetry()) {
      try {
        return sendRequestInternal(
            normalizedSupplier.get(), normalizedRequest, requestTimeout, SENDER_WITHOUT_RETRY);
      } catch (final Throwable error) {
        return failedFuture(unwrap(error));
      }
    }
    try {
      normalizedRequest.serializeValue();
    } catch (final Throwable error) {
      return failedFuture(new BrokerResponseException("Failed to serialize broker request", error));
    }
    return normalizeResponse(
        sendWithEndpointRefresh(normalizedSupplier, normalizedRequest, maxRetries));
  }

  public <T> ActorFuture<BrokerResponse<T>> sendRequest(
      final BrokerEndpoint endpoint, final BrokerRequest<T> request, final Duration timeout) {
    return sendRequestInternal(endpoint, request, timeout, SENDER_WITHOUT_RETRY);
  }

  public <T> ActorFuture<BrokerResponse<T>> sendRequestWithRetry(
      final BrokerEndpoint endpoint, final BrokerRequest<T> request) {
    return sendRequestWithRetry(endpoint, request, requestTimeout);
  }

  public <T> ActorFuture<BrokerResponse<T>> sendRequestWithRetry(
      final BrokerEndpoint endpoint, final BrokerRequest<T> request, final Duration timeout) {
    return sendRequestInternal(endpoint, request, timeout, SENDER_WITH_RETRY);
  }

  private <T> ActorFuture<BrokerResponse<T>> sendRequestInternal(
      final BrokerEndpoint endpoint,
      final BrokerRequest<T> request,
      final Duration timeout,
      final TransportRequestSender sender) {
    final var normalizedEndpoint = Objects.requireNonNull(endpoint, "endpoint");
    final var normalizedRequest = Objects.requireNonNull(request, "request");
    final var normalizedTimeout = Objects.requireNonNull(timeout, "timeout");

    try {
      normalizedRequest.serializeValue();
    } catch (final Throwable error) {
      return failedFuture(new BrokerResponseException("Failed to serialize broker request", error));
    }

    final ActorFuture<BrokerResponse<T>> transportResponse;
    try {
      transportResponse =
          (normalizedRequest.allowsTransportRetry() ? sender : SENDER_WITHOUT_RETRY)
              .send(
                  transport, normalizedEndpoint, normalizedRequest, normalizedTimeout, maxRetries);
    } catch (final Throwable error) {
      return failedFuture(unwrap(error));
    }

    return normalizeResponse(transportResponse);
  }

  private static <T> ActorFuture<BrokerResponse<T>> normalizeResponse(
      final ActorFuture<BrokerResponse<T>> transportResponse) {
    final var result = new CompletableActorFuture<BrokerResponse<T>>();
    Objects.requireNonNull(transportResponse, "transportResponse")
        .onComplete(
            (response, error) -> {
              if (error != null) {
                fail(result, unwrap(error));
                return;
              }

              handleResponse(response, result);
            },
            Runnable::run);
    return result;
  }

  private <T> ActorFuture<BrokerResponse<T>> sendWithEndpointRefresh(
      final Supplier<BrokerEndpoint> endpointSupplier,
      final BrokerRequest<T> request,
      final int retriesRemaining) {
    final ActorFuture<BrokerResponse<T>> result = new CompletableActorFuture<>();
    final ActorFuture<BrokerResponse<T>> attempt;
    try {
      final BrokerEndpoint endpoint =
          Objects.requireNonNull(endpointSupplier.get(), "resolved broker endpoint");
      attempt =
          transport.sendRequestWithRetry(
              endpoint, request, requestTimeout, 0, BrokerRequestManager::isRetryable);
    } catch (final Throwable error) {
      retryWithFreshEndpoint(endpointSupplier, request, retriesRemaining, result, error);
      return result;
    }
    attempt.onComplete(
        (response, error) -> {
          if (error == null) {
            result.complete(response);
          } else {
            retryWithFreshEndpoint(endpointSupplier, request, retriesRemaining, result, error);
          }
        },
        Runnable::run);
    return result;
  }

  private <T> void retryWithFreshEndpoint(
      final Supplier<BrokerEndpoint> endpointSupplier,
      final BrokerRequest<T> request,
      final int retriesRemaining,
      final ActorFuture<BrokerResponse<T>> result,
      final Throwable error) {
    final Throwable unwrapped = unwrap(error);
    if (retriesRemaining <= 0 || !isSafeToRetry(unwrapped)) {
      result.completeExceptionally(unwrapped);
      return;
    }
    sendWithEndpointRefresh(endpointSupplier, request, retriesRemaining - 1)
        .onComplete(result, Runnable::run);
  }

  private static boolean isSafeToRetry(final Throwable error) {
    return error instanceof BrokerRequestRejectedBeforeAppendException;
  }

  private static <T> void handleResponse(
      final BrokerResponse<T> response, final CompletableActorFuture<BrokerResponse<T>> result) {
    try {
      if (response == null) {
        fail(result, new BrokerResponseException("Broker returned a null response"));
      } else if (response.isResponse()) {
        complete(result, response);
      } else if (response.isRejection()) {
        fail(result, new BrokerRejectionException(response.getRejection()));
      } else if (response.isError()) {
        fail(result, new BrokerErrorException(response.getError()));
      } else {
        fail(
            result,
            new BrokerResponseException(
                "Expected broker response to be response, rejection, or error"));
      }
    } catch (final RuntimeException error) {
      fail(result, new BrokerResponseException("Failed to handle broker response", error));
    }
  }

  private static <T> CompletableActorFuture<T> failedFuture(final Throwable error) {
    final var future = new CompletableActorFuture<T>();
    future.completeExceptionally(error);
    return future;
  }

  private static <T> void complete(final CompletableActorFuture<T> future, final T value) {
    if (!future.isDone()) {
      future.complete(value);
    }
  }

  private static void fail(final CompletableActorFuture<?> future, final Throwable error) {
    if (!future.isDone()) {
      future.completeExceptionally(error);
    }
  }

  private static Throwable unwrap(final Throwable error) {
    if (error instanceof CompletionException completionException
        && completionException.getCause() != null) {
      return completionException.getCause();
    }
    return error;
  }

  @Override
  public void close() throws Exception {
    transport.close();
  }

  @FunctionalInterface
  private interface TransportRequestSender {
    <T> ActorFuture<BrokerResponse<T>> send(
        BrokerClientTransport transport,
        BrokerEndpoint endpoint,
        BrokerRequest<T> request,
        Duration requestTimeout,
        int maxRetries);
  }
}
