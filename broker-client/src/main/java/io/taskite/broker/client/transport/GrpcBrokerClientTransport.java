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

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.taskite.broker.client.request.BrokerRequest;
import io.taskite.broker.client.response.BrokerRequestRejectedBeforeAppendException;
import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.protocol.record.ErrorCode;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.topology.BrokerEndpoint;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class GrpcBrokerClientTransport implements BrokerClientTransport {
  private final ConcurrentHashMap<ChannelKey, ManagedChannel> channels = new ConcurrentHashMap<>();
  private final ScheduledExecutorService timeoutExecutor;

  public GrpcBrokerClientTransport() {
    timeoutExecutor =
        Executors.newSingleThreadScheduledExecutor(new BrokerClientTransportThreadFactory());
  }

  @Override
  public <T> ActorFuture<BrokerResponse<T>> sendRequest(
      final BrokerEndpoint endpoint,
      final BrokerRequest<T> request,
      final Duration requestTimeout) {
    final BrokerEndpoint resolvedEndpoint = resolveEndpoint(endpoint);
    final ChannelKey key = new ChannelKey(resolvedEndpoint, request.getRoutingEpoch());
    final ManagedChannel channel = channel(key);
    final ActorFuture<BrokerResponse<T>> response =
        BrokerRequestGrpc.sendRequest(channel, request.serializeRequest(), requestTimeout)
            .thenApply(
                responseBytes -> {
                  final var decoded = request.getResponse(new UnsafeBuffer(responseBytes));
                  if (decoded.isError()
                      && decoded.getError().code() == ErrorCode.PARTITION_LEADER_MISMATCH) {
                    throw new BrokerRequestRejectedBeforeAppendException();
                  }
                  return decoded;
                });
    return invalidateChannelOnFailure(withTimeout(response, requestTimeout), key, channel);
  }

  @Override
  public <T> ActorFuture<BrokerResponse<T>> sendRequestWithRetry(
      final BrokerEndpoint endpoint,
      final BrokerRequest<T> request,
      final Duration requestTimeout,
      final int maxRetries,
      final Predicate<DirectBuffer> retryable) {
    if (!request.allowsTransportRetry()) {
      return sendRequest(endpoint, request, requestTimeout);
    }
    return sendWithRetry(
        () -> sendRequestForRetry(endpoint, request, requestTimeout, retryable), maxRetries);
  }

  /**
   * Sends one attempt and decides retryability from the raw response bytes. A retryable routing
   * error, such as a partition leader mismatch, completes the attempt with {@link
   * BrokerRequestRejectedBeforeAppendException} so {@link #sendWithRetry} can try again. Any other
   * response is decoded and returned unchanged.
   */
  private <T> ActorFuture<BrokerResponse<T>> sendRequestForRetry(
      final BrokerEndpoint endpoint,
      final BrokerRequest<T> request,
      final Duration requestTimeout,
      final Predicate<DirectBuffer> retryable) {
    final BrokerEndpoint resolvedEndpoint = resolveEndpoint(endpoint);
    final ManagedChannel channel =
        channel(new ChannelKey(resolvedEndpoint, request.getRoutingEpoch()));
    final ActorFuture<BrokerResponse<T>> response =
        BrokerRequestGrpc.sendRequest(channel, request.serializeRequest(), requestTimeout)
            .thenApply(
                responseBytes -> {
                  final DirectBuffer buffer = new UnsafeBuffer(responseBytes);
                  if (retryable != null && retryable.test(buffer)) {
                    throw new BrokerRequestRejectedBeforeAppendException();
                  }
                  return request.getResponse(buffer);
                });
    return invalidateChannelOnFailure(
        withTimeout(response, requestTimeout),
        new ChannelKey(resolvedEndpoint, request.getRoutingEpoch()),
        channel);
  }

  /**
   * Returns the channel for {@code key}, opening it on demand. Because the key includes the
   * fenced-recovery routing epoch (G2), a request routed under a newer epoch never reuses a channel
   * opened for a superseded generation. When a higher-epoch channel is opened for an endpoint, any
   * lower-epoch channels for that same endpoint are evicted and closed so stale connections to a
   * demoted/quarantined generation drain promptly.
   */
  private ManagedChannel channel(final ChannelKey key) {
    final ManagedChannel channel =
        channels.computeIfAbsent(
            key,
            ignored ->
                NettyChannelBuilder.forAddress(key.endpoint().host(), key.endpoint().port())
                    .usePlaintext()
                    .build());
    evictSupersededEpochs(key);
    if (channel.getState(false) == ConnectivityState.TRANSIENT_FAILURE) {
      channel.resetConnectBackoff();
    }
    return channel;
  }

  private void evictSupersededEpochs(final ChannelKey current) {
    if (current.routingEpoch() <= 0L) {
      return;
    }
    for (final Map.Entry<ChannelKey, ManagedChannel> entry : channels.entrySet()) {
      final ChannelKey key = entry.getKey();
      if (key.endpoint().equals(current.endpoint())
          && key.routingEpoch() < current.routingEpoch()
          && channels.remove(key, entry.getValue())) {
        entry.getValue().shutdownNow();
      }
    }
  }

  /** Visible for testing: number of open channels currently cached. */
  int openChannelCount() {
    return channels.size();
  }

  /**
   * Visible for testing: opens (or reuses) the channel for {@code endpoint} at {@code routingEpoch}
   * through the same cache path as live traffic, exercising epoch-based eviction. gRPC channels are
   * lazy, so this performs no network I/O until an RPC is issued.
   */
  void openChannelForTesting(final BrokerEndpoint endpoint, final long routingEpoch) {
    channel(new ChannelKey(resolveEndpoint(endpoint), routingEpoch));
  }

  private static BrokerEndpoint resolveEndpoint(final BrokerEndpoint endpoint) {
    if (endpoint == null) {
      throw new IllegalStateException("No broker endpoint available");
    }
    return endpoint;
  }

  private <T> ActorFuture<T> sendWithRetry(
      final Supplier<ActorFuture<T>> attemptSupplier, final int retriesRemaining) {
    final ActorFuture<T> result = new CompletableActorFuture<>();
    try {
      attemptSupplier
          .get()
          .onComplete(
              (response, error) -> {
                if (error == null) {
                  complete(result, response);
                  return;
                }
                if (retriesRemaining <= 0
                    || !(unwrap(error) instanceof BrokerRequestRejectedBeforeAppendException)) {
                  fail(result, unwrap(error));
                  return;
                }
                sendWithRetry(attemptSupplier, retriesRemaining - 1)
                    .onComplete(result, Runnable::run);
              },
              Runnable::run);
    } catch (final Throwable error) {
      if (retriesRemaining <= 0
          || !(unwrap(error) instanceof BrokerRequestRejectedBeforeAppendException)) {
        fail(result, unwrap(error));
      } else {
        sendWithRetry(attemptSupplier, retriesRemaining - 1).onComplete(result, Runnable::run);
      }
    }
    return result;
  }

  private <T> ActorFuture<T> withTimeout(final ActorFuture<T> attempt, final Duration timeout) {
    final ActorFuture<T> result = new CompletableActorFuture<>();
    final var timeoutTask =
        timeoutExecutor.schedule(
            () ->
                fail(
                    result,
                    new TimeoutException(
                        "broker request timed out after " + timeout.toMillis() + " ms")),
            timeout.toMillis(),
            TimeUnit.MILLISECONDS);
    attempt.onComplete(
        (response, error) -> {
          timeoutTask.cancel(false);
          if (error == null) {
            complete(result, response);
          } else {
            fail(result, error);
          }
        },
        Runnable::run);
    return result;
  }

  private <T> ActorFuture<T> invalidateChannelOnFailure(
      final ActorFuture<T> attempt, final ChannelKey key, final ManagedChannel channel) {
    final ActorFuture<T> result = new CompletableActorFuture<>();
    attempt.onComplete(
        (response, error) -> {
          if (error == null) {
            complete(result, response);
          } else {
            // Evict before exposing the failure so a subsequent call opens a fresh channel.
            if (channels.remove(key, channel)) {
              channel.shutdown();
            }
            fail(result, error);
          }
        },
        Runnable::run);
    return result;
  }

  private static <T> void complete(final ActorFuture<T> future, final T value) {
    if (!future.isDone()) {
      future.complete(value);
    }
  }

  private static <T> CompletableActorFuture<T> failed(final Throwable error) {
    final var future = new CompletableActorFuture<T>();
    future.completeExceptionally(error);
    return future;
  }

  private static void fail(final ActorFuture<?> future, final Throwable error) {
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
  public void close() {
    timeoutExecutor.shutdownNow();
    for (Map.Entry<ChannelKey, ManagedChannel> entry : channels.entrySet()) {
      closeChannel(entry.getValue());
    }
    channels.clear();
  }

  private void closeChannel(final ManagedChannel channel) {
    channel.shutdown();
    try {
      if (!channel.awaitTermination(1, TimeUnit.SECONDS)) {
        channel.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      channel.shutdownNow();
    }
  }

  private static final class BrokerClientTransportThreadFactory implements ThreadFactory {
    @Override
    public Thread newThread(final Runnable runnable) {
      final Thread thread = new Thread(runnable, "grpc-broker-client-transport-timeout");
      thread.setDaemon(true);
      return thread;
    }
  }

  /**
   * Channel-cache key that partitions connections by fenced-recovery routing epoch (G2) in addition
   * to the target endpoint, so traffic routed under a newer generation never shares a channel with
   * a superseded one.
   */
  private record ChannelKey(BrokerEndpoint endpoint, long routingEpoch) {
    private ChannelKey {
      routingEpoch = Math.max(0L, routingEpoch);
    }
  }
}
