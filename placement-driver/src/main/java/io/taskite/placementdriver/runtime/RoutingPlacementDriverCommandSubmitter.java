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
package io.taskite.placementdriver.runtime;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.taskite.placementdriver.common.mapper.PlacementDriverTopologyMapper;
import io.taskite.placementdriver.protocol.PlacementDriverGrpc;
import io.taskite.placementdriver.protocol.UpdatePlacementDriverTopologyRequest;
import io.taskite.placementdriver.protocol.UpdatePlacementDriverTopologyResponse;
import io.taskite.placementdriver.runtime.transport.InternalPlacementDriverCommandGrpc;
import io.taskite.protocol.impl.encoding.ExecuteCommandRequest;
import io.taskite.protocol.impl.encoding.ExecuteCommandResponse;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.util.BufferUtil;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.agrona.concurrent.UnsafeBuffer;

/** Routes runtime-generated commands locally or to the currently known placement-driver leader. */
final class RoutingPlacementDriverCommandSubmitter
    implements PlacementDriverCommandSubmitter, AutoCloseable {
  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);

  private final PlacementDriverCommandSubmitter localSubmitter;
  private final Function<MemberTopologyRecord, ActorFuture<MemberTopologyRecord>>
      localTopologyUpdater;
  private final PlacementDriverClusterLeaderLocator leaderLocator;
  private final TypedEventRegistry eventRegistry;
  private final Map<PlacementDriverEndpoint, ManagedChannel> channels = new ConcurrentHashMap<>();
  private volatile PlacementDriverEndpoint leaderHint;

  RoutingPlacementDriverCommandSubmitter(
      final PlacementDriverCommandSubmitter localSubmitter,
      final Function<MemberTopologyRecord, ActorFuture<MemberTopologyRecord>> localTopologyUpdater,
      final PlacementDriverClusterLeaderLocator leaderLocator,
      final TypedEventRegistry eventRegistry,
      final String bootstrapLeaderEndpoint) {
    this.localSubmitter = localSubmitter;
    this.localTopologyUpdater = localTopologyUpdater;
    this.leaderLocator = leaderLocator;
    this.eventRegistry = eventRegistry;
    leaderHint = parseEndpoint(bootstrapLeaderEndpoint).orElse(null);
  }

  @Override
  public ActorFuture<LoggedEvent> submitPlacementDriverCommand(final LoggedEvent command) {
    if (leaderLocator.isLocalLeader()) {
      return localSubmitter.submitPlacementDriverCommand(command);
    }
    final PlacementDriverEndpoint endpoint =
        leaderLocator.locateLeaderEndpoint().orElseGet(() -> leaderHint);
    if (endpoint == null) {
      return CompletableActorFuture.completedExceptionally(
          new IllegalStateException("placement-driver leader endpoint is not available"));
    }
    return submitRemote(command, endpoint, true);
  }

  private ActorFuture<LoggedEvent> submitRemote(
      final LoggedEvent command,
      final PlacementDriverEndpoint endpoint,
      final boolean retryNotLeader) {
    final CompletableActorFuture<LoggedEvent> result = new CompletableActorFuture<>();
    InternalPlacementDriverCommandGrpc.executeCommand(
            channel(endpoint), encode(command), REQUEST_TIMEOUT)
        .onComplete(
            (bytes, error) -> {
              if (error == null) {
                try {
                  leaderHint = endpoint;
                  result.complete(decode(bytes));
                } catch (final RuntimeException decodeError) {
                  result.completeExceptionally(decodeError);
                }
                return;
              }
              final Optional<PlacementDriverEndpoint> redirected = notLeaderEndpoint(error);
              if (retryNotLeader && redirected.isPresent() && !redirected.get().equals(endpoint)) {
                leaderHint = redirected.get();
                submitRemote(command, redirected.get(), false)
                    .onComplete(
                        (response, retryError) -> {
                          if (retryError == null) {
                            result.complete(response);
                          } else {
                            result.completeExceptionally(retryError);
                          }
                        });
              } else {
                result.completeExceptionally(error);
              }
            });
    return result;
  }

  ActorFuture<MemberTopologyRecord> updateTopology(final MemberTopologyRecord topology) {
    if (leaderLocator.isLocalLeader()) {
      return localTopologyUpdater.apply(topology);
    }
    final PlacementDriverEndpoint endpoint =
        leaderLocator.locateLeaderEndpoint().orElseGet(() -> leaderHint);
    if (endpoint == null) {
      return CompletableActorFuture.completedExceptionally(
          new IllegalStateException("placement-driver leader endpoint is not available"));
    }
    return updateRemoteTopology(topology, endpoint, true);
  }

  private ActorFuture<MemberTopologyRecord> updateRemoteTopology(
      final MemberTopologyRecord topology,
      final PlacementDriverEndpoint endpoint,
      final boolean retryNotLeader) {
    final CompletableActorFuture<MemberTopologyRecord> result = new CompletableActorFuture<>();
    PlacementDriverGrpc.newStub(channel(endpoint))
        .withDeadlineAfter(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
        .updatePlacementDriverTopology(
            UpdatePlacementDriverTopologyRequest.newBuilder()
                .setTopology(PlacementDriverTopologyMapper.toProto(topology))
                .build(),
            new StreamObserver<UpdatePlacementDriverTopologyResponse>() {
              private MemberTopologyRecord committed;

              @Override
              public void onNext(final UpdatePlacementDriverTopologyResponse response) {
                try {
                  committed = PlacementDriverTopologyMapper.fromProto(response.getTopology());
                } catch (final RuntimeException error) {
                  result.completeExceptionally(error);
                }
              }

              @Override
              public void onError(final Throwable error) {
                final var redirected = notLeaderEndpoint(error);
                if (retryNotLeader
                    && redirected.isPresent()
                    && !redirected.get().equals(endpoint)) {
                  leaderHint = redirected.get();
                  updateRemoteTopology(topology, redirected.get(), false)
                      .onComplete(
                          (response, retryError) -> {
                            if (retryError == null) {
                              result.complete(response);
                            } else {
                              result.completeExceptionally(retryError);
                            }
                          });
                } else {
                  result.completeExceptionally(error);
                }
              }

              @Override
              public void onCompleted() {
                if (committed == null) {
                  result.completeExceptionally(
                      new IllegalStateException("missing committed topology"));
                } else {
                  leaderHint = endpoint;
                  result.complete(committed);
                }
              }
            });
    return result;
  }

  private byte[] encode(final LoggedEvent command) {
    final UnsafeBuffer value = new UnsafeBuffer(new byte[command.value().getLength()]);
    command.value().write(value, 0);
    final ExecuteCommandRequest request =
        new ExecuteCommandRequest()
            .setPartitionId(1)
            .setValueType(command.metadata().getValueType())
            .setIntent(command.metadata().getIntent())
            .setValue(value, 0, value.capacity());
    if (command.key() > 0) {
      request.setKey(command.key());
    }
    final UnsafeBuffer encoded = new UnsafeBuffer(new byte[request.getLength()]);
    request.write(encoded, 0);
    return Arrays.copyOf(encoded.byteArray(), request.getLength());
  }

  private LoggedEvent decode(final byte[] bytes) {
    final ExecuteCommandResponse response = new ExecuteCommandResponse();
    response.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
    if (response.getRecordType() == RecordType.COMMAND_REJECTION) {
      throw new PlacementDriverRejectionException(
          response.getRejectionType(), BufferUtil.bufferAsString(response.getRejectionReason()));
    }
    final UnifiedRecordValue value =
        eventRegistry.create(response.getValueType(), response.getIntent());
    value.wrap(response.getValue(), 0, response.getValue().capacity());
    return LoggedEvent.ofRecord(
        response.getKey(),
        new RecordMetadata()
            .recordType(response.getRecordType())
            .valueType(response.getValueType())
            .intent(response.getIntent())
            .rejectionType(
                response.getRejectionType() == null
                    ? RejectionType.NULL_VAL
                    : response.getRejectionType()),
        value);
  }

  private ManagedChannel channel(final PlacementDriverEndpoint endpoint) {
    return channels.computeIfAbsent(
        endpoint,
        ignored ->
            NettyChannelBuilder.forAddress(endpoint.host(), endpoint.port())
                .usePlaintext()
                .build());
  }

  private static Optional<PlacementDriverEndpoint> notLeaderEndpoint(final Throwable error) {
    if (!(error instanceof StatusRuntimeException statusError)) {
      return Optional.empty();
    }
    final String description = statusError.getStatus().getDescription();
    if (description == null || !description.startsWith("NOT_LEADER:")) {
      return Optional.empty();
    }
    return parseEndpoint(description.substring("NOT_LEADER:".length()));
  }

  private static Optional<PlacementDriverEndpoint> parseEndpoint(final String value) {
    if (value == null || value.isBlank()) {
      return Optional.empty();
    }
    try {
      final PlacementDriverEndpoint endpoint = PlacementDriverEndpoint.parse(value);
      return endpoint.host().isBlank() || endpoint.port() <= 0
          ? Optional.empty()
          : Optional.of(endpoint);
    } catch (final RuntimeException ignored) {
      return Optional.empty();
    }
  }

  @Override
  public void close() {
    channels.values().forEach(ManagedChannel::shutdownNow);
    channels.clear();
  }
}
