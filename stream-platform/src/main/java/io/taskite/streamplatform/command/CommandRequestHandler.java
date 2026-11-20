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
package io.taskite.streamplatform.command;

import io.taskite.protocol.impl.encoding.ErrorResponse;
import io.taskite.protocol.record.ErrorCode;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import org.agrona.concurrent.UnsafeBuffer;

public final class CommandRequestHandler {
  private final Function<LoggedEvent, ActorFuture<LoggedEvent>> commandWriter;
  private final CommandRequestReader requestReader;
  private final CommandResponseEncoder responseWriter = new CommandResponseEncoder();

  public CommandRequestHandler(
      final Function<LoggedEvent, ActorFuture<LoggedEvent>> commandWriter,
      final TypedEventRegistry registry,
      final long clusterKey) {
    this.commandWriter = Objects.requireNonNull(commandWriter, "commandWriter");
    requestReader =
        new CommandRequestReader(Objects.requireNonNull(registry, "registry"), clusterKey);
  }

  /**
   * Decodes a serialized {@code ExecuteCommandRequest} and submits the resulting command record.
   */
  public ActorFuture<byte[]> handleRequest(final byte[] requestBytes) {
    Objects.requireNonNull(requestBytes, "requestBytes");

    final LoggedEvent requestRecord;
    synchronized (requestReader) {
      try {
        requestReader.reset();
        requestReader.wrap(new UnsafeBuffer(requestBytes), 0, requestBytes.length);
        // The wire request carries no correlation id; assign one so the response can be routed back
        // to this caller.
        requestReader.metadata().requestId(UUID.randomUUID().toString());
        requestRecord = requestReader.record();
      } catch (final Throwable error) {
        return failed(error);
      }
    }

    return handleRequest(requestRecord);
  }

  /** Submits an already-decoded command record and encodes the resulting response. */
  public ActorFuture<byte[]> handleRequest(final LoggedEvent requestRecord) {
    final var result = new CompletableActorFuture<byte[]>();
    try {
      commandWriter
          .apply(requestRecord)
          .onComplete(
              (response, error) -> {
                if (error != null) {
                  completeFailure(result, error);
                  return;
                }
                try {
                  result.complete(encodeResponse(response));
                } catch (final Throwable encodingError) {
                  result.completeExceptionally(encodingError);
                }
              },
              Runnable::run);
    } catch (final Throwable error) {
      completeFailure(result, error);
    }
    return result;
  }

  private static void completeFailure(final ActorFuture<byte[]> result, final Throwable error) {
    if (error instanceof PartitionNotLeaderBeforeAppendException) {
      final var response =
          new ErrorResponse()
              .setErrorCode(ErrorCode.PARTITION_LEADER_MISMATCH)
              .setErrorMessage(
                  new UnsafeBuffer(error.getMessage().getBytes(StandardCharsets.UTF_8)));
      final var buffer = new UnsafeBuffer(new byte[response.getLength()]);
      response.write(buffer, 0);
      result.complete(buffer.byteArray());
    } else {
      result.completeExceptionally(error);
    }
  }

  private byte[] encodeResponse(final LoggedEvent responseRecord) {
    synchronized (responseWriter) {
      responseWriter.reset();
      responseWriter.wrap(responseRecord);
      final var buffer = new UnsafeBuffer(new byte[responseWriter.getLength()]);
      responseWriter.write(buffer, 0);
      return Arrays.copyOf(buffer.byteArray(), responseWriter.getLength());
    }
  }

  private static <T> ActorFuture<T> failed(final Throwable error) {
    return CompletableActorFuture.completedExceptionally(error);
  }
}
