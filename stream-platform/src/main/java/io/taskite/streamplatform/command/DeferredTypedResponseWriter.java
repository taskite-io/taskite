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

import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.streamplatform.api.writer.TypedResponseWriter;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.util.Objects;
import org.agrona.MutableDirectBuffer;

/** Accumulates command-bound responses for delivery after the processing transaction commits. */
final class DeferredTypedResponseWriter implements TypedResponseWriter {
  private final CommandFailureObserver failureObserver;
  private final ProcessingBatchResult batchResult;

  DeferredTypedResponseWriter(
      final CommandFailureObserver failureObserver, final ProcessingBatchResult batchResult) {
    this.failureObserver = Objects.requireNonNull(failureObserver, "failureObserver");
    this.batchResult = Objects.requireNonNull(batchResult, "batchResult");
  }

  @Override
  public void writeEventOnCommand(
      final long eventKey,
      final Intent eventIntent,
      final UnifiedRecordValue eventValue,
      final TypedRecord<?> sourceCommand) {
    putResponse(
        sourceCommand,
        LoggedEvent.ofRecord(
            eventKey,
            copyFrom(sourceCommand).recordType(RecordType.EVENT).intent(eventIntent),
            eventValue));
  }

  @Override
  public void writeRejectionOnCommand(
      final TypedRecord<?> sourceCommand, final RejectionType type, final String reason) {
    putResponse(
        sourceCommand,
        LoggedEvent.ofRecord(
            sourceCommand.getKey(),
            copyFrom(sourceCommand)
                .recordType(RecordType.COMMAND_REJECTION)
                .intent(sourceCommand.getIntent())
                .rejectionType(type)
                .rejectionReason(reason),
            EmptyRecordValue.INSTANCE));
  }

  void fail(final String requestId, final Throwable error) {
    failureObserver.onFailure(requestId, error);
  }

  private void putResponse(final TypedRecord<?> command, final LoggedEvent response) {
    batchResult.putResponse(command.getRequestStreamId(), command.getRequestId(), response);
  }

  private RecordMetadata copyFrom(final TypedRecord<?> command) {
    return new RecordMetadata()
        .requestStreamId(command.getRequestStreamId())
        .requestId(command.getRequestId())
        .valueType(command.getValueType())
        .recordVersion(command.getRecordVersion());
  }

  private static final class EmptyRecordValue extends UnifiedRecordValue {
    private static final EmptyRecordValue INSTANCE = new EmptyRecordValue();

    @Override
    public int getLength() {
      return 0;
    }

    @Override
    public void write(final MutableDirectBuffer buffer, final int offset) {}
  }
}
