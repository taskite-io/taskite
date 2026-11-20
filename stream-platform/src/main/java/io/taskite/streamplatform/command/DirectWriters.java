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
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.streamplatform.api.exception.ExceededBatchRecordSizeException;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.api.state.EventAppliers;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.PostCommitActionWriter;
import io.taskite.streamplatform.api.writer.StateWriter;
import io.taskite.streamplatform.api.writer.TypedCommandWriter;
import io.taskite.streamplatform.api.writer.TypedRejectionWriter;
import io.taskite.streamplatform.api.writer.TypedResponseWriter;
import io.taskite.streamplatform.api.writer.Writers;
import io.taskite.streamplatform.logstream.LogAppendEntry;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import org.agrona.MutableDirectBuffer;

/** Direct command writers backed by follow-up appends and eager event application. */
public final class DirectWriters implements Writers {
  private final TypedCommandWriter commandWriter;
  private final StateWriter stateWriter;
  private final TypedRejectionWriter rejectionWriter;
  private final DeferredTypedResponseWriter responseWriter;
  private final PostCommitActionWriter postCommitActionWriter;
  private final AppendSupport appendSupport;
  private final Executor executor;

  public DirectWriters(
      final RecordAppender appender,
      final CommandFailureObserver failureObserver,
      final EventAppliers eventAppliers,
      final KeyGenerator keyGenerator,
      final Executor executor) {
    this(appender, failureObserver, eventAppliers, keyGenerator, Integer.MAX_VALUE, executor);
  }

  public DirectWriters(
      final RecordAppender appender,
      final CommandFailureObserver failureObserver,
      final EventAppliers eventAppliers,
      final KeyGenerator keyGenerator,
      final int maxBatchBytes,
      final Executor executor) {
    this(appender, failureObserver, eventAppliers, keyGenerator, maxBatchBytes, null, executor);
  }

  public DirectWriters(
      final RecordAppender appender,
      final CommandFailureObserver failureObserver,
      final EventAppliers eventAppliers,
      final KeyGenerator keyGenerator,
      final int maxBatchBytes,
      final TypedEventRegistry eventRegistry,
      final Executor executor) {
    final ProcessingBatchResult batchResult =
        new ProcessingBatchResult(maxBatchBytes, eventRegistry);
    responseWriter = new DeferredTypedResponseWriter(failureObserver, batchResult);
    postCommitActionWriter = batchResult::appendPostCommitAction;
    appendSupport = new AppendSupport(appender, responseWriter, batchResult);
    commandWriter =
        new TypedCommandWriter() {
          @Override
          public void appendFollowUpCommand(long key, Intent intent, UnifiedRecordValue value) {
            appendSupport.appendCommand(key, intent, value);
          }

          @Override
          public void appendFollowUpCommand(
              long key, ValueType valueType, Intent intent, UnifiedRecordValue value) {
            appendSupport.appendCommand(key, valueType, intent, value);
          }
        };
    rejectionWriter = appendSupport::appendRejection;
    stateWriter = new EagerStateWriter(appendSupport, eventAppliers, keyGenerator);
    this.executor = Objects.requireNonNull(executor, "executor");
  }

  @Override
  public TypedCommandWriter command() {
    return commandWriter;
  }

  @Override
  public StateWriter state() {
    return stateWriter;
  }

  @Override
  public TypedRejectionWriter rejection() {
    return rejectionWriter;
  }

  @Override
  public TypedResponseWriter response() {
    return responseWriter;
  }

  @Override
  public PostCommitActionWriter postCommit() {
    return postCommitActionWriter;
  }

  public ActorFuture<Void> awaitAppends() {
    return appendSupport.awaitAppends(executor);
  }

  public long highestWrittenPosition() {
    return appendSupport.highestWrittenPosition();
  }

  public List<LoggedEvent> writtenRecords() {
    return appendSupport.writtenRecords();
  }

  public List<LoggedEvent> responses() {
    return appendSupport.responses();
  }

  public List<Runnable> postCommitActions() {
    return appendSupport.postCommitActions();
  }

  public boolean isEmpty() {
    return appendSupport.isEmpty();
  }

  public void beginBatch(final TypedRecord<?> rootCommand) {
    appendSupport.beginBatch(rootCommand);
    beginCommand(rootCommand);
  }

  public void beginCommand(final TypedRecord<?> command) {
    appendSupport.beginCommand(command);
  }

  public int recordCount() {
    return appendSupport.recordCount();
  }

  public LogAppendEntry recordAt(final int index) {
    return appendSupport.recordAt(index);
  }

  public void markProcessed(final int index) {
    appendSupport.markProcessed(index);
  }

  public long payloadBytes() {
    return appendSupport.payloadBytes();
  }

  private static final class AppendSupport {
    private final RecordAppender appender;
    private final DeferredTypedResponseWriter responseWriter;
    private final ProcessingBatchResult batchResult;
    private long highestPosition = -1L;
    private TypedRecord<?> commandRecord;

    private AppendSupport(
        final RecordAppender appender,
        final DeferredTypedResponseWriter responseWriter,
        final ProcessingBatchResult batchResult) {
      this.appender = Objects.requireNonNull(appender, "appender");
      this.responseWriter = Objects.requireNonNull(responseWriter, "responseWriter");
      this.batchResult = Objects.requireNonNull(batchResult, "batchResult");
    }

    private void beginBatch(final TypedRecord<?> rootCommand) {
      batchResult.reset(rootCommand.getPosition());
      highestPosition = -1L;
    }

    private void beginCommand(final TypedRecord<?> commandRecord) {
      this.commandRecord = Objects.requireNonNull(commandRecord, "commandRecord");
    }

    private void appendCommand(
        final long key, final Intent intent, final UnifiedRecordValue value) {
      appendCommand(key, commandRecord().getValueType(), intent, value);
    }

    private void appendCommand(
        final long key,
        final ValueType valueType,
        final Intent intent,
        final UnifiedRecordValue value) {
      final TypedRecord<?> command = commandRecord();
      append(
          LogAppendEntry.of(
              key,
              copyFrom(command).recordType(RecordType.COMMAND).valueType(valueType).intent(intent),
              value));
    }

    private void appendEvent(final long key, final Intent intent, final UnifiedRecordValue value) {
      appendEvent(key, commandRecord().getValueType(), intent, value);
    }

    private void appendEvent(
        final long key,
        final ValueType valueType,
        final Intent intent,
        final UnifiedRecordValue value) {
      append(
          LogAppendEntry.of(
              key,
              copyFrom(commandRecord())
                  .recordType(RecordType.EVENT)
                  .valueType(valueType)
                  .intent(intent),
              value));
    }

    private void appendRejection(final RejectionType rejectionType, final String rejectionReason) {
      final TypedRecord<?> command = commandRecord();
      append(
          LogAppendEntry.of(
              command.getKey(),
              copyFrom(command)
                  .recordType(RecordType.COMMAND_REJECTION)
                  .valueType(command.getValueType())
                  .intent(command.getIntent())
                  .rejectionType(rejectionType)
                  .rejectionReason(rejectionReason),
              EmptyRecordValue.INSTANCE));
    }

    private void append(final LogAppendEntry followUpEntry) {
      batchResult.append(followUpEntry);
    }

    private ActorFuture<Void> awaitAppends(final Executor executor) {
      final List<LogAppendEntry> records = batchResult.records();
      if (records.isEmpty()) {
        return CompletableActorFuture.completed(null);
      }
      final CompletableActorFuture<Void> result = new CompletableActorFuture<>();
      final ActorFuture<Long> append;
      try {
        append = appender.append(records, batchResult.sourcePosition());
      } catch (final Throwable error) {
        failResponse(error);
        result.completeExceptionally(error);
        return result;
      }
      append.onComplete(
          (position, error) -> {
            if (error != null) {
              failResponse(error);
              result.completeExceptionally(error);
              return;
            }
            if (position != null && position > highestPosition) {
              highestPosition = position;
            }
            result.complete(null);
          },
          executor);
      return result;
    }

    private long highestWrittenPosition() {
      return highestPosition;
    }

    private List<LoggedEvent> writtenRecords() {
      return batchResult.asLoggedEvents();
    }

    private List<LoggedEvent> responses() {
      return batchResult.responses();
    }

    private List<Runnable> postCommitActions() {
      return batchResult.postCommitActions();
    }

    private boolean isEmpty() {
      return batchResult.isEmpty();
    }

    private int recordCount() {
      return batchResult.size();
    }

    private LogAppendEntry recordAt(final int index) {
      return batchResult.get(index);
    }

    private void markProcessed(final int index) {
      batchResult.markProcessed(index);
    }

    private long payloadBytes() {
      return batchResult.payloadBytes();
    }

    private void failResponse(final Throwable error) {
      if (!ExceededBatchRecordSizeException.causedBy(error)) {
        responseWriter.fail(commandRecord().getRequestId(), error);
      }
    }

    private RecordMetadata copyFrom(final TypedRecord<?> command) {
      return new RecordMetadata()
          .requestStreamId(command.getRequestStreamId())
          .requestId(command.getRequestId())
          .recordVersion(command.getRecordVersion());
    }

    private TypedRecord<?> commandRecord() {
      return Objects.requireNonNull(commandRecord, "commandRecord");
    }
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

  private static final class EagerStateWriter implements StateWriter {
    private final AppendSupport appendSupport;
    private final EventAppliers eventAppliers;
    private final KeyGenerator keyGenerator;

    private EagerStateWriter(
        final AppendSupport appendSupport,
        final EventAppliers eventAppliers,
        final KeyGenerator keyGenerator) {
      this.appendSupport = Objects.requireNonNull(appendSupport, "appendSupport");
      this.eventAppliers = Objects.requireNonNull(eventAppliers, "eventAppliers");
      this.keyGenerator = Objects.requireNonNull(keyGenerator, "keyGenerator");
    }

    @Override
    public void appendFollowUpEvent(
        final long key, final Intent intent, final UnifiedRecordValue value) {
      appendFollowUpEvent(key, intent, value, eventAppliers.getLatestVersion(intent));
    }

    @Override
    public void appendFollowUpEvent(
        final long key,
        final Intent intent,
        final UnifiedRecordValue value,
        final int recordVersion) {
      appendFollowUpEvent(key, null, intent, value, recordVersion);
    }

    @Override
    public void appendFollowUpEvent(
        final long key,
        final ValueType valueType,
        final Intent intent,
        final UnifiedRecordValue value) {
      appendFollowUpEvent(key, valueType, intent, value, eventAppliers.getLatestVersion(intent));
    }

    @Override
    public void appendFollowUpEvent(
        final long key,
        final ValueType valueType,
        final Intent intent,
        final UnifiedRecordValue value,
        final int recordVersion) {
      keyGenerator.setKeyIfHigher(key);
      eventAppliers.applyState(key, intent, value, recordVersion);
      if (valueType == null) {
        appendSupport.appendEvent(key, intent, value);
      } else {
        appendSupport.appendEvent(key, valueType, intent, value);
      }
    }
  }
}
