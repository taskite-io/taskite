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
package io.taskite.streamplatform.processing;

import io.taskite.db.DbTransaction;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.state.processing.ProcessingStateStore;
import io.taskite.streamplatform.api.exception.ExceededBatchRecordSizeException;
import io.taskite.streamplatform.api.metrics.ProcessingMetrics;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;
import io.taskite.streamplatform.api.record.CommandResponseObserver;
import io.taskite.streamplatform.api.record.CommittedRecordObserver;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.api.writer.Writers;
import io.taskite.streamplatform.command.CommandFailureObserver;
import io.taskite.streamplatform.command.DirectWriters;
import io.taskite.streamplatform.logstream.LogStreamReader;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.streamplatform.record.TypedRecordImpl;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Leader-only state machine that reads committed {@code COMMAND}s, produces follow-up records,
 * eagerly applies generated events, and skips those events or rejections when they are read back
 * from the log.
 *
 * <p>Self-driving: {@link #startProcessing(long)} seeks the reader past the last processed position
 * and kicks {@link #tryToReadNextRecord()}, which handles one record per pass and re-schedules
 * itself via {@code actor.run} once the command's appends complete. A command's whole effect
 * (business state, generated-key high-water and the processed position) commits atomically in one
 * transaction owned here.
 */
final class ProcessingStateMachine {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessingStateMachine.class);
  static final long UNSET_POSITION = -1L;
  private static final Duration READ_VISIBILITY_RETRY_DELAY = Duration.ofMillis(1);
  private static final int DEFAULT_FAST_DRAIN_RECORD_LIMIT = 64;
  private static final Duration DEFAULT_FAST_DRAIN_DURATION = Duration.ofMillis(1);

  private final int partitionId;
  private final TypedEventRegistry eventRegistry;
  private final ConcurrencyControl actor;
  private final LogStreamReader reader;
  private final ProcessingStateStore storage;
  private final LongSupplier committedPositionSupplier;
  private final LongConsumer appliedPositionConsumer;
  private final TypedRecordProcessorContext processorContext;
  private final TypedRecordProcessors typedRecordProcessors;
  private final DirectWriters writers;
  private final ProcessingBatch processingBatch;
  private final CommandFailureObserver failureObserver;
  private final CommandResponseObserver commandResponseObserver;
  private final CommittedRecordObserver committedRecordObserver;
  private final ProcessingMetrics processingMetrics;
  private final int maxCommandsInBatch;
  private final int maxRecordsInBatch;
  private final Duration maxBatchDuration;

  private boolean inProcessing;
  private boolean closed;
  private Runnable commitBoundaryTask = () -> {};
  private ScheduledTimer readVisibilityRetry;
  private DbTransaction currentTransaction;
  private long lastSuccessfulProcessedRecordPosition = UNSET_POSITION;
  private long lastConsumedRecordPosition = UNSET_POSITION;
  private long lastWrittenPosition = UNSET_POSITION;

  ProcessingStateMachine(
      final StreamStateMachineContext context,
      final TypedRecordProcessors typedRecordProcessors,
      final StreamProcessorContext.Batching batching) {
    partitionId = context.identity().partitionId();
    eventRegistry = Objects.requireNonNull(context.processing().eventRegistry(), "eventRegistry");
    actor = Objects.requireNonNull(context.runtime().actor(), "actor");
    reader = Objects.requireNonNull(context.runtime().reader(), "reader");
    storage = Objects.requireNonNull(context.state().storage(), "storage");
    this.committedPositionSupplier =
        Objects.requireNonNull(
            context.state().committedPositionSupplier(), "committedPositionSupplier");
    this.appliedPositionConsumer =
        Objects.requireNonNull(
            context.state().appliedPositionConsumer(), "appliedPositionConsumer");
    processorContext =
        Objects.requireNonNull(context.processing().processorContext(), "processorContext");
    this.typedRecordProcessors =
        Objects.requireNonNull(typedRecordProcessors, "typedRecordProcessors");
    writers = directWriters(this.processorContext.getWriters());
    processingBatch =
        new ProcessingBatch(partitionId, eventRegistry, typedRecordProcessors, writers);
    failureObserver =
        Objects.requireNonNull(context.observers().failureObserver(), "failureObserver");
    this.commandResponseObserver =
        Objects.requireNonNull(
            context.observers().commandResponseObserver(), "commandResponseObserver");
    this.committedRecordObserver =
        Objects.requireNonNull(
            context.observers().committedRecordObserver(), "committedRecordObserver");
    processingMetrics =
        Objects.requireNonNull(context.observers().processingMetrics(), "processingMetrics");
    maxCommandsInBatch = batching.maxCommandsInBatch();
    maxRecordsInBatch = batching.maxRecordsInBatch();
    maxBatchDuration = batching.maxBatchDuration();
  }

  /** Seeks the reader past {@code lastProcessedPosition} and starts the processing loop. */
  void startProcessing(final long lastProcessedPosition) {
    lastSuccessfulProcessedRecordPosition = lastProcessedPosition;
    lastConsumedRecordPosition = lastProcessedPosition;
    processingMetrics.onRecordPositions(
        lastConsumedRecordPosition, committedPositionSupplier.getAsLong());
    reader.seekToNextEvent(lastProcessedPosition);
    actor.run(this::tryToReadNextRecord);
  }

  /** Reads and processes the next committed command, then re-schedules itself. */
  void tryToReadNextRecord() {
    cancelReadVisibilityRetry();
    if (closed || inProcessing) {
      return;
    }
    commitBoundaryTask.run();
    final long drainStartedAt = System.nanoTime();
    int drainedRecords = 0;
    while (true) {
      final long committedPosition = committedPositionSupplier.getAsLong();
      processingMetrics.onRecordPositions(lastConsumedRecordPosition, committedPosition);
      if (reader.getNextPosition() > committedPosition) {
        return;
      }
      if (!reader.hasNext()) {
        scheduleReadVisibilityRetry();
        return;
      }
      final LoggedEvent entry = reader.next();
      if (entry == null) {
        return;
      }

      final RecordMetadata metadata = entry.readMetadata(new RecordMetadata());
      final RecordType recordType = metadata.getRecordType();
      final UnifiedRecordValue value = eventRegistry.read(entry, metadata);
      final TypedRecordImpl typedRecord = new TypedRecordImpl(partitionId);
      typedRecord.wrap(entry, metadata, value);
      if (recordType != RecordType.COMMAND) {
        // The leader eager-applied its events when processing the command; skip them on read-back,
        // but
        // notify so a pending client response completes from the committed EVENT/REJECTION.
        // All preceding commands have committed before this read. Expose the consumed log position
        // to the snapshot guard while keeping the durable recovery cursor at its source command.
        appliedPositionConsumer.accept(entry.getPosition());
        final LoggedEvent committedRecord = LoggedEvent.ofRecord(entry.getKey(), metadata, value);
        commandResponseObserver.onResponse(committedRecord);
        committedRecordObserver.onProcessed(committedRecord);
        processingMetrics.onSkipped();
        updateConsumedPosition(entry.getPosition());
      } else if (typedRecord.shouldSkipProcessing()) {
        processingMetrics.onSkipped();
        updateConsumedPosition(entry.getPosition());
      } else {
        processCommand(typedRecord);
        return;
      }

      drainedRecords++;
      if (canFastDrain(drainedRecords, drainStartedAt)) {
        continue;
      }
      actor.run(this::tryToReadNextRecord);
      return;
    }
  }

  private boolean canFastDrain(final int drainedRecords, final long startedAt) {
    final int recordLimit =
        maxRecordsInBatch > 0 ? maxRecordsInBatch : DEFAULT_FAST_DRAIN_RECORD_LIMIT;
    final Duration durationLimit =
        maxBatchDuration.isZero() ? DEFAULT_FAST_DRAIN_DURATION : maxBatchDuration;
    return drainedRecords < recordLimit && System.nanoTime() - startedAt < durationLimit.toNanos();
  }

  private void processCommand(final TypedRecordImpl typedRecord) {
    inProcessing = true;
    final long queueWaitMillis =
        typedRecord.getTimestamp() <= 0
            ? 0
            : Math.max(0, System.currentTimeMillis() - typedRecord.getTimestamp());
    processingMetrics.onPhaseCompleted(
        ProcessingMetrics.Phase.QUEUE_WAIT, TimeUnit.MILLISECONDS.toNanos(queueWaitMillis));
    processCommand(typedRecord, maxCommandsInBatch);
  }

  private void processCommand(final TypedRecordImpl typedRecord, final int initialCommandLimit) {
    final PreparedBatch preparedBatch;
    try {
      preparedBatch = prepareBatch(typedRecord, initialCommandLimit);
    } catch (final Throwable error) {
      failCommand(currentTransaction, typedRecord, error);
      return;
    }

    final long appendStartedAt = System.nanoTime();
    final ActorFuture<Void> appended = writers.awaitAppends();
    actor.runOnCompletion(
        appended,
        (ignored, error) -> {
          if (closed) {
            return;
          }
          if (error != null) {
            if (ExceededBatchRecordSizeException.causedBy(error)) {
              retryAfterAppendOverflow(typedRecord, preparedBatch, error);
            } else {
              failCommand(preparedBatch.transaction(), typedRecord, error);
            }
            return;
          }
          processingMetrics.onPhaseCompleted(
              ProcessingMetrics.Phase.RESULT_RAFT_COMMIT, System.nanoTime() - appendStartedAt);
          final long stateCommitStartedAt = System.nanoTime();
          try {
            preparedBatch.transaction().commit();
          } catch (final Throwable commitError) {
            failCommand(preparedBatch.transaction(), typedRecord, commitError);
            return;
          }
          processingMetrics.onPhaseCompleted(
              ProcessingMetrics.Phase.STATE_COMMIT, System.nanoTime() - stateCommitStartedAt);
          currentTransaction = null;
          lastSuccessfulProcessedRecordPosition = typedRecord.getPosition();
          appliedPositionConsumer.accept(typedRecord.getPosition());
          final long written = writers.highestWrittenPosition();
          if (written > lastWrittenPosition) {
            lastWrittenPosition = written;
          }
          processingMetrics.onProcessed(writers.writtenRecords());
          processingMetrics.onBatchProcessed(
              preparedBatch.stats().commandCount(),
              preparedBatch.stats().commandLimitContinuationCount(),
              preparedBatch.stats().recordLimitContinuationCount(),
              preparedBatch.stats().timeLimitContinuationCount(),
              preparedBatch.durationNanos());
          processingMetrics.onBatchResult(batchResultState());
          updateConsumedPosition(typedRecord.getPosition());
          deliverResponses();
          runPostCommitActions();
          inProcessing = false;
          typedRecordProcessors
              .lifecycleListeners()
              .forEach(listener -> listener.onProcessed(typedRecord));
          actor.run(this::tryToReadNextRecord);
        });
  }

  private PreparedBatch prepareBatch(
      final TypedRecordImpl rootCommand, final int initialCommandLimit) throws Exception {
    int commandLimit = initialCommandLimit;
    while (commandLimit > 0) {
      final DbTransaction transaction = storage.getCurrentTransaction();
      currentTransaction = transaction;
      processorContext.setProcessingTime(rootCommand.getTimestamp());
      writers.beginBatch(rootCommand);
      final long batchStartedAt = System.nanoTime();
      try {
        final ProcessingBatch.Result stats =
            processingBatch.process(rootCommand, commandLimit, maxRecordsInBatch, maxBatchDuration);
        storage.markAsProcessed(rootCommand.getPosition());
        return new PreparedBatch(transaction, stats, System.nanoTime() - batchStartedAt, false);
      } catch (final ProcessingBatch.Overflow overflow) {
        rollbackForRetry(transaction, overflow);
        processingMetrics.onBatchRetried();
        commandLimit = overflow.completedCommands();
      }
    }
    return prepareOversizedRootRejection(rootCommand);
  }

  private PreparedBatch prepareOversizedRootRejection(final TypedRecordImpl rootCommand) {
    final DbTransaction transaction = storage.getCurrentTransaction();
    currentTransaction = transaction;
    processorContext.setProcessingTime(rootCommand.getTimestamp());
    writers.beginBatch(rootCommand);
    final long batchStartedAt = System.nanoTime();
    writers.rejection().appendRejection(RejectionType.EXCEEDED_BATCH_RECORD_SIZE, "");
    writers
        .response()
        .writeRejectionOnCommand(rootCommand, RejectionType.EXCEEDED_BATCH_RECORD_SIZE, "");
    storage.markAsProcessed(rootCommand.getPosition());
    return new PreparedBatch(
        transaction,
        new ProcessingBatch.Result(1, 0, 0, 0),
        System.nanoTime() - batchStartedAt,
        true);
  }

  private void retryAfterAppendOverflow(
      final TypedRecordImpl rootCommand,
      final PreparedBatch preparedBatch,
      final Throwable overflow) {
    try {
      rollbackForRetry(preparedBatch.transaction(), overflow);
    } catch (final Throwable rollbackError) {
      failCommand(currentTransaction, rootCommand, rollbackError);
      return;
    }
    processingMetrics.onBatchRetried();
    if (preparedBatch.oversizedRootRejection()) {
      failCommand(null, rootCommand, overflow);
      return;
    }
    processCommand(rootCommand, Math.max(0, preparedBatch.stats().commandCount() - 1));
  }

  private void rollbackForRetry(final DbTransaction transaction, final Throwable overflow)
      throws Exception {
    try {
      transaction.rollback();
      if (currentTransaction == transaction) {
        currentTransaction = null;
      }
    } catch (final Exception rollbackError) {
      overflow.addSuppressed(rollbackError);
      throw new IllegalStateException("Failed to roll back oversized processing batch", overflow);
    }
  }

  private DirectWriters directWriters(final Writers writers) {
    if (writers instanceof DirectWriters directWriters) {
      return directWriters;
    }
    throw new IllegalStateException("Processing context must provide DirectWriters");
  }

  private void deliverResponses() {
    final long startedAt = System.nanoTime();
    for (final LoggedEvent response : writers.responses()) {
      try {
        commandResponseObserver.onResponse(response);
      } catch (final Throwable error) {
        LOG.error(
            "Failed to deliver committed command response {}",
            response.metadata().getRequestId(),
            error);
      }
    }
    processingMetrics.onPhaseCompleted(
        ProcessingMetrics.Phase.RESPONSE_DELIVERY, System.nanoTime() - startedAt);
  }

  private void runPostCommitActions() {
    final long startedAt = System.nanoTime();
    for (final Runnable action : writers.postCommitActions()) {
      try {
        action.run();
      } catch (final Throwable error) {
        LOG.error("Failed to execute post-commit action", error);
      }
    }
    processingMetrics.onPostCommitCompleted(System.nanoTime() - startedAt);
  }

  private ProcessingMetrics.BatchResultState batchResultState() {
    if (!writers.writtenRecords().isEmpty()) {
      return ProcessingMetrics.BatchResultState.RECORDS;
    }
    if (!writers.responses().isEmpty()) {
      return ProcessingMetrics.BatchResultState.RESPONSE_ONLY;
    }
    if (!writers.postCommitActions().isEmpty()) {
      return ProcessingMetrics.BatchResultState.POST_COMMIT_ONLY;
    }
    return ProcessingMetrics.BatchResultState.EMPTY;
  }

  private void failCommand(
      final DbTransaction transaction, final TypedRecordImpl typedRecord, final Throwable error) {
    LOG.error("Failed to process command {}", typedRecord.getRequestId(), error);
    rollbackQuietly(transaction);
    if (currentTransaction == transaction) {
      currentTransaction = null;
    }
    close();
    typedRecordProcessors.lifecycleListeners().forEach(listener -> listener.onClose());
    failureObserver.onFailure(typedRecord.getRequestId(), error);
    inProcessing = false;
  }

  void setCommitBoundaryTask(final Runnable task) {
    commitBoundaryTask = Objects.requireNonNull(task, "task");
  }

  void close() {
    closed = true;
    cancelReadVisibilityRetry();
    final DbTransaction transaction = currentTransaction;
    currentTransaction = null;
    inProcessing = false;
    rollbackQuietly(transaction);
  }

  private void scheduleReadVisibilityRetry() {
    if (readVisibilityRetry == null) {
      readVisibilityRetry = actor.schedule(READ_VISIBILITY_RETRY_DELAY, this::tryToReadNextRecord);
    }
  }

  private void cancelReadVisibilityRetry() {
    if (readVisibilityRetry != null) {
      readVisibilityRetry.cancel();
      readVisibilityRetry = null;
    }
  }

  long getLastSuccessfulProcessedRecordPosition() {
    return lastSuccessfulProcessedRecordPosition;
  }

  long getLastWrittenPosition() {
    return lastWrittenPosition;
  }

  private void updateConsumedPosition(final long position) {
    lastConsumedRecordPosition = Math.max(lastConsumedRecordPosition, position);
    processingMetrics.onRecordPositions(
        lastConsumedRecordPosition, committedPositionSupplier.getAsLong());
  }

  private void rollbackQuietly(final DbTransaction transaction) {
    if (transaction == null) {
      return;
    }
    try {
      transaction.rollback();
    } catch (final Exception rollbackError) {
      LOG.error("Failed to roll back apply transaction", rollbackError);
    }
  }

  private record PreparedBatch(
      DbTransaction transaction,
      ProcessingBatch.Result stats,
      long durationNanos,
      boolean oversizedRootRejection) {}
}
