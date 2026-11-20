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
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.state.processing.ProcessingStateStore;
import io.taskite.streamplatform.api.metrics.ProcessingMetrics;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.record.CommandResponseObserver;
import io.taskite.streamplatform.api.record.CommittedRecordObserver;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.api.state.EventAppliers;
import io.taskite.streamplatform.command.CommandFailureObserver;
import io.taskite.streamplatform.logstream.LogStreamReader;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.streamplatform.record.TypedRecordImpl;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rebuilds local state from committed records. Replicated commands are not executed in this mode;
 * only {@code EVENT}s mutate state. Replaying an event or rejection advances the durable processed
 * position to the record's source position.
 *
 * <p>Self-driving: {@link #startRecover(long)} seeks the reader and kicks {@link
 * #replayNextEvent()}, which applies one source group per actor turn and re-schedules itself via
 * {@code actor.run}. A leader (PROCESSING mode) replays up to the committed position once and
 * completes {@link #recoveryFuture} with the last processed position, handing off to processing. A
 * follower (REPLAY mode) never completes: it drains what is committed, then waits for {@link
 * #replayNextEvent()} to be poked again when new records commit.
 *
 * <p>All committed result records carrying the same source position are replayed and committed in
 * one transaction. The durable processed position therefore never advances across only part of a
 * command result. Whether a group still needs applying is decided against the fixed {@code
 * replayBaseline} captured at recovery start.
 */
final class ReplayStateMachine {
  private static final Logger LOG = LoggerFactory.getLogger(ReplayStateMachine.class);
  static final long UNSET_POSITION = -1L;
  private static final Duration READ_VISIBILITY_RETRY_DELAY = Duration.ofMillis(1);

  private final int partitionId;
  private final TypedEventRegistry eventRegistry;
  private final ConcurrencyControl actor;
  private final LogStreamReader reader;
  private final ProcessingStateStore storage;
  private final LongSupplier committedPositionSupplier;
  private final LongConsumer appliedPositionConsumer;
  private final StreamProcessorMode mode;
  private final TypedRecordProcessorContext processorContext;
  private final CommandFailureObserver failureObserver;
  private final CommandResponseObserver commandResponseObserver;
  private final CommittedRecordObserver committedRecordObserver;
  private final ProcessingMetrics processingMetrics;

  private CompletableActorFuture<Long> recoveryFuture;
  // The durable processed position captured at recovery start; events are deduplicated against this
  // fixed baseline rather than the live position (see class javadoc).
  private long replayBaseline;
  // The last source (command) position seen; becomes the processing start position after recovery.
  private long lastSourceEventPosition = UNSET_POSITION;
  // The last event position actually replayed into state.
  private long lastReplayedEventPosition = UNSET_POSITION;
  private long lastConsumedRecordPosition = UNSET_POSITION;
  private LoggedEvent pendingRecord;
  private ScheduledTimer readVisibilityRetry;
  private boolean closed;
  private volatile boolean caughtUp;

  ReplayStateMachine(final StreamStateMachineContext context) {
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
    mode = Objects.requireNonNull(context.identity().mode(), "mode");
    processorContext =
        Objects.requireNonNull(context.processing().processorContext(), "processorContext");
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
  }

  /**
   * Starts replay recovery from {@code snapshotPosition}. Returns a future completed (PROCESSING
   * mode) with the last processed position once the committed log is drained, so the caller can
   * hand off to processing. In REPLAY mode the future never completes normally (a follower replays
   * forever); it only completes exceptionally on failure.
   */
  ActorFuture<Long> startRecover(final long snapshotPosition) {
    recoveryFuture = new CompletableActorFuture<>();
    replayBaseline = storage.getLastProcessedPosition();
    lastSourceEventPosition = snapshotPosition > 0 ? snapshotPosition : UNSET_POSITION;
    lastConsumedRecordPosition = snapshotPosition > 0 ? snapshotPosition : UNSET_POSITION;
    reader.seekToNextEvent(snapshotPosition);
    // A checkpoint can cover result records beyond the durable source-command cursor. Those
    // positions may have no remaining journal entry after compaction, even when the log is empty.
    final long checkpointPosition = reader.snapshotPosition();
    if (checkpointPosition > 0L) {
      lastConsumedRecordPosition = Math.max(lastConsumedRecordPosition, checkpointPosition);
      appliedPositionConsumer.accept(lastConsumedRecordPosition);
    }
    processingMetrics.onRecordPositions(
        lastConsumedRecordPosition, committedPositionSupplier.getAsLong());
    replayNextEvent();
    return recoveryFuture;
  }

  /** Applies the next committed record and re-schedules itself, or ends the pass. */
  void replayNextEvent() {
    if (closed || (mode == StreamProcessorMode.PROCESSING && recoveryFuture.isDone())) {
      return;
    }
    try {
      cancelReadVisibilityRetry();
      final long committedPosition = committedPositionSupplier.getAsLong();
      processingMetrics.onRecordPositions(lastConsumedRecordPosition, committedPosition);
      if (nextRecordPosition() > committedPosition) {
        onReplayCaughtUp();
        return;
      }
      if (pendingRecord == null && !reader.hasNext()) {
        scheduleReadVisibilityRetry();
        return;
      }
      final LoggedEvent entry = takeNextRecord();
      if (entry == null) {
        onReplayCaughtUp();
        return;
      }
      final long consumedPosition = applyRecordGroup(entry, committedPosition);
      if (consumedPosition == UNSET_POSITION) {
        return;
      }
      lastConsumedRecordPosition = Math.max(lastConsumedRecordPosition, consumedPosition);
      processingMetrics.onRecordPositions(
          lastConsumedRecordPosition, committedPositionSupplier.getAsLong());
      actor.run(this::replayNextEvent);
    } catch (final Throwable error) {
      completeRecoveryExceptionally(error);
    }
  }

  private void onReplayCaughtUp() {
    caughtUp = true;
    if (mode == StreamProcessorMode.PROCESSING && !recoveryFuture.isDone()) {
      recoveryFuture.complete(storage.getLastProcessedPosition());
    }
    // REPLAY mode (follower): stop and wait; StreamProcessor.onRecordAvailable pokes us again.
  }

  boolean hasCaughtUp() {
    return caughtUp;
  }

  private void scheduleReadVisibilityRetry() {
    if (readVisibilityRetry == null) {
      readVisibilityRetry = actor.schedule(READ_VISIBILITY_RETRY_DELAY, this::replayNextEvent);
    }
  }

  private void cancelReadVisibilityRetry() {
    if (readVisibilityRetry != null) {
      readVisibilityRetry.cancel();
      readVisibilityRetry = null;
    }
  }

  private long nextRecordPosition() {
    return pendingRecord == null ? reader.getNextPosition() : pendingRecord.getPosition();
  }

  private LoggedEvent takeNextRecord() {
    if (pendingRecord == null) {
      return reader.next();
    }
    final LoggedEvent next = pendingRecord;
    pendingRecord = null;
    return next;
  }

  private long applyRecordGroup(final LoggedEvent entry, final long committedPosition) {
    final ReplayRecord first = readRecord(entry);
    if (!isResultRecord(first.recordType())) {
      return entry.getPosition();
    }

    final long processedPosition = processedPosition(first.typedRecord());
    if (processedPosition <= replayBaseline) {
      appliedPositionConsumer.accept(entry.getPosition());
      notifyCommitted(first);
      return entry.getPosition();
    }

    final DbTransaction transaction = storage.getCurrentTransaction();
    final List<LoggedEvent> committedRecords = new ArrayList<>();
    long consumedPosition = entry.getPosition();
    long replayedEventPosition = UNSET_POSITION;
    try {
      replayedEventPosition = applyRecord(first, committedRecords, replayedEventPosition);
      while (reader.getNextPosition() <= committedPosition) {
        if (!reader.hasNext()) {
          return retryRecordGroup(transaction, entry.getPosition());
        }
        final LoggedEvent nextEntry = reader.next();
        if (nextEntry == null) {
          return retryRecordGroup(transaction, entry.getPosition());
        }
        final ReplayRecord next = readRecord(nextEntry);
        if (processedPosition(next.typedRecord()) != processedPosition) {
          pendingRecord = nextEntry;
          break;
        }
        consumedPosition = nextEntry.getPosition();
        if (isResultRecord(next.recordType())) {
          replayedEventPosition = applyRecord(next, committedRecords, replayedEventPosition);
        }
      }
      storage.markAsProcessed(processedPosition);
      transaction.commit();
    } catch (final Throwable error) {
      rollbackQuietly(transaction);
      throw new IllegalStateException("Failed to replay record group", error);
    }

    lastSourceEventPosition = Math.max(lastSourceEventPosition, processedPosition);
    lastReplayedEventPosition = Math.max(lastReplayedEventPosition, replayedEventPosition);
    // Readiness follows committed record consumption; the durable cursor still deduplicates
    // sources.
    appliedPositionConsumer.accept(consumedPosition);
    committedRecords.forEach(this::notifyCommitted);
    return consumedPosition;
  }

  private ReplayRecord readRecord(final LoggedEvent entry) {
    final RecordMetadata metadata = entry.readMetadata(new RecordMetadata());
    final UnifiedRecordValue value = eventRegistry.read(entry, metadata);
    final TypedRecordImpl typedRecord = new TypedRecordImpl(partitionId);
    typedRecord.wrap(entry, metadata, value);
    return new ReplayRecord(typedRecord, metadata, value);
  }

  private long applyRecord(
      final ReplayRecord record,
      final List<LoggedEvent> committedRecords,
      final long replayedEventPosition) {
    long latestReplayedEventPosition = replayedEventPosition;
    if (record.recordType() == RecordType.EVENT) {
      final TypedRecordImpl typedRecord = record.typedRecord();
      // Recover the key-generator high-water before applying state so promotion cannot re-issue a
      // key already present in the committed log.
      processorContext.getKeyGenerator().setKeyIfHigher(typedRecord.getKey());
      applyEvent(typedRecord);
      latestReplayedEventPosition =
          Math.max(latestReplayedEventPosition, typedRecord.getPosition());
    }
    committedRecords.add(
        LoggedEvent.ofRecord(record.typedRecord().getKey(), record.metadata(), record.value()));
    return latestReplayedEventPosition;
  }

  private long retryRecordGroup(final DbTransaction transaction, final long groupStartPosition) {
    rollbackQuietly(transaction);
    try {
      pendingRecord = null;
      reader.seek(groupStartPosition);
    } catch (final Throwable error) {
      throw new IllegalStateException("Failed to rewind incomplete replay group", error);
    }
    scheduleReadVisibilityRetry();
    return UNSET_POSITION;
  }

  private boolean isResultRecord(final RecordType recordType) {
    return recordType == RecordType.EVENT || recordType == RecordType.COMMAND_REJECTION;
  }

  private void notifyCommitted(final ReplayRecord record) {
    notifyCommitted(
        LoggedEvent.ofRecord(record.typedRecord().getKey(), record.metadata(), record.value()));
  }

  private void notifyCommitted(final LoggedEvent committedRecord) {
    deliverResponse(committedRecord);
    committedRecordObserver.onProcessed(committedRecord);
  }

  private void deliverResponse(final LoggedEvent response) {
    try {
      commandResponseObserver.onResponse(response);
    } catch (final Throwable error) {
      LOG.error(
          "Failed to deliver replayed command response {}",
          response.metadata().getRequestId(),
          error);
    }
  }

  private void applyEvent(final TypedRecordImpl typedRecord) {
    final EventAppliers eventAppliers = processorContext.getEventAppliers();
    final int recordVersion = typedRecord.getRecordVersion();
    if (!eventAppliers.hasApplierFor(typedRecord.getIntent(), recordVersion)) {
      return;
    }
    eventAppliers.applyState(
        typedRecord.getKey(), typedRecord.getIntent(), typedRecord.getValue(), recordVersion);
  }

  private long processedPosition(final TypedRecordImpl typedRecord) {
    final long sourcePosition = typedRecord.getSourceEventPosition();
    return sourcePosition > 0 ? sourcePosition : typedRecord.getPosition();
  }

  long getLastSourceEventPosition() {
    return lastSourceEventPosition;
  }

  long getLastReplayedEventPosition() {
    return lastReplayedEventPosition;
  }

  private void completeRecoveryExceptionally(final Throwable error) {
    closed = true;
    cancelReadVisibilityRetry();
    LOG.error("Replay failed for partition {}", partitionId, error);
    failureObserver.onFailure(null, error);
    if (recoveryFuture != null && !recoveryFuture.isDone()) {
      recoveryFuture.completeExceptionally(error);
    }
  }

  void close() {
    closed = true;
    cancelReadVisibilityRetry();
    if (recoveryFuture != null && !recoveryFuture.isDone()) {
      recoveryFuture.completeExceptionally(
          new IllegalStateException("Processor closed during replay"));
    }
  }

  private void rollbackQuietly(final DbTransaction transaction) {
    if (transaction == null) {
      return;
    }
    try {
      transaction.rollback();
    } catch (final Exception rollbackError) {
      LOG.error("Failed to roll back replay transaction", rollbackError);
    }
  }

  private record ReplayRecord(
      TypedRecordImpl typedRecord, RecordMetadata metadata, UnifiedRecordValue value) {
    private RecordType recordType() {
      return metadata.getRecordType();
    }
  }
}
