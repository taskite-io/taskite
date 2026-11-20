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

import io.taskite.scheduler.Actor;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.state.processing.ProcessingStateStore;
import io.taskite.streamplatform.api.metrics.ProcessingMetrics;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContextFactory;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;
import io.taskite.streamplatform.api.record.CommandResponseObserver;
import io.taskite.streamplatform.api.record.CommittedRecordObserver;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.api.state.EventAppliers;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.command.CommandFailureObserver;
import io.taskite.streamplatform.command.DirectWriters;
import io.taskite.streamplatform.command.RecordAppender;
import io.taskite.streamplatform.logstream.LogStreamReader;
import io.taskite.streamplatform.state.PersistedKeyGenerator;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The actor-driven apply loop for one partition. An instance has a fixed {@link
 * StreamProcessorMode}: a leader (PROCESSING) recovers state by replaying committed events and then
 * processes commands, while a follower (REPLAY) continuously applies committed events without
 * executing commands. A leadership change replaces the processor instance instead of mutating its
 * role in place.
 *
 * <p>Lifecycle: {@code onActorStarted → recoverFromSnapshot → replayStateMachine.startRecover →
 * (leader) onRecovered → processingStateMachine.startProcessing}. New committed records wake the
 * active state machine through {@link #onRecordAvailable()}.
 */
public final class StreamProcessor extends Actor {
  private static final Logger LOG = LoggerFactory.getLogger(StreamProcessor.class);

  private final int partitionId;
  private final StreamProcessorMode mode;
  private final TypedEventRegistry eventRegistry;
  private final KeyGenerator keyGenerator;
  private final TypedRecordProcessorContext processorContext;
  private final TypedRecordProcessors typedRecordProcessors;
  private final ProcessingStateStore storage;
  private final LongSupplier committedPositionSupplier;
  private final LongConsumer appliedPositionConsumer;
  private final LogStreamReader reader;
  private final RecordAppender appender;
  private final CommandFailureObserver failureObserver;
  private final CommandResponseObserver commandResponseObserver;
  private final CommittedRecordObserver committedRecordObserver;
  private final ProcessingMetrics processingMetrics;
  private final String actorName;
  private final int maxCommandsInBatch;
  private final int maxRecordsInBatch;
  private final Duration maxBatchDuration;
  private final int maxBatchBytes;

  private volatile ReplayStateMachine replayStateMachine;
  private ProcessingStateMachine processingStateMachine;
  private ScheduledCommandExecutor scheduledCommands;
  private volatile boolean closed;
  private volatile boolean recovered;

  public StreamProcessor(final StreamProcessorContext context) {
    partitionId = context.partitionId();
    mode = Objects.requireNonNull(context.mode(), "mode");
    eventRegistry = Objects.requireNonNull(context.eventRegistry(), "eventRegistry");
    storage = Objects.requireNonNull(context.storage(), "storage");
    committedPositionSupplier =
        Objects.requireNonNull(context.committedPositionSupplier(), "committedPositionSupplier");
    appliedPositionConsumer =
        Objects.requireNonNull(context.appliedPositionConsumer(), "appliedPositionConsumer");
    reader = Objects.requireNonNull(context.reader(), "reader");
    appender = Objects.requireNonNull(context.appender(), "appender");
    failureObserver = Objects.requireNonNull(context.failureObserver(), "failureObserver");
    commandResponseObserver =
        Objects.requireNonNull(context.commandResponseObserver(), "commandResponseObserver");
    committedRecordObserver =
        Objects.requireNonNull(context.committedRecordObserver(), "committedRecordObserver");
    actorName = Objects.requireNonNull(context.actorName(), "actorName");
    maxCommandsInBatch = context.maxCommandsInBatch();
    maxRecordsInBatch = context.maxRecordsInBatch();
    maxBatchDuration = context.maxBatchDuration();
    maxBatchBytes = context.maxBatchBytes();

    final TypedRecordProcessorContextFactory processorContextFactory =
        Objects.requireNonNull(context.processorContextFactory(), "processorContextFactory");
    final Function<TypedRecordProcessorContext, TypedRecordProcessors>
        typedRecordProcessorsFactory =
            Objects.requireNonNull(
                context.typedRecordProcessorsFactory(), "typedRecordProcessorsFactory");
    keyGenerator = new PersistedKeyGenerator(context.clusterKey(), partitionId, storage);
    processorContext =
        processorContextFactory.create(
            partitionId, storage, keyGenerator, this::createCommandWriters);
    typedRecordProcessors =
        Objects.requireNonNull(
            typedRecordProcessorsFactory.apply(processorContext), "typedRecordProcessors");
    processingMetrics =
        Objects.requireNonNull(
            context.processingMetricsFactory().create(partitionId, processorContext),
            "processingMetrics");
  }

  @Override
  public String getName() {
    return actorName;
  }

  public int partitionId() {
    return partitionId;
  }

  public StreamProcessorMode mode() {
    return mode;
  }

  public boolean isRecovered() {
    final var replay = replayStateMachine;
    return !closed
        && (recovered
            || mode == StreamProcessorMode.REPLAY && replay != null && replay.hasCaughtUp());
  }

  private StreamStateMachineContext stateMachineContext() {
    return new StreamStateMachineContext(
        new StreamStateMachineContext.Identity(partitionId, mode),
        new StreamStateMachineContext.Processing(eventRegistry, processorContext),
        new StreamProcessorContext.State(
            storage, committedPositionSupplier, appliedPositionConsumer),
        new StreamStateMachineContext.Runtime(this, reader),
        new StreamStateMachineContext.Observers(
            this::onProcessingFailure,
            commandResponseObserver,
            committedRecordObserver,
            processingMetrics));
  }

  @Override
  protected void onActorStarted() {
    final long snapshotPosition = storage.getLastProcessedPosition();
    appliedPositionConsumer.accept(snapshotPosition);
    replayStateMachine = new ReplayStateMachine(stateMachineContext());
    final ActorFuture<Long> replayCompleted = replayStateMachine.startRecover(snapshotPosition);
    if (mode == StreamProcessorMode.PROCESSING) {
      runOnCompletion(
          replayCompleted,
          (lastProcessedPosition, error) -> {
            if (closed) {
              return;
            }
            if (error != null) {
              LOG.error("Replay recovery failed for partition {}", partitionId, error);
              return;
            }
            onRecovered(lastProcessedPosition);
          });
    } else {
      runOnCompletion(
          replayCompleted,
          (ignored, error) -> {
            if (error != null) {
              LOG.error("Replay failed for partition {}", partitionId, error);
            }
          });
    }
  }

  private void onRecovered(final long lastProcessedPosition) {
    if (closed) {
      return;
    }
    final var batching =
        new StreamProcessorContext.Batching(
            maxCommandsInBatch, maxRecordsInBatch, maxBatchDuration, maxBatchBytes);
    processingStateMachine =
        new ProcessingStateMachine(stateMachineContext(), typedRecordProcessors, batching);
    scheduledCommands =
        new ScheduledCommandExecutor(
            this,
            storage,
            eventRegistry,
            appender,
            batching,
            processingStateMachine::getLastSuccessfulProcessedRecordPosition,
            processingStateMachine::tryToReadNextRecord);
    processingStateMachine.setCommitBoundaryTask(scheduledCommands::runAtCommitBoundary);
    final var transaction = storage.getCurrentTransaction();
    try {
      processingMetrics.onRecovered();
      typedRecordProcessors
          .lifecycleListeners()
          .forEach(listener -> listener.onRecovered(scheduledCommands));
      transaction.commit();
    } catch (final Throwable error) {
      try {
        transaction.rollback();
      } catch (final Exception rollbackError) {
        error.addSuppressed(rollbackError);
      }
      onProcessingFailure(null, error);
      return;
    }
    processingStateMachine.startProcessing(lastProcessedPosition);
    recovered = true;
  }

  private DirectWriters createCommandWriters(final EventAppliers eventAppliers) {
    return new DirectWriters(
        appender, failureObserver, eventAppliers, keyGenerator, maxBatchBytes, eventRegistry, this);
  }

  /** Wakes the active state machine when new records are committed to the log. */
  public void onRecordAvailable() {
    run(
        () -> {
          if (closed) {
            return;
          }
          if (processingStateMachine != null) {
            processingStateMachine.tryToReadNextRecord();
          } else if (replayStateMachine != null) {
            replayStateMachine.replayNextEvent();
          }
        });
  }

  /** The last processed source/command position; feeds the snapshot director's lower bound. */
  public ActorFuture<Long> getLastProcessedPositionAsync() {
    return call(
        () -> {
          if (processingStateMachine != null) {
            return processingStateMachine.getLastSuccessfulProcessedRecordPosition();
          }
          if (replayStateMachine != null) {
            return replayStateMachine.getLastSourceEventPosition();
          }
          return ReplayStateMachine.UNSET_POSITION;
        });
  }

  /** The last written follow-up position; feeds the snapshot director's commit-wait. */
  public ActorFuture<Long> getLastWrittenPositionAsync() {
    return call(
        () -> {
          if (processingStateMachine != null) {
            return processingStateMachine.getLastWrittenPosition();
          }
          if (replayStateMachine != null) {
            return replayStateMachine.getLastReplayedEventPosition();
          }
          return ReplayStateMachine.UNSET_POSITION;
        });
  }

  @Override
  protected void onActorClosing() {
    if (closed) {
      return;
    }
    closed = true;
    typedRecordProcessors.lifecycleListeners().forEach(listener -> listener.onClose());
    if (scheduledCommands != null) {
      scheduledCommands.close();
    }
    if (replayStateMachine != null) {
      replayStateMachine.close();
    }
    if (processingStateMachine != null) {
      processingStateMachine.close();
    }
    processingMetrics.close();
    reader.close();
  }

  private void onProcessingFailure(final String requestId, final Throwable error) {
    onActorClosing();
    failureObserver.onFailure(requestId, error);
  }

  @Override
  protected void handleFailure(final Throwable failure) {
    super.handleFailure(failure);
    onProcessingFailure(null, failure);
  }

  @Override
  public void onActorFailed() {
    onActorClosing();
  }

  @Override
  protected void onActorClosed() {
    onActorClosing();
  }
}
