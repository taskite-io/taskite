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

import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.streamplatform.api.exception.ExceededBatchRecordSizeException;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.command.DirectWriters;
import io.taskite.streamplatform.logstream.LogAppendEntry;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Executes one root command and its admitted follow-ups through a bounded FIFO. */
final class ProcessingBatch {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessingBatch.class);

  private final int partitionId;
  private final TypedEventRegistry eventRegistry;
  private final TypedRecordProcessors processors;
  private final DirectWriters writers;
  private final LongSupplier nanoTime;

  ProcessingBatch(
      final int partitionId,
      final TypedEventRegistry eventRegistry,
      final TypedRecordProcessors processors,
      final DirectWriters writers) {
    this(partitionId, eventRegistry, processors, writers, System::nanoTime);
  }

  ProcessingBatch(
      final int partitionId,
      final TypedEventRegistry eventRegistry,
      final TypedRecordProcessors processors,
      final DirectWriters writers,
      final LongSupplier nanoTime) {
    this.partitionId = partitionId;
    this.eventRegistry = Objects.requireNonNull(eventRegistry, "eventRegistry");
    this.processors = Objects.requireNonNull(processors, "processors");
    this.writers = Objects.requireNonNull(writers, "writers");
    this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
  }

  Result process(final TypedRecord<?> rootCommand, final int commandLimit) {
    return process(rootCommand, commandLimit, 0, Duration.ZERO);
  }

  Result process(
      final TypedRecord<?> rootCommand,
      final int commandLimit,
      final int maxRecords,
      final Duration maxDuration) {
    if (maxRecords < 0) {
      throw new IllegalArgumentException("maxRecords must not be negative");
    }
    Objects.requireNonNull(maxDuration, "maxDuration");
    if (maxDuration.isNegative()) {
      throw new IllegalArgumentException("maxDuration must not be negative");
    }
    final ArrayDeque<TypedRecord<?>> pendingCommands = new ArrayDeque<>();
    pendingCommands.addLast(rootCommand);
    final long startedAt = nanoTime.getAsLong();
    final long maxDurationNanos = maxDuration.isZero() ? 0 : maxDuration.toNanos();
    int processedCommands = 0;
    int commandLimitContinuations = 0;
    int recordLimitContinuations = 0;
    int timeLimitContinuations = 0;

    while (!pendingCommands.isEmpty()) {
      if (processedCommands >= commandLimit) {
        throw new IllegalStateException(
            "Batch command limit reached with admitted commands still pending");
      }
      final TypedRecord<?> command = pendingCommands.removeFirst();
      writers.beginCommand(command);
      final TypedRecordProcessor<?> processor = resolveProcessor(command);
      if (processor == null) {
        throw new IllegalStateException(
            "No command processor registered for "
                + command.getRecordType()
                + '/'
                + command.getValueType()
                + '/'
                + command.getIntent());
      }

      final int firstNewRecord = writers.recordCount();
      try {
        processTypedRecord(processor, command);
      } catch (final ExceededBatchRecordSizeException overflow) {
        throw new Overflow(processedCommands, overflow);
      }
      processedCommands++;

      final boolean recordBudgetAvailable = maxRecords == 0 || writers.recordCount() < maxRecords;
      final boolean timeBudgetAvailable =
          maxDurationNanos == 0 || nanoTime.getAsLong() - startedAt < maxDurationNanos;
      int remainingCapacity = commandLimit - processedCommands - pendingCommands.size();
      for (int index = firstNewRecord; index < writers.recordCount(); index++) {
        final LogAppendEntry record = writers.recordAt(index);
        if (record.recordMetadata().getRecordType() != RecordType.COMMAND) {
          continue;
        }
        if (remainingCapacity <= 0) {
          commandLimitContinuations++;
        } else if (!recordBudgetAvailable) {
          recordLimitContinuations++;
        } else if (!timeBudgetAvailable) {
          timeLimitContinuations++;
        } else {
          pendingCommands.addLast(new UnwrittenTypedRecord(partitionId, record, eventRegistry));
          writers.markProcessed(index);
          remainingCapacity--;
        }
      }
    }
    return new Result(
        processedCommands,
        commandLimitContinuations,
        recordLimitContinuations,
        timeLimitContinuations);
  }

  private TypedRecordProcessor<?> resolveProcessor(final TypedRecord<?> record) {
    final ValueType valueType = record.getValueType();
    final Intent intent = record.getIntent();
    final int recordVersion = record.getRecordVersion();
    final TypedRecordProcessor<?> exact = processors.get(valueType, intent, recordVersion);
    if (exact != null) {
      return exact;
    }

    final int latestVersion = processors.getLatestVersion(valueType, intent);
    if (latestVersion < 0) {
      return null;
    }
    LOG.warn(
        "No command processor for {}/{} version {}; falling back to latest registered version {}",
        valueType,
        intent,
        recordVersion,
        latestVersion);
    return processors.get(valueType, intent, latestVersion);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void processTypedRecord(
      final TypedRecordProcessor processor, final TypedRecord<?> record) {
    processor.processRecord(record);
  }

  record Result(
      int commandCount,
      int commandLimitContinuationCount,
      int recordLimitContinuationCount,
      int timeLimitContinuationCount) {
    int continuationCount() {
      return commandLimitContinuationCount
          + recordLimitContinuationCount
          + timeLimitContinuationCount;
    }
  }

  static final class Overflow extends RuntimeException {
    private final int completedCommands;

    private Overflow(final int completedCommands, final ExceededBatchRecordSizeException cause) {
      super(cause);
      this.completedCommands = completedCommands;
    }

    int completedCommands() {
      return completedCommands;
    }
  }
}
