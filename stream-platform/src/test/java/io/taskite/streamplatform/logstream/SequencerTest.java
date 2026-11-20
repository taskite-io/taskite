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
package io.taskite.streamplatform.logstream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.streamplatform.api.exception.ExceededBatchRecordSizeException;
import io.taskite.streamplatform.api.metrics.LogAppendMetrics;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

final class SequencerTest {

  @Test
  void recordsTheFinalFramedBatchShape() {
    final TestLogStorage storage = new TestLogStorage();
    final RecordingLogAppendMetrics metrics = new RecordingLogAppendMetrics();
    final Sequencer sequencer = new Sequencer(storage, 0, metrics);

    final var result = sequencer.tryWrite(List.of(jobEntry(), jobEntry()), -1);

    assertTrue(result.isRight());
    assertEquals(2, metrics.recordCount);
    assertEquals(storage.batchPayload.length, metrics.payloadBytes);
    assertEquals(
        LogBatchSizing.payloadBytes(List.of(jobEntry(), jobEntry())), metrics.payloadBytes);
  }

  @Test
  void rejectsBatchBeforeAssigningPositionsWhenFinalPayloadExceedsLimit() {
    final TestLogStorage storage = new TestLogStorage();
    final List<LogAppendEntry> entries = List.of(jobEntry());
    final int requiredBytes = Math.toIntExact(LogBatchSizing.payloadBytes(entries));
    final Sequencer sequencer = new Sequencer(storage, 0, LogAppendMetrics.NOOP, requiredBytes - 1);

    final var error =
        assertThrows(ExceededBatchRecordSizeException.class, () -> sequencer.tryWrite(entries, -1));

    assertEquals(requiredBytes, error.actualBytes());
    assertEquals(requiredBytes - 1, error.maxBytes());
    assertNull(storage.batchPayload);
    assertEquals(0, storage.highestPosition);
  }

  @Test
  void metricsFailureDoesNotChangeAnAcceptedAppend() {
    final TestLogStorage storage = new TestLogStorage();
    final Sequencer sequencer =
        new Sequencer(
            storage,
            0,
            new LogAppendMetrics() {
              @Override
              public void onBatchAppended(final int recordCount, final int payloadBytes) {
                throw new IllegalStateException("metric failure");
              }
            });

    final var result = sequencer.tryWrite(List.of(jobEntry()), -1);

    assertTrue(result.isRight());
    assertEquals(1, result.get());
    assertEquals(1, storage.highestPosition);
  }

  @Test
  void reconcilesCommittedPositionRecoveredAfterConstruction() {
    final TestLogStorage storage = new TestLogStorage();
    final Sequencer sequencer = new Sequencer(storage, storage.getCommittedPosition());
    storage.committedPosition = 10;

    final var result = sequencer.tryWrite(List.of(jobEntry()), -1);

    assertTrue(result.isRight());
    assertEquals(11, result.get());
    assertEquals(11, storage.lowestPosition);
    assertEquals(11, storage.highestPosition);
  }

  @Test
  void assignsContinuousPositionsAcrossConcurrentWrites() {
    final TestLogStorage storage = new TestLogStorage();
    final Sequencer sequencer = new Sequencer(storage, 0);

    final List<Long> positions =
        IntStream.range(0, 100)
            .parallel()
            .mapToObj(ignored -> sequencer.tryWrite(List.of(jobEntry()), -1).get())
            .sorted(Comparator.naturalOrder())
            .toList();

    assertEquals(IntStream.rangeClosed(1, 100).mapToObj(value -> (long) value).toList(), positions);
    assertEquals(positions, new ArrayList<>(storage.appendedPositions).stream().sorted().toList());
  }

  private static LogAppendEntry jobEntry() {
    return LogAppendEntry.of(
        new RecordMetadata()
            .recordType(RecordType.COMMAND)
            .valueType(ValueType.JOB)
            .intent(JobIntent.COMPLETE),
        new JobRecord().setJobType("type").setWorker("worker").setTenantId("tenant"));
  }

  private static final class TestLogStorage implements LogStorage {
    private final ConcurrentLinkedQueue<Long> appendedPositions = new ConcurrentLinkedQueue<>();
    private long committedPosition;
    private long lowestPosition;
    private long highestPosition;
    private byte[] batchPayload;

    @Override
    public LogStreamReader newReader() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void append(
        final long lowestPosition,
        final long highestPosition,
        final byte[] batchPayload,
        final AppendListener listener) {
      this.lowestPosition = lowestPosition;
      this.highestPosition = highestPosition;
      this.batchPayload = batchPayload;
      appendedPositions.add(highestPosition);
    }

    @Override
    public void addCommitListener(final CommitListener listener) {}

    @Override
    public void removeCommitListener(final CommitListener listener) {}

    @Override
    public long getCommittedPosition() {
      return committedPosition;
    }
  }

  private static final class RecordingLogAppendMetrics implements LogAppendMetrics {
    private int recordCount;
    private int payloadBytes;

    @Override
    public void onBatchAppended(final int recordCount, final int payloadBytes) {
      this.recordCount = recordCount;
      this.payloadBytes = payloadBytes;
    }
  }
}
