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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.cluster.raft.log.PersistedRaftRecord;
import io.taskite.cluster.raft.log.RaftJournalReader;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies the position-based reader expands batch-carrying raft entries into individual {@link
 * LoggedEvent}s, one position at a time, and resumes correctly after a processed position.
 */
class RaftLogStreamReaderTest {

  // Two raft entries: index 1 carries positions [1..3] (a 3-record batch), index 2 carries
  // position [4] (a single-record batch). Each framed entry's key encodes its position for asserts.
  private List<PersistedRaftRecord> batchedLog() {
    final byte[] batchA = BatchPayloadCodec.encode(List.of(entry(1), entry(2), entry(3)));
    final byte[] batchB = BatchPayloadCodec.encode(List.of(entry(4)));
    return List.of(
        new PersistedRaftRecord(1, 1, 1, 3, batchA), new PersistedRaftRecord(1, 2, 4, 4, batchB));
  }

  /** A framed log entry whose key is {@code position}, so reads can be asserted by key. */
  private static byte[] entry(final long position) {
    final RecordMetadata metadata =
        new RecordMetadata()
            .recordType(RecordType.EVENT)
            .valueType(ValueType.JOB)
            .intent(JobIntent.ACTIVATED);
    final JobRecord value = new JobRecord().setJobType("t").setWorker("w").setTenantId("x");
    final LogAppendEntry appendEntry = LogAppendEntry.of(position, metadata, value);
    final byte[] bytes = new byte[LogEntryDescriptor.framedLength(appendEntry)];
    LogEntryDescriptor.serialize(new UnsafeBuffer(bytes), 0, appendEntry, position, -1, 0);
    return bytes;
  }

  @Test
  void expandsBatchEntriesOnePositionAtATime() {
    final RaftLogStreamReader reader =
        new RaftLogStreamReader(() -> new FakeJournalReader(batchedLog()));
    reader.seekToNextEvent(-1); // from the start

    long expectedPosition = 1;
    while (reader.hasNext()) {
      assertEquals(expectedPosition, reader.getNextPosition());
      final LoggedEvent event = reader.next();
      assertEquals(expectedPosition, event.getPosition());
      assertEquals(expectedPosition, event.getKey());
      expectedPosition++;
    }

    assertEquals(5, reader.getNextPosition());
    reader.close();
  }

  @Test
  void resumesAfterAProcessedPositionMidBatch() {
    final RaftLogStreamReader reader =
        new RaftLogStreamReader(() -> new FakeJournalReader(batchedLog()));
    // Resume after position 2: the next record returned must be position 3 (still inside batch A).
    reader.seekToNextEvent(2);

    assertTrue(reader.hasNext());
    assertEquals(3, reader.getNextPosition());
    assertEquals(3, reader.next().getKey());
    assertEquals(4, reader.next().getKey());
    assertFalse(reader.hasNext());
    reader.close();
  }

  @Test
  void resumesAtFirstAvailablePositionAfterSnapshotCompaction() {
    final byte[] compactedBatch =
        BatchPayloadCodec.encode(List.of(entry(10), entry(11), entry(12)));
    final RaftLogStreamReader reader =
        new RaftLogStreamReader(
            () ->
                new FakeJournalReader(
                    List.of(new PersistedRaftRecord(1, 5, 10, 12, compactedBatch))));

    reader.seekToNextEvent(4);

    assertTrue(reader.hasNext());
    assertEquals(10, reader.getNextPosition());
    assertEquals(10, reader.next().getKey());
    assertEquals(11, reader.next().getKey());
    assertEquals(12, reader.next().getKey());
    assertFalse(reader.hasNext());
    reader.close();
  }

  @Test
  void reportsNoRecordWhenCommittedJournalIsEmpty() {
    final RaftLogStreamReader reader =
        new RaftLogStreamReader(() -> new FakeJournalReader(List.of()));

    reader.seekToNextEvent(-1);

    assertFalse(reader.hasNext());
    assertEquals(1, reader.getNextPosition());
    reader.close();
  }

  @Test
  void emptyCompactedJournalResumesAfterCheckpointHighWater() {
    try (final var reader =
        new RaftLogStreamReader(() -> new FakeJournalReader(List.of()), () -> 12L)) {
      reader.seekToNextEvent(4);

      assertFalse(reader.hasNext());
      assertEquals(12L, reader.snapshotPosition());
      assertEquals(13L, reader.getNextPosition());
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {0, 2, 4})
  void checkpointFloorPreservesEveryRecordAfterTheSnapshot(final long snapshotPosition) {
    try (final var reader =
        new RaftLogStreamReader(
            () -> new FakeJournalReader(batchedLog()), () -> snapshotPosition)) {
      reader.seekToNextEvent(0);

      for (long position = snapshotPosition + 1; position <= 4; position++) {
        assertTrue(reader.hasNext());
        assertEquals(position, reader.next().getKey());
      }
      assertFalse(reader.hasNext());
      assertEquals(5L, reader.getNextPosition());
    }
  }

  @Test
  void laterCheckpointAdvancesAnEmptyReaderWithoutInventingRecords() {
    final var snapshotPosition = new AtomicLong();
    try (final var reader =
        new RaftLogStreamReader(() -> new FakeJournalReader(List.of()), snapshotPosition::get)) {
      reader.seekToNextEvent(0);
      assertFalse(reader.hasNext());
      assertEquals(1L, reader.getNextPosition());
      snapshotPosition.set(12L);

      assertFalse(reader.hasNext());
      assertEquals(13L, reader.getNextPosition());
      reader.seekToNextEvent(20L);
      assertEquals(21L, reader.getNextPosition());
    }
  }

  /** A forward cursor over a fixed list of batch entries, mirroring the journal reader contract. */
  private static final class FakeJournalReader implements RaftJournalReader {
    private final List<PersistedRaftRecord> entries;
    private int cursor = 0;

    FakeJournalReader(final List<PersistedRaftRecord> entries) {
      this.entries = entries;
    }

    @Override
    public void seek(final long index) {
      cursor = (int) Math.max(0, index - 1);
    }

    @Override
    public void seekToFirst() {
      cursor = 0;
    }

    @Override
    public long nextIndex() {
      return cursor + 1L;
    }

    @Override
    public boolean hasNext() {
      return cursor < entries.size();
    }

    @Override
    public PersistedRaftRecord next() {
      return entries.get(cursor++);
    }

    @Override
    public void close() {}
  }
}
