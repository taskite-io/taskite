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

import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.UnifiedRecordValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * A single log record represented in one of two modes:
 *
 * <ul>
 *   <li><b>Byte-backed</b> — a read view over a committed entry's bytes (header + metadata + value
 *       regions). The value bytes are handed to the caller as-is; decoding them into a concrete
 *       business type is the engine's concern (it owns the value-type registry), so logstream stays
 *       value-type agnostic. Produced by {@link #of}, {@link #fromBatch}, and the reader path.
 *   <li><b>Decoded</b> — an in-memory {@code (key, metadata, value)} carrier used before an entry
 *       is appended (client commands, engine follow-ups) and for building responses, where no log
 *       position exists yet. Produced by {@link #ofRecord}.
 * </ul>
 *
 * A byte-backed instance owns exactly one entry's bytes; a decoded instance owns its typed fields.
 */
public final class LoggedEvent {
  public static final int DEFAULT_RECORD_VERSION = 1;

  // Byte-backed mode: non-null buffer over the framed entry. Null in decoded mode.
  private final DirectBuffer buffer;
  private final int entryLength;
  // Decoded mode: the typed record. Null in byte-backed mode.
  private final long recordKey;
  private final RecordMetadata metadata;
  private final UnifiedRecordValue value;

  LoggedEvent(final byte[] entryBytes) {
    this.buffer = new UnsafeBuffer(entryBytes);
    this.entryLength = entryBytes.length;
    this.recordKey = LogAppendEntry.KEY_NULL_VALUE;
    this.metadata = null;
    this.value = null;
  }

  private LoggedEvent(
      final long recordKey, final RecordMetadata metadata, final UnifiedRecordValue value) {
    this.buffer = null;
    this.entryLength = 0;
    this.recordKey = recordKey;
    this.metadata = Objects.requireNonNull(metadata, "metadata").copy();
    this.value = Objects.requireNonNull(value, "value");
  }

  /**
   * Serializes {@code entry} at the given coordinates into a standalone byte-backed {@link
   * LoggedEvent}. Used by the sequencer's reader path and by tests to synthesize a committed entry
   * without going through raft.
   */
  public static LoggedEvent of(
      final LogAppendEntry entry,
      final long position,
      final long sourcePosition,
      final long timestamp) {
    final byte[] bytes = new byte[LogEntryDescriptor.framedLength(entry)];
    LogEntryDescriptor.serialize(
        new UnsafeBuffer(bytes), 0, entry, position, sourcePosition, timestamp);
    return new LoggedEvent(bytes);
  }

  /**
   * Builds a decoded {@code (key, metadata, value)} record with no log position yet. Used for
   * client commands, engine follow-ups, and responses before they are appended to (or independent
   * of) the log.
   */
  public static LoggedEvent ofRecord(
      final long key, final RecordMetadata metadata, final UnifiedRecordValue value) {
    return new LoggedEvent(key, metadata, value);
  }

  /**
   * Decodes a raft entry's batch payload (one or more framed entries) into the byte-backed {@link
   * LoggedEvent}s it carries, in position order. Used by tooling/tests that read raw segment
   * payloads.
   */
  public static List<LoggedEvent> fromBatch(final byte[] batchPayload) {
    final List<byte[]> entries = BatchPayloadCodec.decode(batchPayload);
    final List<LoggedEvent> events = new ArrayList<>(entries.size());
    for (final byte[] entry : entries) {
      events.add(new LoggedEvent(entry));
    }
    return events;
  }

  public long getPosition() {
    return buffer == null ? -1 : LogEntryDescriptor.position(buffer, 0);
  }

  public long getSourceEventPosition() {
    return buffer == null ? -1 : LogEntryDescriptor.sourceEventPosition(buffer, 0);
  }

  public long getKey() {
    return buffer == null ? recordKey : LogEntryDescriptor.key(buffer, 0);
  }

  public long getTimestamp() {
    return buffer == null ? 0 : LogEntryDescriptor.timestamp(buffer, 0);
  }

  public boolean shouldSkipProcessing() {
    return buffer != null && LogEntryDescriptor.skipProcessing(buffer, 0);
  }

  /** The record key. Available in both modes (see {@link #getKey()}). */
  public long key() {
    return getKey();
  }

  /**
   * The decoded metadata. Only present on a decoded record ({@link #ofRecord}); a byte-backed entry
   * exposes its metadata bytes through {@link #readMetadata(RecordMetadata)} instead.
   */
  public RecordMetadata metadata() {
    return metadata;
  }

  /**
   * The decoded value. Only present on a decoded record ({@link #ofRecord}); a byte-backed entry's
   * value bytes are decoded by the configured typed event registry using {@link #valueBuffer()} /
   * {@link #valueOffset()} / {@link #valueLength()}.
   */
  public UnifiedRecordValue value() {
    return value;
  }

  /** Reads the byte-backed entry's metadata into {@code target} and returns it. */
  public RecordMetadata readMetadata(final RecordMetadata target) {
    target.wrap(buffer, LogEntryDescriptor.metadataOffset(0), metadataLength());
    return target;
  }

  /** The backing buffer; read the value region at {@link #valueOffset()}/{@link #valueLength()}. */
  public DirectBuffer valueBuffer() {
    return buffer;
  }

  public int valueOffset() {
    return LogEntryDescriptor.valueOffset(0, metadataLength());
  }

  public int valueLength() {
    return entryLength - LogEntryDescriptor.HEADER_LENGTH - metadataLength();
  }

  private int metadataLength() {
    return LogEntryDescriptor.metadataLength(buffer, 0);
  }
}
