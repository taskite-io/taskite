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
import java.util.Objects;

/**
 * An immutable application record awaiting append. The caller supplies typed {@code metadata} and
 * {@code value}; the logstream layer serializes them through {@link RecordMetadata#write} and
 * {@link UnifiedRecordValue#write} without knowing concrete value types. {@code position}/{@code
 * sourceEventPosition}/{@code timestamp} are assigned by the {@link Sequencer} and stored in the
 * log entry header, not here.
 */
public interface LogAppendEntry {

  long KEY_NULL_VALUE = -1L;

  /** The record key, or {@link #KEY_NULL_VALUE} when unset. */
  long key();

  /** Back-pointer to a command in the same batch that produced this entry, or -1. */
  int sourceIndex();

  RecordMetadata recordMetadata();

  UnifiedRecordValue recordValue();

  /** True when the entry was already processed (state-only on read-back). */
  default boolean isProcessed() {
    return false;
  }

  /**
   * Serialized length of the application payload (metadata + value), excluding the entry header.
   */
  default int getLength() {
    return recordMetadata().getLength() + recordValue().getLength();
  }

  static LogAppendEntry of(
      final RecordMetadata recordMetadata, final UnifiedRecordValue recordValue) {
    return of(KEY_NULL_VALUE, recordMetadata, recordValue);
  }

  static LogAppendEntry of(
      final long key, final RecordMetadata recordMetadata, final UnifiedRecordValue recordValue) {
    return new LogAppendEntryImpl(
        key,
        -1,
        Objects.requireNonNull(recordMetadata, "recordMetadata"),
        Objects.requireNonNull(recordValue, "recordValue"),
        false);
  }

  /** Wraps {@code entry}, marking it processed (used for state-only follow-ups). */
  static LogAppendEntry ofProcessed(final LogAppendEntry entry) {
    return new LogAppendEntryImpl(
        entry.key(), entry.sourceIndex(), entry.recordMetadata(), entry.recordValue(), true);
  }

  record LogAppendEntryImpl(
      long key,
      int sourceIndex,
      RecordMetadata recordMetadata,
      UnifiedRecordValue recordValue,
      boolean isProcessed)
      implements LogAppendEntry {}
}
