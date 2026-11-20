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
import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Defines the little-endian on-disk layout of one log entry. The logstream layer owns this header
 * and frames opaque metadata and value regions without interpreting business value types:
 *
 * <pre>
 * [flags:byte][position:long][sourceEventPosition:long][key:long][timestamp:long]
 * [metadataLength:int][metadata bytes][value bytes]
 * </pre>
 *
 * The value length is implied by the entry's total length minus the header and metadata.
 */
final class LogEntryDescriptor {
  static final byte FLAG_SKIP_PROCESSING = 0x01;

  private static final int FLAGS_OFFSET = 0;
  private static final int POSITION_OFFSET = FLAGS_OFFSET + Byte.BYTES;
  private static final int SOURCE_EVENT_POSITION_OFFSET = POSITION_OFFSET + Long.BYTES;
  private static final int KEY_OFFSET = SOURCE_EVENT_POSITION_OFFSET + Long.BYTES;
  private static final int TIMESTAMP_OFFSET = KEY_OFFSET + Long.BYTES;
  private static final int METADATA_LENGTH_OFFSET = TIMESTAMP_OFFSET + Long.BYTES;
  static final int HEADER_LENGTH = METADATA_LENGTH_OFFSET + Integer.BYTES;

  private LogEntryDescriptor() {}

  static int framedLength(final LogAppendEntry entry) {
    return HEADER_LENGTH + entry.recordMetadata().getLength() + entry.recordValue().getLength();
  }

  static int serialize(
      final MutableDirectBuffer buffer,
      final int offset,
      final LogAppendEntry entry,
      final long position,
      final long sourcePosition,
      final long timestamp) {
    final RecordMetadata metadata = entry.recordMetadata();
    final UnifiedRecordValue value = entry.recordValue();
    final int metadataLength = metadata.getLength();

    byte flags = 0;
    if (entry.isProcessed()) {
      flags |= FLAG_SKIP_PROCESSING;
    }
    buffer.putByte(offset + FLAGS_OFFSET, flags);
    buffer.putLong(offset + POSITION_OFFSET, position, ByteOrder.LITTLE_ENDIAN);
    buffer.putLong(offset + SOURCE_EVENT_POSITION_OFFSET, sourcePosition, ByteOrder.LITTLE_ENDIAN);
    buffer.putLong(offset + KEY_OFFSET, entry.key(), ByteOrder.LITTLE_ENDIAN);
    buffer.putLong(offset + TIMESTAMP_OFFSET, timestamp, ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(offset + METADATA_LENGTH_OFFSET, metadataLength, ByteOrder.LITTLE_ENDIAN);
    metadata.write(buffer, offset + HEADER_LENGTH);
    value.write(buffer, offset + HEADER_LENGTH + metadataLength);
    return framedLength(entry);
  }

  static boolean skipProcessing(final DirectBuffer buffer, final int offset) {
    return (buffer.getByte(offset + FLAGS_OFFSET) & FLAG_SKIP_PROCESSING) != 0;
  }

  static long position(final DirectBuffer buffer, final int offset) {
    return buffer.getLong(offset + POSITION_OFFSET, ByteOrder.LITTLE_ENDIAN);
  }

  static long sourceEventPosition(final DirectBuffer buffer, final int offset) {
    return buffer.getLong(offset + SOURCE_EVENT_POSITION_OFFSET, ByteOrder.LITTLE_ENDIAN);
  }

  static long key(final DirectBuffer buffer, final int offset) {
    return buffer.getLong(offset + KEY_OFFSET, ByteOrder.LITTLE_ENDIAN);
  }

  static long timestamp(final DirectBuffer buffer, final int offset) {
    return buffer.getLong(offset + TIMESTAMP_OFFSET, ByteOrder.LITTLE_ENDIAN);
  }

  static int metadataLength(final DirectBuffer buffer, final int offset) {
    return buffer.getInt(offset + METADATA_LENGTH_OFFSET, ByteOrder.LITTLE_ENDIAN);
  }

  static int metadataOffset(final int offset) {
    return offset + HEADER_LENGTH;
  }

  static int valueOffset(final int offset, final int metadataLength) {
    return offset + HEADER_LENGTH + metadataLength;
  }
}
