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

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Frames a batch of business records into the single opaque payload carried by one raft entry, and
 * decodes it back. Layout: {@code [recordCount:int][ (recordLen:int, recordBytes)... ]}, little
 * endian. The records' positions are not stored per-record: a batch spans the contiguous range
 * {@code [lowestPosition, highestPosition]} carried on the raft entry, so record {@code i} has
 * position {@code lowestPosition + i}.
 */
final class BatchPayloadCodec {
  private BatchPayloadCodec() {}

  static byte[] encode(final List<byte[]> records) {
    int length = Integer.BYTES;
    for (final byte[] record : records) {
      length += Integer.BYTES + record.length;
    }
    final byte[] out = new byte[length];
    final UnsafeBuffer buffer = new UnsafeBuffer(out);
    int cursor = 0;
    buffer.putInt(cursor, records.size(), ByteOrder.LITTLE_ENDIAN);
    cursor += Integer.BYTES;
    for (final byte[] record : records) {
      buffer.putInt(cursor, record.length, ByteOrder.LITTLE_ENDIAN);
      cursor += Integer.BYTES;
      buffer.putBytes(cursor, record);
      cursor += record.length;
    }
    return out;
  }

  static List<byte[]> decode(final byte[] payload) {
    final UnsafeBuffer buffer = new UnsafeBuffer(payload);
    int cursor = 0;
    final int count = buffer.getInt(cursor, ByteOrder.LITTLE_ENDIAN);
    cursor += Integer.BYTES;
    final List<byte[]> records = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      final int recordLength = buffer.getInt(cursor, ByteOrder.LITTLE_ENDIAN);
      cursor += Integer.BYTES;
      final byte[] record = new byte[recordLength];
      buffer.getBytes(cursor, record);
      cursor += recordLength;
      records.add(record);
    }
    return records;
  }
}
