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
package io.taskite.cluster.raft.jraft.log;

import io.taskite.cluster.raft.log.PersistedRaftRecord;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Encodes/decodes a batch's application position span alongside its payload inside a jraft {@code
 * Task} / log entry (migration decision D1). The business layer reasons in {@code [lowestPosition,
 * highestPosition]} spans (see {@link PersistedRaftRecord}) while jraft reasons in {@code index}; a
 * jraft entry carries no first-class position fields, so the span is prefixed onto the user bytes:
 *
 * <pre>[lowestPosition:long][highestPosition:long][payload...]</pre>
 *
 * <p>On {@code onApply}, {@link #decode(ByteBuffer, long, long)} reconstructs a {@link
 * PersistedRaftRecord} (using the jraft-assigned {@code term}/{@code index}) so the existing apply
 * path can expand the batch into per-position records exactly as {@code RaftLogStreamReader} does.
 */
public final class JRaftLogEntryCodec {
  /** Bytes prepended to every entry payload: two longs (low, high). */
  public static final int HEADER_LENGTH = Long.BYTES * 2;

  private JRaftLogEntryCodec() {}

  /** Frames {@code payload} with its position span into a fresh flipped buffer ready for a Task. */
  public static ByteBuffer encode(
      final long lowestPosition, final long highestPosition, final byte[] payload) {
    final byte[] body = payload == null ? new byte[0] : payload;
    final ByteBuffer buffer = ByteBuffer.allocate(HEADER_LENGTH + body.length);
    buffer.putLong(lowestPosition);
    buffer.putLong(highestPosition);
    buffer.put(body);
    buffer.flip();
    return buffer;
  }

  /** The lowest position of the framed entry, read without consuming {@code buffer}. */
  public static long lowestPosition(final ByteBuffer buffer) {
    return buffer.getLong(buffer.position());
  }

  /** The highest position of the framed entry, read without consuming {@code buffer}. */
  public static long highestPosition(final ByteBuffer buffer) {
    return buffer.getLong(buffer.position() + Long.BYTES);
  }

  /** Negative spans are reserved for replicated partition execution authority. */
  public static boolean isExecutionAuthority(final ByteBuffer buffer) {
    return buffer.remaining() >= HEADER_LENGTH
        && lowestPosition(buffer) == -1
        && highestPosition(buffer) == -1;
  }

  public static ByteBuffer executionAuthority(final long generation, final String owner) {
    final byte[] id = owner.getBytes(StandardCharsets.UTF_8);
    return encode(-1, -1, ByteBuffer.allocate(8 + id.length).putLong(generation).put(id).array());
  }

  public static long authorityGeneration(final ByteBuffer buffer) {
    return buffer.getLong(buffer.position() + HEADER_LENGTH);
  }

  public static String authorityOwner(final ByteBuffer buffer) {
    final byte[] body = payload(buffer);
    return new String(body, Long.BYTES, body.length - Long.BYTES, StandardCharsets.UTF_8);
  }

  /** Extracts the payload bytes following the span header. */
  public static byte[] payload(final ByteBuffer buffer) {
    final int payloadLength = buffer.remaining() - HEADER_LENGTH;
    final byte[] payload = new byte[Math.max(0, payloadLength)];
    if (payloadLength > 0) {
      buffer.get(buffer.position() + HEADER_LENGTH, payload);
    }
    return payload;
  }

  /**
   * Reconstructs the {@link PersistedRaftRecord} a committed jraft entry represents, marrying the
   * jraft-assigned {@code term}/{@code index} with the position span carried in the payload.
   */
  public static PersistedRaftRecord decode(
      final ByteBuffer buffer, final long term, final long index) {
    return new PersistedRaftRecord(
        term, index, lowestPosition(buffer), highestPosition(buffer), payload(buffer));
  }
}
