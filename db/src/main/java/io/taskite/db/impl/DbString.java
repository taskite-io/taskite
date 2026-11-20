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
package io.taskite.db.impl;

import static io.taskite.db.impl.DbConstants.DB_BYTE_ORDER;

import io.taskite.db.DbKey;
import io.taskite.db.DbValue;
import io.taskite.util.BufferUtil;
import java.nio.charset.StandardCharsets;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/** A {@link DbKey}/{@link DbValue} holding a UTF-8 string, length-prefixed on the wire. */
public final class DbString implements DbKey, DbValue {

  private final DirectBuffer bytes = new UnsafeBuffer(0, 0);

  public void wrapString(final String string) {
    bytes.wrap(string.getBytes(StandardCharsets.UTF_8));
  }

  public void wrapBuffer(final DirectBuffer buffer) {
    bytes.wrap(buffer);
  }

  @Override
  public void wrap(final DirectBuffer directBuffer, int offset, final int length) {
    final int stringLen = directBuffer.getInt(offset, DB_BYTE_ORDER);
    offset += Integer.BYTES;

    final byte[] b = new byte[stringLen];
    directBuffer.getBytes(offset, b);
    bytes.wrap(b);
  }

  @Override
  public int getLength() {
    return Integer.BYTES + bytes.capacity();
  }

  @Override
  public void write(final MutableDirectBuffer mutableDirectBuffer, int offset) {
    final int length = bytes.capacity();
    mutableDirectBuffer.putInt(offset, length, DB_BYTE_ORDER);
    offset += Integer.BYTES;

    mutableDirectBuffer.putBytes(offset, bytes, 0, bytes.capacity());
  }

  public DirectBuffer getBuffer() {
    return bytes;
  }

  @Override
  public String toString() {
    return BufferUtil.bufferAsString(bytes);
  }
}
