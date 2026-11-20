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

import io.taskite.db.DbKey;
import io.taskite.db.DbValue;
import io.taskite.util.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * A {@link DbKey}/{@link DbValue} holding a raw byte payload, written verbatim (no length prefix).
 * As a value it consumes the whole stored buffer on {@link #wrap}, so it reads opaque bytes (e.g. a
 * record serialized by another module) without decoding them.
 */
public final class DbBytes implements DbKey, DbValue {

  private final DirectBuffer bytes = new UnsafeBuffer(0, 0);

  public void wrapBytes(final byte[] value) {
    bytes.wrap(value);
  }

  @Override
  public void wrap(final DirectBuffer directBuffer, final int offset, final int length) {
    final byte[] bytesToWrap = new byte[length];
    directBuffer.getBytes(offset, bytesToWrap, 0, bytesToWrap.length);
    bytes.wrap(bytesToWrap);
  }

  @Override
  public int getLength() {
    return bytes.capacity();
  }

  @Override
  public void write(final MutableDirectBuffer mutableDirectBuffer, final int offset) {
    mutableDirectBuffer.putBytes(offset, bytes, 0, bytes.capacity());
  }

  public byte[] getBytes() {
    return bytes.byteArray();
  }

  public DirectBuffer getDirectBuffer() {
    return bytes;
  }

  @Override
  public String toString() {
    return "DbBytes{" + BufferUtil.bufferAsString(bytes) + '}';
  }
}
