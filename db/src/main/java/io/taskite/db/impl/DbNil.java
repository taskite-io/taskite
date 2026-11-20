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

import io.taskite.db.DbValue;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/** A zero-information {@link DbValue} used when only the key matters (existence marker). */
public final class DbNil implements DbValue {

  public static final DbNil INSTANCE = new DbNil();

  private static final byte EXISTENCE_BYTE = (byte) -1;

  private DbNil() {}

  @Override
  public void wrap(final DirectBuffer directBuffer, final int offset, final int length) {
    // nothing to do
  }

  @Override
  public int getLength() {
    return Byte.BYTES;
  }

  @Override
  public void write(final MutableDirectBuffer mutableDirectBuffer, final int offset) {
    mutableDirectBuffer.putByte(offset, EXISTENCE_BYTE);
  }

  @Override
  public String toString() {
    return "DbNil{}";
  }
}
