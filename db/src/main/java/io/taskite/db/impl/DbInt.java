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
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/** A {@link DbKey}/{@link DbValue} holding a single {@code int}. */
public final class DbInt implements DbKey, DbValue {

  private int intValue;

  public void wrapInt(final int value) {
    intValue = value;
  }

  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    intValue = buffer.getInt(offset, DB_BYTE_ORDER);
  }

  @Override
  public int getLength() {
    return Integer.BYTES;
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    buffer.putInt(offset, intValue, DB_BYTE_ORDER);
  }

  public int getValue() {
    return intValue;
  }

  @Override
  public String toString() {
    return "DbInt{" + intValue + '}';
  }
}
