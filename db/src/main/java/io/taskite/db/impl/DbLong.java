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

/** A {@link DbKey}/{@link DbValue} holding a single {@code long}. */
public final class DbLong implements DbKey, DbValue {

  private long longValue;

  public void wrapLong(final long value) {
    longValue = value;
  }

  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    longValue = buffer.getLong(offset, DB_BYTE_ORDER);
  }

  @Override
  public int getLength() {
    return Long.BYTES;
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    buffer.putLong(offset, longValue, DB_BYTE_ORDER);
  }

  public long getValue() {
    return longValue;
  }

  @Override
  public String toString() {
    return "DbLong{" + longValue + '}';
  }
}
