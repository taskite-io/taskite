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
package io.taskite.engine.state.variable;

import io.taskite.db.DbValue;
import io.taskite.protocol.impl.encoding.MsgPackConverter;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/** Persisted variable key and value in one MessagePack document. */
final class VariableRecordValue implements DbValue {

  private PersistedVariable record;
  private byte[] bytes = new byte[0];

  long getKey() {
    return record.key();
  }

  Object getValue() {
    return record.value();
  }

  VariableRecordValue set(final long key, final Object value) {
    bytes = MsgPackConverter.convertToMsgPack(new PersistedVariable(key, value));
    record = MsgPackConverter.convertToObject(PersistedVariable.class, bytes);
    return this;
  }

  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    final byte[] encoded = new byte[length];
    buffer.getBytes(offset, encoded);
    final var decoded = MsgPackConverter.convertToObject(PersistedVariable.class, encoded);
    bytes = encoded;
    record = decoded;
  }

  @Override
  public int getLength() {
    return bytes.length;
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    buffer.putBytes(offset, bytes);
  }

  private record PersistedVariable(long key, Object value) {}
}
