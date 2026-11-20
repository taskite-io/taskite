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
package io.taskite.protocol.record;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.taskite.protocol.impl.encoding.MsgPackConverter;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class UnifiedRecordValue implements RecordValue {
  @Override
  @JsonIgnore
  public int getLength() {
    return serializedBytes().length;
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    buffer.putBytes(offset, serializedBytes());
  }

  public void wrap(final DirectBuffer buff) {
    wrap(buff, 0, buff.capacity());
  }

  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    MsgPackConverter.decodeInto(this, buffer, offset, length);
  }

  @Override
  public String toJson() {
    return MsgPackConverter.convertToJson(serializedBytes());
  }

  private byte[] serializedBytes() {
    return MsgPackConverter.convertToMsgPack(this);
  }
}
