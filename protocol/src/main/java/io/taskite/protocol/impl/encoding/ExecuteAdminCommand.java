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
package io.taskite.protocol.impl.encoding;

import io.taskite.protocol.record.*;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.util.BufferWriter;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public abstract class ExecuteAdminCommand<T extends UnifiedRecordValue> {
  protected final ExecuteAdminRequest request = new ExecuteAdminRequest();
  protected final ExecuteAdminResponse response = new ExecuteAdminResponse();

  protected ExecuteAdminCommand(final ValueType valueType, final Intent intent) {
    request.setValueType(valueType).setIntent(intent);
  }

  public abstract BufferWriter getRequestWriter();

  protected abstract T toResponseDto(DirectBuffer buffer);

  public void serializeValue() {
    final var writer = getRequestWriter();
    final var buffer = new UnsafeBuffer(new byte[writer.getLength()]);
    writer.write(buffer, 0);
    request.setValue(buffer, 0, buffer.capacity());
  }

  public byte[] serializeRequest() {
    serializeValue();
    final var buffer = new UnsafeBuffer(new byte[request.getLength()]);
    request.write(buffer, 0);
    return buffer.byteArray();
  }

  public String getClusterId() {
    return request.getClusterId();
  }

  public void setClusterId(final String clusterId) {
    if (!getClusterId().isEmpty() && !getClusterId().equals(clusterId)) {
      throw new IllegalArgumentException("administration cluster identity is immutable");
    }
    request.setClusterId(clusterId);
  }

  public int getPartitionId() {
    return request.getPartitionId();
  }

  public void setPartitionId(final int partitionId) {
    if (getPartitionId() != 0 && getPartitionId() != partitionId) {
      throw new IllegalArgumentException("administration partition identity is immutable");
    }
    request.setPartitionId(partitionId);
  }

  public ValueType getValueType() {
    return request.getValueType();
  }

  public Intent getIntent() {
    return request.getIntent();
  }
}
