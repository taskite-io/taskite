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

import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.util.BufferWriter;
import java.util.Arrays;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public abstract class ExecuteCommand<T extends UnifiedRecordValue> {
  protected final ExecuteCommandRequest request = new ExecuteCommandRequest();
  protected final ExecuteCommandResponse response = new ExecuteCommandResponse();

  protected ExecuteCommand(final ValueType valueType, final Intent intent) {
    request.setValueType(valueType);
    request.setIntent(intent);
  }

  public abstract BufferWriter getRequestWriter();

  protected abstract T toResponseDto(DirectBuffer buffer);

  public void serializeValue() {
    final BufferWriter valueWriter = getRequestWriter();
    if (valueWriter != null) {
      final int valueLength = valueWriter.getLength();
      final MutableDirectBuffer buffer = new UnsafeBuffer(new byte[valueLength]);
      valueWriter.write(buffer, 0);
      setSerializedValue(buffer);
    }
  }

  public byte[] serializeRequest() {
    final var buffer = new UnsafeBuffer(new byte[request.getLength()]);
    request.write(buffer, 0);
    return Arrays.copyOf(buffer.byteArray(), request.getLength());
  }

  public int getPartitionId() {
    return request.getPartitionId();
  }

  public void setPartitionId(final int partitionId) {
    request.setPartitionId(partitionId);
  }

  public String getClusterId() {
    return request.getClusterId();
  }

  public void setClusterId(final String clusterId) {
    request.setClusterId(clusterId);
  }

  public long getKey() {
    return request.getKey();
  }

  public void setKey(final long key) {
    request.setKey(key);
  }

  public ValueType getValueType() {
    return request.getValueType();
  }

  public Intent getIntent() {
    return request.getIntent();
  }

  protected void setSerializedValue(final DirectBuffer buffer) {
    request.setValue(buffer, 0, buffer.capacity());
  }

  protected void wrapResponse(final DirectBuffer buffer) {
    response.wrap(buffer, 0, buffer.capacity());
  }

  protected boolean isValidResponse() {
    return response.getValueType() == request.getValueType();
  }

  protected boolean isRejection() {
    return response.getRecordType() == RecordType.COMMAND_REJECTION;
  }

  protected long responseKey() {
    return response.getKey();
  }

  protected int responsePartitionId() {
    return response.getPartitionId();
  }

  protected DirectBuffer responseValue() {
    return response.getValue();
  }

  protected RejectionType responseRejectionType() {
    return response.getRejectionType();
  }

  protected DirectBuffer responseRejectionReason() {
    return response.getRejectionReason();
  }
}
