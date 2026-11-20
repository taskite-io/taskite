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
package io.taskite.streamplatform.command;

import io.taskite.protocol.record.ExecuteCommandResponseEncoder;
import io.taskite.protocol.record.MessageHeaderEncoder;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.util.BufferWriter;
import java.nio.charset.StandardCharsets;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

final class CommandResponseEncoder {
  private static final BufferWriter NO_VALUE =
      new BufferWriter() {
        @Override
        public int getLength() {
          return 0;
        }

        @Override
        public void write(final MutableDirectBuffer buffer, final int offset) {}
      };

  private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
  private final ExecuteCommandResponseEncoder responseEncoder = new ExecuteCommandResponseEncoder();
  private final UnsafeBuffer rejectionReason = new UnsafeBuffer(0, 0);

  private int partitionId = ExecuteCommandResponseEncoder.partitionIdNullValue();
  private long key = ExecuteCommandResponseEncoder.keyNullValue();
  private BufferWriter valueWriter = NO_VALUE;
  private RecordType recordType = RecordType.NULL_VAL;
  private ValueType valueType = ValueType.NULL_VAL;
  private short intent = Intent.NULL_VAL;
  private RejectionType rejectionType = RejectionType.NULL_VAL;

  public void reset() {
    partitionId = ExecuteCommandResponseEncoder.partitionIdNullValue();
    key = ExecuteCommandResponseEncoder.keyNullValue();
    valueWriter = NO_VALUE;
    recordType = RecordType.NULL_VAL;
    valueType = ValueType.NULL_VAL;
    intent = Intent.NULL_VAL;
    rejectionType = RejectionType.NULL_VAL;
    rejectionReason.wrap(0, 0);
  }

  public void wrap(final LoggedEvent record) {
    final RecordMetadata metadata = record.metadata();
    // The response partition id is decorative — the gateway routes by request, and requestStreamId
    // is a pure transport-correlation field, not a partition source.
    final byte[] rejectionReasonBytes = bytes(metadata.getRejectionReason());
    partitionId(ExecuteCommandResponseEncoder.partitionIdNullValue())
        .key(record.key())
        .recordType(metadata.getRecordType())
        .valueType(metadata.getValueType())
        .intent(metadata.getIntent())
        .rejectionType(metadata.getRejectionType())
        .rejectionReason(new UnsafeBuffer(rejectionReasonBytes))
        .valueWriter(record.value());
  }

  public CommandResponseEncoder partitionId(final int partitionId) {
    this.partitionId = partitionId;
    return this;
  }

  public CommandResponseEncoder key(final long key) {
    this.key = key;
    return this;
  }

  public CommandResponseEncoder intent(final Intent intent) {
    this.intent = intent == null ? Intent.NULL_VAL : intent.value();
    return this;
  }

  public CommandResponseEncoder recordType(final RecordType recordType) {
    this.recordType = recordType;
    return this;
  }

  public CommandResponseEncoder valueType(final ValueType valueType) {
    this.valueType = valueType;
    return this;
  }

  public CommandResponseEncoder rejectionType(final RejectionType rejectionType) {
    this.rejectionType = rejectionType;
    return this;
  }

  public CommandResponseEncoder rejectionReason(final DirectBuffer rejectionReason) {
    this.rejectionReason.wrap(rejectionReason);
    return this;
  }

  public CommandResponseEncoder valueWriter(final BufferWriter writer) {
    valueWriter = writer == null ? NO_VALUE : writer;
    return this;
  }

  public int getLength() {
    return MessageHeaderEncoder.ENCODED_LENGTH
        + ExecuteCommandResponseEncoder.BLOCK_LENGTH
        + ExecuteCommandResponseEncoder.valueHeaderLength()
        + valueLength()
        + ExecuteCommandResponseEncoder.rejectionReasonHeaderLength()
        + rejectionReason.capacity();
  }

  public void write(final MutableDirectBuffer buffer, final int offset) {
    writeHeader(buffer, offset);
    writeValue(buffer);
    responseEncoder.putRejectionReason(rejectionReason, 0, rejectionReason.capacity());
  }

  private void writeHeader(final MutableDirectBuffer buffer, final int offset) {
    responseEncoder
        .wrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
        .partitionId(partitionId)
        .key(key)
        .recordType(recordType)
        .valueType(valueType)
        .intent(intent)
        .rejectionType(rejectionType);
  }

  private void writeValue(final MutableDirectBuffer buffer) {
    final int valueLength = valueWriter.getLength();
    int cursor = responseEncoder.limit();
    buffer.putInt(cursor, valueLength, ExecuteCommandResponseEncoder.BYTE_ORDER);

    cursor += ExecuteCommandResponseEncoder.valueHeaderLength();
    valueWriter.write(buffer, cursor);
    responseEncoder.limit(cursor + valueLength);
  }

  private byte[] bytes(final String value) {
    return value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
  }

  private int valueLength() {
    return valueWriter.getLength();
  }
}
