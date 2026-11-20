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

import io.taskite.protocol.record.intent.Intent;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * The business metadata of a log record: record type, value type, intent, version and rejection
 * info, plus the request routing ids. This is self-serializing ({@link #write}/{@link #wrap}/{@link
 * #getLength}) so the log layer can persist it polymorphically without knowing concrete value
 * types. Log coordinates (position, sourceEventPosition, timestamp, skipProcessing) are
 * <em>not</em> part of this value; they live in the entry header owned by the logstream layer.
 */
public final class RecordMetadata {
  private static final int DEFAULT_RECORD_VERSION = 1;

  private RecordType recordType = RecordType.NULL_VAL;
  private String requestStreamId;
  private String requestId;
  private ValueType valueType = ValueType.NULL_VAL;
  private Intent intent = null;
  private int recordVersion = DEFAULT_RECORD_VERSION;
  private short intentValue = Intent.NULL_VAL;
  private RejectionType rejectionType;
  private String rejectionReason = "";

  public RecordMetadata() {
    reset();
  }

  public RecordMetadata reset() {
    requestId = "";
    requestStreamId = "";
    recordType = RecordType.NULL_VAL;
    valueType = ValueType.NULL_VAL;
    intentValue = Intent.NULL_VAL;
    intent = null;
    rejectionType = RejectionType.NULL_VAL;
    rejectionReason = "";
    recordVersion = 1;
    return this;
  }

  public RecordMetadata copy() {
    return new RecordMetadata()
        .requestId(requestId)
        .requestStreamId(requestStreamId)
        .recordType(recordType)
        .valueType(valueType)
        .intent(intent)
        .intentValue(intentValue)
        .rejectionType(rejectionType)
        .rejectionReason(rejectionReason)
        .recordVersion(recordVersion);
  }

  public RecordType getRecordType() {
    return recordType;
  }

  public RecordMetadata recordType(RecordType recordType) {
    this.recordType = recordType;
    return this;
  }

  public String getRequestStreamId() {
    return requestStreamId;
  }

  public RecordMetadata requestStreamId(String requestStreamId) {
    this.requestStreamId = requestStreamId;
    return this;
  }

  public String getRequestId() {
    return requestId;
  }

  public RecordMetadata requestId(String requestId) {
    this.requestId = requestId;
    return this;
  }

  public ValueType getValueType() {
    return valueType;
  }

  public RecordMetadata valueType(ValueType valueType) {
    this.valueType = valueType;
    return this;
  }

  public Intent getIntent() {
    return intent;
  }

  public RecordMetadata intent(Intent intent) {
    this.intent = intent;
    return this;
  }

  public int getRecordVersion() {
    return recordVersion;
  }

  public RecordMetadata recordVersion(int recordVersion) {
    this.recordVersion = recordVersion;
    return this;
  }

  public short getIntentValue() {
    return intentValue;
  }

  public RecordMetadata intentValue(short intentValue) {
    this.intentValue = intentValue;
    return this;
  }

  public RejectionType getRejectionType() {
    return rejectionType;
  }

  public RecordMetadata rejectionType(RejectionType rejectionType) {
    this.rejectionType = rejectionType;
    return this;
  }

  public String getRejectionReason() {
    return rejectionReason;
  }

  public RecordMetadata rejectionReason(String rejectionReason) {
    this.rejectionReason = rejectionReason;
    return this;
  }

  // --- serialization --------------------------------------------------------------------------
  // Self-describing byte layout so the log layer can persist metadata without knowing value types.
  // [version:byte][recordType:byte][valueType:byte][intent:short][recordVersion:int]
  // [rejectionType:byte][requestStreamId:str][requestId:str][rejectionReason:str]
  private static final byte FORMAT_VERSION = 1;

  public int getLength() {
    return Byte.BYTES // format version
        + Byte.BYTES // recordType
        + Byte.BYTES // valueType
        + Short.BYTES // intent
        + Integer.BYTES // recordVersion
        + Byte.BYTES // rejectionType
        + sizedLength(requestStreamId)
        + sizedLength(requestId)
        + sizedLength(rejectionReason);
  }

  public void write(final MutableDirectBuffer buffer, final int offset) {
    int cursor = offset;
    buffer.putByte(cursor, FORMAT_VERSION);
    cursor += Byte.BYTES;
    buffer.putByte(cursor, (byte) recordType.value());
    cursor += Byte.BYTES;
    buffer.putByte(cursor, (byte) valueType.value());
    cursor += Byte.BYTES;
    buffer.putShort(cursor, toIntentValue(), ByteOrder.LITTLE_ENDIAN);
    cursor += Short.BYTES;
    buffer.putInt(cursor, recordVersion, ByteOrder.LITTLE_ENDIAN);
    cursor += Integer.BYTES;
    buffer.putByte(cursor, (byte) rejectionType.value());
    cursor += Byte.BYTES;
    cursor = writeString(buffer, cursor, requestStreamId);
    cursor = writeString(buffer, cursor, requestId);
    writeString(buffer, cursor, rejectionReason);
  }

  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    reset();
    int cursor = offset;
    final byte formatVersion = buffer.getByte(cursor);
    cursor += Byte.BYTES;
    if (formatVersion != FORMAT_VERSION) {
      throw new IllegalArgumentException("Unexpected metadata format version: " + formatVersion);
    }
    recordType = RecordType.get((short) (buffer.getByte(cursor) & 0xFF));
    cursor += Byte.BYTES;
    valueType = ValueType.get((short) (buffer.getByte(cursor) & 0xFF));
    cursor += Byte.BYTES;
    intentValue = buffer.getShort(cursor, ByteOrder.LITTLE_ENDIAN);
    cursor += Short.BYTES;
    intent = Intent.fromProtocolValue(valueType, intentValue);
    recordVersion = buffer.getInt(cursor, ByteOrder.LITTLE_ENDIAN);
    cursor += Integer.BYTES;
    rejectionType = RejectionType.get((short) (buffer.getByte(cursor) & 0xFF));
    cursor += Byte.BYTES;
    requestStreamId = readString(buffer, cursor);
    cursor += Integer.BYTES + utf8Length(requestStreamId);
    requestId = readString(buffer, cursor);
    cursor += Integer.BYTES + utf8Length(requestId);
    rejectionReason = readString(buffer, cursor);
  }

  private short toIntentValue() {
    if (intent != null) {
      return intent.value();
    }
    return intentValue;
  }

  private static int sizedLength(final String value) {
    return Integer.BYTES + utf8Length(value);
  }

  private static int utf8Length(final String value) {
    return value == null ? 0 : value.getBytes(StandardCharsets.UTF_8).length;
  }

  private static int writeString(
      final MutableDirectBuffer buffer, final int offset, final String value) {
    final byte[] bytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
    buffer.putInt(offset, bytes.length, ByteOrder.LITTLE_ENDIAN);
    if (bytes.length > 0) {
      buffer.putBytes(offset + Integer.BYTES, bytes);
    }
    return offset + Integer.BYTES + bytes.length;
  }

  private static String readString(final DirectBuffer buffer, final int offset) {
    final int valueLength = buffer.getInt(offset, ByteOrder.LITTLE_ENDIAN);
    if (valueLength == 0) {
      return "";
    }
    final byte[] bytes = new byte[valueLength];
    buffer.getBytes(offset + Integer.BYTES, bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final RecordMetadata that = (RecordMetadata) o;
    return requestId.equals(that.requestId)
        && requestStreamId.equals(that.requestStreamId)
        && intentValue == that.intentValue
        && valueType == that.valueType
        && recordType == that.recordType
        && rejectionType == that.rejectionType
        && rejectionReason.equals(that.rejectionReason)
        && recordVersion == that.recordVersion;
  }

  @Override
  public String toString() {
    // The toString is intentionally cut-down to the only important properties for debugging
    // (mostly for tests).
    // If the record is already written to the log (in production) we have other ways to make
    // it readable again.
    final var builder =
        new StringBuilder(
            "RecordMetadata{"
                + "recordType="
                + recordType
                + ", valueType="
                + valueType
                + ", intent="
                + intent);
    if (!rejectionType.equals(RejectionType.NULL_VAL)) {
      builder.append(", rejectionType=").append(rejectionType);
    }
    if (!rejectionReason.isEmpty()) {
      builder.append(", rejectionReason=").append(rejectionReason);
    }

    builder.append('}');
    return builder.toString();
  }
}
