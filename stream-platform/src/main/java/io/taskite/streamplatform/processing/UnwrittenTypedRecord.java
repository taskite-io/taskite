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
package io.taskite.streamplatform.processing;

import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.logstream.LogAppendEntry;
import org.agrona.concurrent.UnsafeBuffer;

/** Typed command that is processed before its accumulated log batch receives positions. */
final class UnwrittenTypedRecord implements TypedRecord<UnifiedRecordValue> {
  private final int partitionId;
  private final long key;
  private final RecordMetadata metadata;
  private final UnsafeBuffer valueBuffer;
  private final UnifiedRecordValue value;

  UnwrittenTypedRecord(
      final int partitionId, final LogAppendEntry entry, final TypedEventRegistry eventRegistry) {
    this.partitionId = partitionId;
    key = entry.key();
    metadata = entry.recordMetadata().copy();
    final UnifiedRecordValue source = entry.recordValue();
    final byte[] bytes = new byte[source.getLength()];
    valueBuffer = new UnsafeBuffer(bytes);
    source.write(valueBuffer, 0);
    value = eventRegistry.create(metadata.getValueType(), metadata.getIntent());
    value.wrap(valueBuffer, 0, bytes.length);
  }

  @Override
  public long getPosition() {
    throw new UnsupportedOperationException("Unwritten commands do not have a log position");
  }

  @Override
  public long getTimestamp() {
    throw new UnsupportedOperationException("Unwritten commands do not have a log timestamp");
  }

  @Override
  public long getKey() {
    return key;
  }

  @Override
  public Intent getIntent() {
    return metadata.getIntent();
  }

  @Override
  public int getPartitionId() {
    return partitionId;
  }

  @Override
  public RecordType getRecordType() {
    return metadata.getRecordType();
  }

  @Override
  public RejectionType getRejectionType() {
    return metadata.getRejectionType();
  }

  @Override
  public String getRejectionReason() {
    return metadata.getRejectionReason();
  }

  @Override
  public int getRecordVersion() {
    return metadata.getRecordVersion();
  }

  @Override
  public ValueType getValueType() {
    return metadata.getValueType();
  }

  @Override
  public UnifiedRecordValue getValue() {
    return value;
  }

  @Override
  public String getRequestStreamId() {
    return metadata.getRequestStreamId();
  }

  @Override
  public String getRequestId() {
    return metadata.getRequestId();
  }

  @Override
  public int getLength() {
    return value.getLength();
  }

  @Override
  public String toJson() {
    return value.toJson();
  }
}
