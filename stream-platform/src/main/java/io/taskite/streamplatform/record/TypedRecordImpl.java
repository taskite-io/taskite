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
package io.taskite.streamplatform.record;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.taskite.protocol.record.*;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.streamplatform.logstream.LoggedEvent;

public class TypedRecordImpl implements TypedRecord<UnifiedRecordValue> {

  private final int partitionId;
  private LoggedEvent rawEvent;
  private RecordMetadata metadata;
  private UnifiedRecordValue value;

  public TypedRecordImpl(final int partitionId) {
    this.partitionId = partitionId;
  }

  public void wrap(
      final LoggedEvent rawEvent, final RecordMetadata metadata, final UnifiedRecordValue value) {
    this.rawEvent = rawEvent;
    this.metadata = metadata;
    this.value = value;
  }

  @JsonIgnore
  public RecordMetadata getMetadata() {
    return metadata;
  }

  @Override
  public long getPosition() {
    return rawEvent.getPosition();
  }

  @Override
  public long getTimestamp() {
    return rawEvent.getTimestamp();
  }

  public long getSourceEventPosition() {
    return rawEvent.getSourceEventPosition();
  }

  public boolean shouldSkipProcessing() {
    return rawEvent.shouldSkipProcessing();
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
  public long getKey() {
    return rawEvent.key();
  }

  @Override
  public UnifiedRecordValue getValue() {
    return value;
  }

  @Override
  @JsonIgnore
  public String getRequestStreamId() {
    return metadata.getRequestStreamId();
  }

  @Override
  @JsonIgnore
  public String getRequestId() {
    return metadata.getRequestId();
  }

  @Override
  @JsonIgnore
  public int getLength() {
    return value == null ? 0 : value.getLength();
  }

  @Override
  public String toJson() {
    return value == null ? "{}" : value.toJson();
  }

  @Override
  public String toString() {
    return "TypedRecordImpl{" + "metadata=" + metadata + ", value=" + '}';
  }
}
