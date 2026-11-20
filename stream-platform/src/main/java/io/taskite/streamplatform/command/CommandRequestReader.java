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

import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.encoding.ExecuteCommandRequest;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.util.Objects;
import org.agrona.DirectBuffer;

/**
 * Decodes an SBE-framed client {@code ExecuteCommandRequest} into an in-memory {@link LoggedEvent}
 * command record. The concrete value is created through the {@link ValueType} registry, keeping
 * this class limited to wire decoding. The caller assigns the request correlation id because it is
 * not carried on the wire.
 */
public final class CommandRequestReader {
  private static final String INVALID_KEY =
      "Broker command key must be nonnegative, got %d for clusterKey %d, partition %d";
  private static final String KEY_IDENTITY_MISMATCH =
      "Broker command key %d does not belong to clusterKey %d, partition %d";
  private final TypedEventRegistry registry;
  private final long clusterKey;
  private final ExecuteCommandRequest request = new ExecuteCommandRequest();
  private final RecordMetadata metadata = new RecordMetadata();
  private long key;
  private UnifiedRecordValue value = new UnifiedRecordValue();

  public CommandRequestReader(final TypedEventRegistry registry, final long clusterKey) {
    this.registry = Objects.requireNonNull(registry, "registry");
    Protocol.validateClusterKey(clusterKey);
    this.clusterKey = clusterKey;
  }

  void reset() {
    request.reset();
    metadata.reset();
    key = 0L;
    value = new UnifiedRecordValue();
  }

  void wrap(final DirectBuffer buffer, final int offset, final int length) {
    request.wrap(buffer, offset, length);
    if (request.getKey() < 0) {
      throw new IllegalArgumentException(
          INVALID_KEY.formatted(request.getKey(), clusterKey, request.getPartitionId()));
    }
    Protocol.validatePartitionId(request.getPartitionId());
    if (request.getKey() > 0
        && request.getValueType() != ValueType.COMMAND_DISTRIBUTION
        && (Protocol.decodeClusterKey(request.getKey()) != clusterKey
            || Protocol.decodePartitionId(request.getKey()) != request.getPartitionId())) {
      throw new IllegalArgumentException(
          KEY_IDENTITY_MISMATCH.formatted(request.getKey(), clusterKey, request.getPartitionId()));
    }

    key =
        request.getKey() > 0 && request.getValueType() != ValueType.COMMAND_DISTRIBUTION
            ? request.getKey()
            : Protocol.encodeKey(clusterKey, request.getPartitionId(), 0);
    metadata.reset();
    metadata
        .recordType(RecordType.COMMAND)
        .valueType(request.getValueType())
        .intent(request.getIntent())
        .recordVersion(LoggedEvent.DEFAULT_RECORD_VERSION);
    value = readValue(request.getValueType(), request.getIntent(), request.getValue());
  }

  long key() {
    return key;
  }

  RecordMetadata metadata() {
    return metadata;
  }

  UnifiedRecordValue value() {
    return value;
  }

  LoggedEvent record() {
    return LoggedEvent.ofRecord(key, metadata, value);
  }

  private UnifiedRecordValue readValue(
      final ValueType valueType, final Intent intent, final DirectBuffer valueBuffer) {
    final UnifiedRecordValue decodedValue = registry.create(valueType, intent);
    decodedValue.wrap(valueBuffer, 0, valueBuffer.capacity());
    return decodedValue;
  }
}
