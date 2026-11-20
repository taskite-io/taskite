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
package io.taskite.protocol.impl.record.value.placementdriver.commanddistribution;

import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.value.placementdriver.commanddistribution.CommandDistributionRecordValue;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Established distribution used by events, persisted state and commands targeting Broker
 * partitions. The nested value is the business command, not a {@link
 * CommandDistributionRequestRecord}.
 */
public class CommandDistributionRecord<T extends UnifiedRecordValue> extends UnifiedRecordValue
    implements CommandDistributionRecordValue {
  private static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
  private static final int DISTRIBUTION_KEY_OFFSET = 0;
  private static final int PARTITION_ID_OFFSET = DISTRIBUTION_KEY_OFFSET + Long.BYTES;
  private static final int VALUE_TYPE_OFFSET = PARTITION_ID_OFFSET + Integer.BYTES;
  private static final int INTENT_OFFSET = VALUE_TYPE_OFFSET + Short.BYTES;
  private static final int CLUSTER_ID_LENGTH_OFFSET = INTENT_OFFSET + Short.BYTES;
  private static final int CLUSTER_ID_OFFSET = CLUSTER_ID_LENGTH_OFFSET + Integer.BYTES;

  private long distributionKey;
  private String clusterId = "";
  private int partitionId;
  private ValueType valueType = ValueType.NULL_VAL;
  private short intent = Intent.NULL_VAL;
  private T value;
  private long generation;
  private long lastRedistributedAt;
  private long dispatchAttemptId;
  private long nextAttemptAtEpochMillis;

  public long getDispatchAttemptId() {
    return dispatchAttemptId;
  }

  public CommandDistributionRecord<T> setDispatchAttemptId(final long dispatchAttemptId) {
    this.dispatchAttemptId = dispatchAttemptId;
    return this;
  }

  public long getNextAttemptAtEpochMillis() {
    return nextAttemptAtEpochMillis;
  }

  public CommandDistributionRecord<T> setNextAttemptAtEpochMillis(
      final long nextAttemptAtEpochMillis) {
    this.nextAttemptAtEpochMillis = nextAttemptAtEpochMillis;
    return this;
  }

  public long getGeneration() {
    return generation;
  }

  public CommandDistributionRecord<T> setGeneration(final long generation) {
    this.generation = generation;
    return this;
  }

  public long getLastRedistributedAt() {
    return lastRedistributedAt;
  }

  public CommandDistributionRecord<T> setLastRedistributedAt(final long lastRedistributedAt) {
    this.lastRedistributedAt = lastRedistributedAt;
    return this;
  }

  @Override
  public long getDistributionKey() {
    return distributionKey;
  }

  public CommandDistributionRecord<T> setDistributionKey(final long distributionKey) {
    this.distributionKey = distributionKey;
    return this;
  }

  @Override
  public String getClusterId() {
    return clusterId;
  }

  public CommandDistributionRecord<T> setClusterId(final String clusterId) {
    this.clusterId = clusterId == null ? "" : clusterId;
    return this;
  }

  @Override
  public int getPartitionId() {
    return partitionId;
  }

  public CommandDistributionRecord<T> setPartitionId(final int partitionId) {
    this.partitionId = partitionId;
    return this;
  }

  @Override
  public ValueType getValueType() {
    return valueType;
  }

  public CommandDistributionRecord<T> setValueType(final ValueType valueType) {
    this.valueType = valueType == null ? ValueType.NULL_VAL : valueType;
    return this;
  }

  @Override
  public short getIntent() {
    return intent;
  }

  public CommandDistributionRecord<T> setIntent(final Intent intent) {
    this.intent = intent == null ? Intent.NULL_VAL : intent.value();
    return this;
  }

  public CommandDistributionRecord<T> setIntent(final short intent) {
    this.intent = intent;
    return this;
  }

  @Override
  public T getValue() {
    return value;
  }

  public CommandDistributionRecord<T> setValue(final T value) {
    this.value = value;
    return this;
  }

  @Override
  public int getLength() {
    return valueOffset() + valueLength() + Long.BYTES * 4;
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    final byte[] clusterIdBytes = clusterIdBytes();
    buffer.putLong(offset + DISTRIBUTION_KEY_OFFSET, distributionKey, BYTE_ORDER);
    buffer.putInt(offset + PARTITION_ID_OFFSET, partitionId, BYTE_ORDER);
    buffer.putShort(offset + VALUE_TYPE_OFFSET, valueType.value(), BYTE_ORDER);
    buffer.putShort(offset + INTENT_OFFSET, intent, BYTE_ORDER);
    buffer.putInt(offset + CLUSTER_ID_LENGTH_OFFSET, clusterIdBytes.length, BYTE_ORDER);
    buffer.putBytes(offset + CLUSTER_ID_OFFSET, clusterIdBytes);
    final int valueLengthOffset = offset + CLUSTER_ID_OFFSET + clusterIdBytes.length;
    buffer.putInt(valueLengthOffset, valueLength(), BYTE_ORDER);
    if (value != null) {
      value.write(buffer, valueLengthOffset + Integer.BYTES);
    }
    final int extensionOffset = valueLengthOffset + Integer.BYTES + valueLength();
    buffer.putLong(extensionOffset, generation, BYTE_ORDER);
    buffer.putLong(extensionOffset + Long.BYTES, lastRedistributedAt, BYTE_ORDER);
    buffer.putLong(extensionOffset + Long.BYTES * 2, dispatchAttemptId, BYTE_ORDER);
    buffer.putLong(extensionOffset + Long.BYTES * 3, nextAttemptAtEpochMillis, BYTE_ORDER);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    distributionKey = buffer.getLong(offset + DISTRIBUTION_KEY_OFFSET, BYTE_ORDER);
    partitionId = buffer.getInt(offset + PARTITION_ID_OFFSET, BYTE_ORDER);
    valueType = ValueType.get(buffer.getShort(offset + VALUE_TYPE_OFFSET, BYTE_ORDER));
    intent = buffer.getShort(offset + INTENT_OFFSET, BYTE_ORDER);
    final int clusterIdLength = buffer.getInt(offset + CLUSTER_ID_LENGTH_OFFSET, BYTE_ORDER);
    final byte[] clusterIdBytes = new byte[clusterIdLength];
    buffer.getBytes(offset + CLUSTER_ID_OFFSET, clusterIdBytes);
    clusterId = new String(clusterIdBytes, StandardCharsets.UTF_8);
    final int valueLengthOffset = offset + CLUSTER_ID_OFFSET + clusterIdLength;
    final int valueLength = buffer.getInt(valueLengthOffset, BYTE_ORDER);
    final int extensionOffset = valueLengthOffset + Integer.BYTES + Math.max(0, valueLength);
    if (extensionOffset + Long.BYTES * 4 > offset + length) {
      throw new IllegalArgumentException(
          "Command distribution record is missing scheduling fields");
    }
    generation = buffer.getLong(extensionOffset, BYTE_ORDER);
    lastRedistributedAt = buffer.getLong(extensionOffset + Long.BYTES, BYTE_ORDER);
    dispatchAttemptId = buffer.getLong(extensionOffset + Long.BYTES * 2, BYTE_ORDER);
    nextAttemptAtEpochMillis = buffer.getLong(extensionOffset + Long.BYTES * 3, BYTE_ORDER);
    if (valueLength <= 0) {
      value = null;
      return;
    }
    final UnifiedRecordValue decodedValue = valueFor(valueType);
    decodedValue.wrap(buffer, valueLengthOffset + Integer.BYTES, valueLength);
    value = (T) decodedValue;
  }

  private int valueOffset() {
    return CLUSTER_ID_OFFSET + clusterIdBytes().length + Integer.BYTES;
  }

  private int valueLength() {
    return value == null ? 0 : value.getLength();
  }

  private byte[] clusterIdBytes() {
    return clusterId == null || clusterId.isEmpty()
        ? new byte[0]
        : clusterId.getBytes(StandardCharsets.UTF_8);
  }

  private static UnifiedRecordValue valueFor(final ValueType valueType) {
    return switch (valueType) {
      case JOB_SUBSCRIPTION -> new JobSubscriptionRecord();
      case JOB_SUBSCRIPTION_DELETION -> new JobSubscriptionDeletionRecord();
      default -> new UnifiedRecordValue();
    };
  }
}
