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
import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Input of {@code CommandDistributionIntent.START}, containing the business command to distribute.
 * The PD assigns distribution identity and scheduling fields when it creates the distribution
 * record.
 */
public final class CommandDistributionRequestRecord<T extends UnifiedRecordValue>
    extends UnifiedRecordValue {
  private static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
  private static final int VALUE_TYPE_OFFSET = 0;
  private static final int INTENT_OFFSET = VALUE_TYPE_OFFSET + Short.BYTES;
  private static final int VALUE_LENGTH_OFFSET = INTENT_OFFSET + Short.BYTES;
  private static final int VALUE_OFFSET = VALUE_LENGTH_OFFSET + Integer.BYTES;

  private ValueType valueType = ValueType.NULL_VAL;
  private short intent = Intent.NULL_VAL;
  private T value;

  public ValueType getValueType() {
    return valueType;
  }

  public CommandDistributionRequestRecord<T> setValueType(final ValueType valueType) {
    this.valueType = valueType == null ? ValueType.NULL_VAL : valueType;
    return this;
  }

  public short getIntent() {
    return intent;
  }

  public CommandDistributionRequestRecord<T> setIntent(final Intent intent) {
    this.intent = intent == null ? Intent.NULL_VAL : intent.value();
    return this;
  }

  public CommandDistributionRequestRecord<T> setIntent(final short intent) {
    this.intent = intent;
    return this;
  }

  public T getValue() {
    return value;
  }

  public CommandDistributionRequestRecord<T> setValue(final T value) {
    this.value = value;
    return this;
  }

  public CommandDistributionRequestRecord<T> setClusterId(final String clusterId) {
    if (value instanceof JobSubscriptionRecord subscription) {
      subscription.setClusterId(clusterId);
      return this;
    }
    if (value instanceof JobSubscriptionDeletionRecord deletion) {
      deletion.setClusterId(clusterId);
      return this;
    }
    throw unsupportedContextValue();
  }

  public CommandDistributionRequestRecord<T> setTenantId(final String tenantId) {
    if (value instanceof JobSubscriptionRecord subscription) {
      subscription.setTenantId(tenantId);
      return this;
    }
    if (value instanceof JobSubscriptionDeletionRecord deletion) {
      deletion.setTenantId(tenantId);
      return this;
    }
    throw unsupportedContextValue();
  }

  @Override
  public int getLength() {
    return VALUE_OFFSET + valueLength();
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    buffer.putShort(offset + VALUE_TYPE_OFFSET, valueType.value(), BYTE_ORDER);
    buffer.putShort(offset + INTENT_OFFSET, intent, BYTE_ORDER);
    buffer.putInt(offset + VALUE_LENGTH_OFFSET, valueLength(), BYTE_ORDER);
    if (value != null) {
      value.write(buffer, offset + VALUE_OFFSET);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    valueType = ValueType.get(buffer.getShort(offset + VALUE_TYPE_OFFSET, BYTE_ORDER));
    intent = buffer.getShort(offset + INTENT_OFFSET, BYTE_ORDER);
    final int valueLength = buffer.getInt(offset + VALUE_LENGTH_OFFSET, BYTE_ORDER);
    if (valueLength <= 0) {
      value = null;
      return;
    }
    final UnifiedRecordValue decodedValue = valueFor(valueType);
    decodedValue.wrap(buffer, offset + VALUE_OFFSET, valueLength);
    value = (T) decodedValue;
  }

  private int valueLength() {
    return value == null ? 0 : value.getLength();
  }

  private IllegalStateException unsupportedContextValue() {
    return new IllegalStateException(
        "Command distribution value does not support cluster/tenant context: "
            + (value == null ? "null" : value.getClass().getName()));
  }

  private static UnifiedRecordValue valueFor(final ValueType valueType) {
    return switch (valueType) {
      case JOB_SUBSCRIPTION -> new JobSubscriptionRecord();
      case JOB_SUBSCRIPTION_DELETION -> new JobSubscriptionDeletionRecord();
      default -> new UnifiedRecordValue();
    };
  }
}
