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
import io.taskite.protocol.record.value.broker.TenantOwned;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public final class JobSubscriptionDeletionRecord extends UnifiedRecordValue implements TenantOwned {
  private static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

  private String clusterId = "";
  private String tenantId = TenantOwned.DEFAULT_TENANT_IDENTIFIER;
  private long subscriptionKey;

  public String getClusterId() {
    return clusterId;
  }

  public JobSubscriptionDeletionRecord setClusterId(final String clusterId) {
    this.clusterId = clusterId == null ? "" : clusterId;
    return this;
  }

  @Override
  public String getTenantId() {
    return tenantId;
  }

  public JobSubscriptionDeletionRecord setTenantId(final String tenantId) {
    this.tenantId =
        tenantId == null || tenantId.isBlank() ? TenantOwned.DEFAULT_TENANT_IDENTIFIER : tenantId;
    return this;
  }

  public long getSubscriptionKey() {
    return subscriptionKey;
  }

  public JobSubscriptionDeletionRecord setSubscriptionKey(final long subscriptionKey) {
    this.subscriptionKey = subscriptionKey;
    return this;
  }

  @Override
  public int getLength() {
    return fieldLength(clusterId) + fieldLength(tenantId) + Long.BYTES;
  }

  @Override
  public void write(final MutableDirectBuffer buffer, int offset) {
    offset = writeString(buffer, offset, clusterId);
    offset = writeString(buffer, offset, tenantId);
    buffer.putLong(offset, subscriptionKey, BYTE_ORDER);
  }

  @Override
  public void wrap(final DirectBuffer buffer, int offset, final int length) {
    final StringValue decodedClusterId = readString(buffer, offset);
    clusterId = decodedClusterId.value();
    offset = decodedClusterId.nextOffset();
    final StringValue decodedTenantId = readString(buffer, offset);
    tenantId = decodedTenantId.value();
    offset = decodedTenantId.nextOffset();
    subscriptionKey = buffer.getLong(offset, BYTE_ORDER);
  }

  private static int fieldLength(final String value) {
    return Integer.BYTES + bytes(value).length;
  }

  private static int writeString(
      final MutableDirectBuffer buffer, final int offset, final String value) {
    final byte[] bytes = bytes(value);
    buffer.putInt(offset, bytes.length, BYTE_ORDER);
    buffer.putBytes(offset + Integer.BYTES, bytes);
    return offset + Integer.BYTES + bytes.length;
  }

  private static StringValue readString(final DirectBuffer buffer, final int offset) {
    final int length = buffer.getInt(offset, BYTE_ORDER);
    final byte[] bytes = new byte[length];
    buffer.getBytes(offset + Integer.BYTES, bytes);
    return new StringValue(
        new String(bytes, StandardCharsets.UTF_8), offset + Integer.BYTES + length);
  }

  private static byte[] bytes(final String value) {
    return value == null || value.isEmpty() ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
  }

  private record StringValue(String value, int nextOffset) {}
}
