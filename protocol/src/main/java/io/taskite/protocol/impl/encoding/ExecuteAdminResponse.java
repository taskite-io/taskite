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
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class ExecuteAdminResponse implements BufferWriter {
  private final DirectBuffer value = new UnsafeBuffer(0, 0);
  private int partitionId = 0;
  private String clusterId = "";
  private ValueType valueType = ValueType.NULL_VAL;
  private Intent intent = Intent.UNKNOWN;
  private RecordType recordType = RecordType.EVENT;
  private RejectionType rejectionType = RejectionType.NULL_VAL;
  private String rejectionReason = "";

  public int getPartitionId() {
    return partitionId;
  }

  public ExecuteAdminResponse setPartitionId(final int value) {
    partitionId = value;
    return this;
  }

  public String getClusterId() {
    return clusterId;
  }

  public ExecuteAdminResponse setClusterId(final String value) {
    clusterId = Objects.requireNonNull(value);
    return this;
  }

  public ValueType getValueType() {
    return valueType;
  }

  public ExecuteAdminResponse setValueType(final ValueType value) {
    valueType = Objects.requireNonNull(value);
    return this;
  }

  public Intent getIntent() {
    return intent;
  }

  public ExecuteAdminResponse setIntent(final Intent value) {
    intent = Objects.requireNonNull(value);
    return this;
  }

  public RecordType getRecordType() {
    return recordType;
  }

  public ExecuteAdminResponse setRecordType(final RecordType value) {
    recordType = Objects.requireNonNull(value);
    return this;
  }

  public RejectionType getRejectionType() {
    return rejectionType;
  }

  public ExecuteAdminResponse setRejectionType(final RejectionType value) {
    rejectionType = Objects.requireNonNull(value);
    return this;
  }

  public String getRejectionReason() {
    return rejectionReason;
  }

  public ExecuteAdminResponse setRejectionReason(final String value) {
    rejectionReason = Objects.requireNonNull(value);
    return this;
  }

  public DirectBuffer getValue() {
    return value;
  }

  public ExecuteAdminResponse setValue(
      final DirectBuffer buffer, final int offset, final int length) {
    value.wrap(buffer, offset, length);
    return this;
  }

  private void validate() {
    if (clusterId == null
        || clusterId.isBlank()
        || partitionId <= 0
        || valueType != ValueType.BROKER_PARTITION_CONFIGURATION
            && valueType != ValueType.BROKER_PARTITION_OPERATION
        || intent == null
        || intent == Intent.UNKNOWN
        || Intent.fromProtocolValue(valueType, intent.value()) != intent
        || recordType != RecordType.EVENT && recordType != RecordType.COMMAND_REJECTION
        || recordType == RecordType.EVENT && !intent.isEvent()
        || recordType == RecordType.COMMAND_REJECTION
            && (intent.isEvent()
                || rejectionType == RejectionType.NULL_VAL
                || rejectionType == RejectionType.SBE_UNKNOWN)) {
      throw new IllegalArgumentException("invalid administration identity, value type or intent");
    }
  }

  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    final byte[] bytes = new byte[length];
    buffer.getBytes(offset, bytes);
    AdminFrameValidator.frame(
            bytes,
            ExecuteAdminResponseDecoder.TEMPLATE_ID,
            ExecuteAdminResponseDecoder.BLOCK_LENGTH,
            ExecuteAdminResponseDecoder.SCHEMA_VERSION)
        .data(3)
        .finish();
    final var body =
        new ExecuteAdminResponseDecoder()
            .wrap(
                new UnsafeBuffer(bytes),
                MessageHeaderDecoder.ENCODED_LENGTH,
                ExecuteAdminResponseDecoder.BLOCK_LENGTH,
                ExecuteAdminResponseDecoder.SCHEMA_VERSION);
    partitionId = body.partitionId();
    valueType = body.valueType();
    intent = Intent.fromProtocolValue(valueType, body.intent());
    recordType = body.recordType();
    rejectionType = body.rejectionType();
    clusterId = body.clusterId();
    rejectionReason = body.rejectionReason();
    final byte[] decoded = new byte[body.valueLength()];
    body.getValue(decoded, 0, decoded.length);
    value.wrap(decoded);
    validate();
  }

  @Override
  public int getLength() {
    return MessageHeaderEncoder.ENCODED_LENGTH
        + ExecuteAdminResponseEncoder.BLOCK_LENGTH
        + ExecuteAdminResponseEncoder.clusterIdHeaderLength()
        + ExecuteAdminResponseEncoder.rejectionReasonHeaderLength()
        + ExecuteAdminResponseEncoder.valueHeaderLength()
        + clusterId.getBytes(StandardCharsets.UTF_8).length
        + rejectionReason.getBytes(StandardCharsets.UTF_8).length
        + value.capacity();
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    validate();
    final var body = new ExecuteAdminResponseEncoder();
    body.wrapAndApplyHeader(buffer, offset, new MessageHeaderEncoder())
        .partitionId(partitionId)
        .valueType(valueType)
        .intent(intent.value())
        .recordType(recordType)
        .rejectionType(rejectionType)
        .clusterId(clusterId)
        .rejectionReason(rejectionReason)
        .putValue(value, 0, value.capacity());
  }
}
