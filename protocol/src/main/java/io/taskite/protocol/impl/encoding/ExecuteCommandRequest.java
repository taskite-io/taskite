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

import static io.taskite.protocol.record.ExecuteCommandRequestEncoder.partitionIdNullValue;

import io.taskite.protocol.Protocol;
import io.taskite.protocol.record.*;
import io.taskite.protocol.record.intent.Intent;
import java.nio.charset.StandardCharsets;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class ExecuteCommandRequest {

  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

  private final ExecuteCommandRequestEncoder bodyEncoder = new ExecuteCommandRequestEncoder();
  private final ExecuteCommandRequestDecoder bodyDecoder = new ExecuteCommandRequestDecoder();
  private final DirectBuffer value = new UnsafeBuffer(0, 0);
  private String clusterId;
  private int partitionId;
  private long key;
  private ValueType valueType;
  private Intent intent;

  public ExecuteCommandRequest() {
    reset();
  }

  public ExecuteCommandRequest reset() {
    clusterId = "";
    partitionId = partitionIdNullValue();
    key = 0;
    valueType = ValueType.NULL_VAL;
    intent = Intent.UNKNOWN;
    value.wrap(0, 0);

    return this;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public ExecuteCommandRequest setPartitionId(final int partitionId) {
    this.partitionId = partitionId;
    return this;
  }

  public long getKey() {
    return key;
  }

  public ExecuteCommandRequest setKey(final long key) {
    this.key = key;
    partitionId = Protocol.decodePartitionId(key);

    return this;
  }

  public String getClusterId() {
    return clusterId;
  }

  public void setClusterId(String clusterId) {
    this.clusterId = clusterId == null ? "" : clusterId;
  }

  public ValueType getValueType() {
    return valueType;
  }

  public ExecuteCommandRequest setValueType(final ValueType valueType) {
    this.valueType = valueType;
    return this;
  }

  public Intent getIntent() {
    return intent;
  }

  public ExecuteCommandRequest setIntent(final Intent intent) {
    this.intent = intent;
    return this;
  }

  public DirectBuffer getValue() {
    return value;
  }

  public ExecuteCommandRequest setValue(
      final DirectBuffer buffer, final int offset, final int length) {
    value.wrap(buffer, offset, length);
    return this;
  }

  public void wrap(final DirectBuffer buffer, int offset, final int length) {
    reset();

    final int frameEnd = offset + length;

    headerDecoder.wrap(buffer, offset);

    offset += headerDecoder.encodedLength();

    bodyDecoder.wrap(buffer, offset, headerDecoder.blockLength(), headerDecoder.version());

    partitionId = bodyDecoder.partitionId();
    key = bodyDecoder.key();
    valueType = bodyDecoder.valueType();
    intent = Intent.fromProtocolValue(valueType, bodyDecoder.intent());

    offset += bodyDecoder.sbeBlockLength();

    final int valueLength = bodyDecoder.valueLength();
    offset += ExecuteCommandRequestDecoder.valueHeaderLength();

    value.wrap(buffer, offset, valueLength);
    offset += valueLength;

    bodyDecoder.limit(offset);
    final String decodedClusterId = bodyDecoder.clusterId();
    clusterId = decodedClusterId == null ? "" : decodedClusterId;
    offset = bodyDecoder.limit();

    assert offset == frameEnd
        : "Decoder read only to position "
            + offset
            + " but expected "
            + frameEnd
            + " as final position";
  }

  public int getLength() {
    return MessageHeaderEncoder.ENCODED_LENGTH
        + ExecuteCommandRequestEncoder.BLOCK_LENGTH
        + ExecuteCommandRequestEncoder.valueHeaderLength()
        + value.capacity()
        + ExecuteCommandRequestEncoder.clusterIdHeaderLength()
        + clusterId.getBytes(StandardCharsets.UTF_8).length;
  }

  public void write(final MutableDirectBuffer buffer, int offset) {
    headerEncoder
        .wrap(buffer, offset)
        .blockLength(bodyEncoder.sbeBlockLength())
        .templateId(bodyEncoder.sbeTemplateId())
        .schemaId(bodyEncoder.sbeSchemaId())
        .version(bodyEncoder.sbeSchemaVersion());

    offset += headerEncoder.encodedLength();

    bodyEncoder
        .wrap(buffer, offset)
        .partitionId(partitionId)
        .key(key)
        .valueType(valueType)
        .intent(intent.value())
        .putValue(value, 0, value.capacity())
        .clusterId(clusterId);
  }
}
