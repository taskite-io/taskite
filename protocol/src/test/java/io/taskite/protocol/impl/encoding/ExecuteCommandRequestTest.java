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

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.taskite.protocol.record.ExecuteCommandRequestEncoder;
import io.taskite.protocol.record.MessageHeaderEncoder;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import java.nio.charset.StandardCharsets;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class ExecuteCommandRequestTest {
  @Test
  void roundTripsClusterIdAfterCommandValue() {
    final byte[] value = "payload".getBytes(StandardCharsets.UTF_8);
    final ExecuteCommandRequest request =
        new ExecuteCommandRequest()
            .setKey(42)
            .setPartitionId(3)
            .setValueType(ValueType.JOB)
            .setIntent(JobIntent.COMPLETE)
            .setValue(new UnsafeBuffer(value), 0, value.length);
    request.setClusterId("cluster-a");
    final byte[] encoded = new byte[request.getLength()];
    request.write(new UnsafeBuffer(encoded), 0);

    final ExecuteCommandRequest decoded = new ExecuteCommandRequest();
    decoded.wrap(new UnsafeBuffer(encoded), 0, encoded.length);

    assertEquals("cluster-a", decoded.getClusterId());
    assertEquals(3, decoded.getPartitionId());
    assertEquals(42, decoded.getKey());
    assertEquals("payload", decoded.getValue().getStringWithoutLengthUtf8(0, value.length));
  }

  @Test
  void decodesVersionSevenRequestWithoutClusterId() {
    final byte[] value = "payload".getBytes(StandardCharsets.UTF_8);
    final UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final ExecuteCommandRequestEncoder body = new ExecuteCommandRequestEncoder();
    header
        .wrap(buffer, 0)
        .blockLength(body.sbeBlockLength())
        .templateId(body.sbeTemplateId())
        .schemaId(body.sbeSchemaId())
        .version(7);
    body.wrap(buffer, header.encodedLength())
        .partitionId(3)
        .key(42)
        .valueType(ValueType.JOB)
        .intent(JobIntent.COMPLETE.value())
        .putValue(value, 0, value.length);
    final int encodedLength = header.encodedLength() + body.encodedLength();

    final ExecuteCommandRequest decoded = new ExecuteCommandRequest();
    decoded.wrap(buffer, 0, encodedLength);

    assertEquals("", decoded.getClusterId());
    assertEquals("payload", decoded.getValue().getStringWithoutLengthUtf8(0, value.length));
  }
}
