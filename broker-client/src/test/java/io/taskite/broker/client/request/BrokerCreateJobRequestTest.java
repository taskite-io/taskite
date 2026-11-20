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
package io.taskite.broker.client.request;

import static org.junit.jupiter.api.Assertions.*;

import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.encoding.ExecuteCommandRequest;
import io.taskite.protocol.impl.encoding.ExecuteCommandResponse;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.util.BufferUtil;
import java.util.Map;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class BrokerCreateJobRequestTest {
  @ParameterizedTest
  @ValueSource(strings = {"", "timer:42:0:0"})
  void decodesOlderJobRecordsWithoutRetainingRemovedMetadata(final String retiredValue) {
    final var value =
        new JobRecord()
            .setJobType("work")
            .setTenantId("tenant")
            .setVariablesMap(Map.of("amount", 42));
    final var legacyJson =
        "{\"timerTriggerId\":\"" + retiredValue + "\"," + value.toJson().substring(1);
    final var decoded = new JobRecord();
    decoded.wrap(BufferUtil.wrapString(legacyJson));
    assertEquals(value.toJson(), decoded.toJson());
    assertEquals(Map.of("amount", 42), decoded.getVariables());
    assertEquals(value.toJson(), decoded.copy().toJson());
  }

  @Test
  void decodesCreatedResponseWithActualProcessMetadata() {
    final var request = new BrokerCreateJobRequest("demo", 3, new JobRecord().setJobType("work"));
    final long key = Protocol.encodePartitionId(3, 123);
    final var value = new JobRecord().setJobType("selected").setTenantId("tenant-a");

    final var valueBuffer = BufferUtil.wrapString(value.toJson());
    final var response =
        new ExecuteCommandResponse()
            .setPartitionId(3)
            .setKey(key)
            .setValueType(ValueType.JOB)
            .setIntent(JobIntent.CREATED)
            .setRecordType(RecordType.EVENT)
            .setValue(valueBuffer, 0, valueBuffer.capacity());
    final var bytes = new UnsafeBuffer(new byte[response.getLength()]);
    response.write(bytes, 0);

    final var decoded = request.getResponse(bytes);
    assertEquals(3, decoded.getPartitionId());
    assertEquals(key, decoded.getKey());
    assertEquals(value.toJson(), decoded.getResponse().toJson());
  }

  @Test
  void decodesBrokerNotFoundRejectionWithReason() {
    final var request = new BrokerCreateJobRequest("demo", 3, new JobRecord().setJobType("work"));
    final var reason = BufferUtil.wrapString("Job subscription not found: missing");
    final var response =
        new ExecuteCommandResponse()
            .setValueType(ValueType.JOB)
            .setIntent(JobIntent.CREATE)
            .setRecordType(RecordType.COMMAND_REJECTION)
            .setRejectionType(RejectionType.NOT_FOUND)
            .setRejectionReason(reason, 0, reason.capacity());
    final var bytes = new UnsafeBuffer(new byte[response.getLength()]);
    response.write(bytes, 0);

    final var decoded = request.getResponse(bytes);
    assertEquals(RejectionType.NOT_FOUND, decoded.getRejection().type());
    assertEquals("Job subscription not found: missing", decoded.getRejection().reason());
  }

  @Test
  void serializesFixedPartitionSubscriptionAndJobBody() {
    final var value =
        new JobRecord()
            .setSubscriptionKey(42)
            .setJobType("work")
            .setTenantId("tenant")
            .setVariablesMap(Map.of("orderId", 42));
    final var request = new BrokerCreateJobRequest("demo", 2, value);
    request.serializeValue();
    final var bytes = request.serializeRequest();
    final var wire = new ExecuteCommandRequest();
    wire.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
    assertEquals(ValueType.JOB, wire.getValueType());
    assertEquals(JobIntent.CREATE, wire.getIntent());
    assertEquals("demo", wire.getClusterId());
    assertEquals(2, wire.getPartitionId());
    final var decoded = new JobRecord();
    decoded.wrap(wire.getValue());
    assertEquals(value.toJson(), decoded.toJson());
    assertFalse(request.allowsTransportRetry());

    final var created = value.copy();
    final var payload = BufferUtil.wrapString(created.toJson());
    final var response =
        new ExecuteCommandResponse()
            .setPartitionId(2)
            .setKey(123)
            .setValueType(ValueType.JOB)
            .setIntent(JobIntent.CREATED)
            .setRecordType(RecordType.EVENT)
            .setValue(payload, 0, payload.capacity());
    final var buffer = new UnsafeBuffer(new byte[response.getLength()]);
    response.write(buffer, 0);
    final var result = request.getResponse(buffer);
    assertTrue(result.isResponse());
    assertEquals(123, result.getKey());
    assertEquals(created.toJson(), result.getResponse().toJson());
  }

  @Test
  void ordinaryCreatesDoNotRetryUncertainTransportResult() {
    final var request = new BrokerCreateJobRequest("demo", 2, new JobRecord().setJobType("work"));
    assertFalse(request.allowsTransportRetry());
  }

  @Test
  void createWithoutAssignedPartitionDoesNotRetry() {
    final var request = new BrokerCreateJobRequest("demo", 0, new JobRecord().setJobType("work"));
    assertFalse(request.allowsTransportRetry());
  }
}
