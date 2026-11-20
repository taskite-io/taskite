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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.encoding.ExecuteCommandRequest;
import io.taskite.protocol.impl.encoding.ExecuteCommandResponse;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.util.BufferUtil;
import java.util.Map;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class BrokerFailJobRequestTest {

  @Test
  void serializesFailureToThePartitionEncodedInJobKey() {
    final long jobKey = Protocol.encodePartitionId(7, 42);
    final var request =
        new BrokerFailJobRequest(jobKey, 2, "temporary failure", 500, Map.of("attempt", 2));
    request.setTenantId("tenant-a");
    request.setClusterId("cluster-a");
    request.serializeValue();
    final byte[] bytes = request.serializeRequest();
    final var command = new ExecuteCommandRequest();
    command.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
    final var value = new JobRecord();
    value.wrap(command.getValue());

    assertTrue(request.addressesSpecificPartition());
    assertEquals(7, command.getPartitionId());
    assertEquals(jobKey, command.getKey());
    assertEquals("cluster-a", command.getClusterId());
    assertEquals(ValueType.JOB, command.getValueType());
    assertEquals(JobIntent.FAIL, command.getIntent());
    assertEquals(2, value.getRetries());
    assertEquals("temporary failure", value.getErrorMessage());
    assertEquals(500, value.getRetryBackoff());
    assertEquals(Map.of("attempt", 2), value.getVariables());
    assertEquals("tenant-a", value.getTenantId());
  }

  @Test
  void decodesFailedJobResponse() {
    final long jobKey = Protocol.encodePartitionId(3, 123);
    final var value = new JobRecord().setRetries(1).setTenantId("tenant-a");
    final var valueBuffer = BufferUtil.wrapString(value.toJson());
    final var response =
        new ExecuteCommandResponse()
            .setPartitionId(3)
            .setKey(jobKey)
            .setValueType(ValueType.JOB)
            .setIntent(JobIntent.FAILED)
            .setRecordType(RecordType.EVENT)
            .setValue(valueBuffer, 0, valueBuffer.capacity());
    final var bytes = new UnsafeBuffer(new byte[response.getLength()]);
    response.write(bytes, 0);

    final var decoded = new BrokerFailJobRequest(jobKey, 1, "", 0, Map.of()).getResponse(bytes);

    assertEquals(value.toJson(), decoded.getResponse().toJson());
    assertEquals(jobKey, decoded.getKey());
  }
}
