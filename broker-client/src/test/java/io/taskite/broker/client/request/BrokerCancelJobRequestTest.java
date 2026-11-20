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
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class BrokerCancelJobRequestTest {

  @Test
  void serializesKeyedCancelCommandWithTenant() {
    final long jobKey = Protocol.encodePartitionId(3, 123);
    final var request = new BrokerCancelJobRequest(jobKey);
    request.setTenantId("tenant-a");
    request.setClusterId("cluster-a");
    request.serializeValue();
    final byte[] bytes = request.serializeRequest();
    final var command = new ExecuteCommandRequest();
    command.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
    final var record = new JobRecord();
    record.wrap(command.getValue());

    assertEquals(ValueType.JOB, command.getValueType());
    assertEquals(JobIntent.CANCEL, command.getIntent());
    assertEquals(jobKey, command.getKey());
    assertEquals(3, command.getPartitionId());
    assertEquals("cluster-a", command.getClusterId());
    assertEquals("tenant-a", record.getTenantId());
    assertTrue(request.addressesSpecificPartition());
  }

  @Test
  void decodesCommittedCancellationResponse() {
    final long jobKey = Protocol.encodePartitionId(3, 123);
    final var request = new BrokerCancelJobRequest(jobKey);
    final var value = new JobRecord().setTenantId("tenant-a");

    final var valueBuffer = BufferUtil.wrapString(value.toJson());
    final var response =
        new ExecuteCommandResponse()
            .setPartitionId(3)
            .setKey(jobKey)
            .setValueType(ValueType.JOB)
            .setIntent(JobIntent.CANCELED)
            .setRecordType(RecordType.EVENT)
            .setValue(valueBuffer, 0, valueBuffer.capacity());
    final var bytes = new UnsafeBuffer(new byte[response.getLength()]);
    response.write(bytes, 0);

    final var decoded = request.getResponse(bytes);

    assertEquals(3, decoded.getPartitionId());
    assertEquals("tenant-a", decoded.getResponse().getTenantId());
    assertEquals(jobKey, decoded.getKey());
  }
}
