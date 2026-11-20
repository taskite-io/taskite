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
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.util.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class BrokerQueryJobRequestTest {
  @Test
  void serializesTenantQueryToThePartitionEncodedInJobKey() {
    final long key = Protocol.encodePartitionId(7, 42);
    final var request = new BrokerQueryJobRequest(key);
    request.setTenantId("tenant-a");
    request.setClusterId("cluster-a");
    request.serializeValue();
    final byte[] bytes = request.serializeRequest();
    final var decoded = new ExecuteCommandRequest();
    decoded.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
    final var value = new JobRecord();
    value.wrap(decoded.getValue());

    assertTrue(request.addressesSpecificPartition());
    assertEquals(7, decoded.getPartitionId());
    assertEquals(key, decoded.getKey());
    assertEquals("cluster-a", decoded.getClusterId());
    assertEquals(ValueType.JOB, decoded.getValueType());
    assertEquals(JobIntent.QUERY, decoded.getIntent());
    assertEquals("tenant-a", value.getTenantId());
  }

  @Test
  void decodesCompleteQueriedJobRecord() {
    final long key = Protocol.encodePartitionId(3, 123);
    final var value =
        new JobRecord()
            .setJobType("payment")
            .setWorker("worker")
            .setRetries(4)
            .setDeadline(10_000)
            .setVariablesJson("{\"amount\":42}")
            .setTenantId("tenant-a");
    final var buffer = BufferUtil.wrapString(value.toJson());
    final var response =
        new ExecuteCommandResponse()
            .setPartitionId(3)
            .setKey(key)
            .setValueType(ValueType.JOB)
            .setIntent(JobIntent.QUERIED)
            .setRecordType(RecordType.EVENT)
            .setValue(buffer, 0, buffer.capacity());
    final var bytes = new UnsafeBuffer(new byte[response.getLength()]);
    response.write(bytes, 0);

    final var decoded = new BrokerQueryJobRequest(key).getResponse(bytes);
    assertEquals(value.toJson(), decoded.getResponse().toJson());
    assertEquals(key, decoded.getKey());
  }
}
