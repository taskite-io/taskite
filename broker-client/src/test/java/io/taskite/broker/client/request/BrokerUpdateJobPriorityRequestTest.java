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
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class BrokerUpdateJobPriorityRequestTest {
  @ParameterizedTest
  @ValueSource(ints = {Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE})
  void shouldSerializeSignedPriorityToThePartitionEncodedInJobKey(final int priority) {
    // given
    final long jobKey = Protocol.encodePartitionId(7, 42);
    final var request = new BrokerUpdateJobPriorityRequest(jobKey, priority);
    request.setTenantId("tenant-a");
    request.setClusterId("cluster-a");

    // when
    request.serializeValue();
    final byte[] bytes = request.serializeRequest();
    final var command = new ExecuteCommandRequest();
    command.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
    final var value = new JobRecord();
    value.wrap(command.getValue());

    // then
    assertTrue(request.addressesSpecificPartition());
    assertEquals(7, command.getPartitionId());
    assertEquals(jobKey, command.getKey());
    assertEquals("cluster-a", command.getClusterId());
    assertEquals(ValueType.JOB, command.getValueType());
    assertEquals(JobIntent.UPDATE_PRIORITY, command.getIntent());
    assertEquals(priority, value.getPriority());
    assertEquals("tenant-a", value.getTenantId());
    assertEquals("", value.getWorkerId());
    assertEquals(0, value.getWorkerLeaseToken());
  }

  @Test
  void shouldDecodePriorityUpdatedResponseWithFullJobContext() {
    // given
    final long jobKey = Protocol.encodePartitionId(3, 123);
    final var value =
        new JobRecord()
            .setPriority(Integer.MIN_VALUE)
            .setJobType("work")
            .setRetries(2)
            .setDeadline(61_000)
            .setTenantId("tenant-a")
            .setWorkerId("owner")
            .setWorkerLeaseToken(42);
    final var valueBuffer = BufferUtil.wrapString(value.toJson());
    final var response =
        new ExecuteCommandResponse()
            .setPartitionId(3)
            .setKey(jobKey)
            .setValueType(ValueType.JOB)
            .setIntent(JobIntent.PRIORITY_UPDATED)
            .setRecordType(RecordType.EVENT)
            .setValue(valueBuffer, 0, valueBuffer.capacity());
    final var bytes = new UnsafeBuffer(new byte[response.getLength()]);
    response.write(bytes, 0);

    // when
    final var decoded = new BrokerUpdateJobPriorityRequest(jobKey, 10).getResponse(bytes);

    // then
    assertEquals(value.toJson(), decoded.getResponse().toJson());
    assertEquals(jobKey, decoded.getKey());
  }

  @ParameterizedTest
  @ValueSource(longs = {0, -1, Long.MIN_VALUE})
  void shouldRejectNonPositiveJobKey(final long key) {
    // when / then
    assertThrows(IllegalArgumentException.class, () -> new BrokerUpdateJobPriorityRequest(key, 10));
  }
}
