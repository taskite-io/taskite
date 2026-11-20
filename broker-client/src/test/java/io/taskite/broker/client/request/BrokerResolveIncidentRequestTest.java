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
import io.taskite.protocol.impl.record.value.broker.IncidentRecord;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.IncidentIntent;
import io.taskite.protocol.record.value.broker.ErrorType;
import io.taskite.util.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class BrokerResolveIncidentRequestTest {

  @Test
  void serializesTenantCommandToThePartitionEncodedInIncidentKey() {
    final long incidentKey = Protocol.encodePartitionId(7, 42);
    final var request = new BrokerResolveIncidentRequest(incidentKey);
    request.setTenantId("tenant-a");
    request.setClusterId("cluster-a");
    request.serializeValue();
    final byte[] bytes = request.serializeRequest();
    final var command = new ExecuteCommandRequest();
    command.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
    final var value = new IncidentRecord();
    value.wrap(command.getValue());

    assertTrue(request.addressesSpecificPartition());
    assertEquals(7, command.getPartitionId());
    assertEquals(incidentKey, command.getKey());
    assertEquals("cluster-a", command.getClusterId());
    assertEquals(ValueType.INCIDENT, command.getValueType());
    assertEquals(IncidentIntent.RESOLVE, command.getIntent());
    assertEquals("tenant-a", value.getTenantId());
  }

  @Test
  void decodesResolvedIncidentResponse() {
    final long incidentKey = Protocol.encodePartitionId(3, 124);
    final var value =
        new IncidentRecord()
            .setJobKey(Protocol.encodePartitionId(3, 123))
            .setErrorType(ErrorType.JOB_NO_RETRIES)
            .setTenantId("tenant-a");
    final var valueBuffer = BufferUtil.wrapString(value.toJson());
    final var response =
        new ExecuteCommandResponse()
            .setPartitionId(3)
            .setKey(incidentKey)
            .setValueType(ValueType.INCIDENT)
            .setIntent(IncidentIntent.RESOLVED)
            .setRecordType(RecordType.EVENT)
            .setValue(valueBuffer, 0, valueBuffer.capacity());
    final var bytes = new UnsafeBuffer(new byte[response.getLength()]);
    response.write(bytes, 0);

    final var decoded = new BrokerResolveIncidentRequest(incidentKey).getResponse(bytes);

    assertEquals(value.toJson(), decoded.getResponse().toJson());
    assertEquals(incidentKey, decoded.getKey());
  }
}
