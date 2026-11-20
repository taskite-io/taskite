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

import io.taskite.broker.client.response.*;
import io.taskite.protocol.impl.encoding.*;
import io.taskite.protocol.impl.record.value.broker.BrokerPartitionConfigurationRecord;
import io.taskite.protocol.record.*;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.*;
import io.taskite.protocol.record.intent.broker.BrokerPartitionConfigurationIntent;
import io.taskite.util.BufferWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class BrokerAdminRequestTest {
  @Test
  void serializesRecordAndKeepsAdminIdentityImmutable() {
    final var request = new ReadRequest();
    final var wire = new ExecuteAdminRequest();
    final byte[] bytes = request.serializeRequest();
    wire.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
    assertEquals(ValueType.BROKER_PARTITION_CONFIGURATION, wire.getValueType());
    assertEquals(BrokerPartitionConfigurationIntent.QUERY, wire.getIntent());
    assertEquals("demo", wire.getClusterId());
    assertEquals(7, wire.getPartitionId());
    final var value = new BrokerPartitionConfigurationRecord();
    value.wrap(wire.getValue());
    assertEquals(request.getRequestWriter().toJson(), value.toJson());
    assertFalse(request.allowsTransportRetry());
    assertTrue(request.addressesSpecificPartition());
    request.setClusterId("demo");
    request.setPartitionId(7);
    assertThrows(IllegalArgumentException.class, () -> request.setClusterId("other"));
    assertThrows(IllegalArgumentException.class, () -> request.setPartitionId(8));
  }

  @Test
  void decodesSuccessAndRejectsMismatchedResponseIdentityOrIntent() {
    final var request = new ReadRequest();
    final var response = success();
    assertEquals(
        "leader",
        request.getResponse(new UnsafeBuffer(bytes(response))).getResponse().getLeaderId());
    for (final var invalid :
        List.of(
            success().setClusterId("other"),
            success().setPartitionId(8),
            success().setIntent(BrokerPartitionConfigurationIntent.AUTHORITY_INSTALLED),
            success()
                .setValueType(ValueType.BROKER_PARTITION_OPERATION)
                .setIntent(BrokerPartitionOperationIntent.ADDED)))
      assertThrows(
          IllegalArgumentException.class,
          () -> request.getResponse(new UnsafeBuffer(bytes(invalid))));
    assertThrows(
        IllegalArgumentException.class,
        () -> request.getResponse(new UnsafeBuffer(request.serializeRequest())));
  }

  @Test
  void decodesStandardErrorAndRejectsEngineRecordMapping() {
    final var error = new ErrorResponse();
    error.setErrorCode(ErrorCode.INTERNAL_ERROR);
    error.setErrorMessage(new UnsafeBuffer("failed".getBytes(StandardCharsets.UTF_8)));
    final var response =
        assertInstanceOf(
            BrokerErrorResponse.class,
            new ReadRequest().getResponse(new UnsafeBuffer(bytes(error))));
    assertEquals(ErrorCode.INTERNAL_ERROR, response.getError().getCode());
    assertEquals("failed", response.getError().getMessage());
    assertThrows(
        UnsupportedOperationException.class, () -> new ReadRequest().mapResponse(7, 1, null));
  }

  @Test
  void mapsCommandRejectionWithOriginalIntentAndReason() {
    final var response =
        success()
            .setRecordType(RecordType.COMMAND_REJECTION)
            .setIntent(BrokerPartitionConfigurationIntent.QUERY)
            .setRejectionType(RejectionType.INVALID_STATE)
            .setRejectionReason("NO_QUORUM");
    final var result =
        assertInstanceOf(
            BrokerRejectionResponse.class,
            new ReadRequest().getResponse(new UnsafeBuffer(bytes(response))));
    assertEquals("NO_QUORUM", result.getRejection().reason());
  }

  private static ExecuteAdminResponse success() {
    final var value = new BrokerPartitionConfigurationRecord().setLeaderId("leader");
    final byte[] bytes = bytes(value);
    return new ExecuteAdminResponse()
        .setClusterId("demo")
        .setPartitionId(7)
        .setValueType(ValueType.BROKER_PARTITION_CONFIGURATION)
        .setIntent(BrokerPartitionConfigurationIntent.QUERIED)
        .setValue(new UnsafeBuffer(bytes), 0, bytes.length);
  }

  private static byte[] bytes(final ErrorResponse writer) {
    final var buffer = new UnsafeBuffer(new byte[writer.getLength()]);
    writer.write(buffer, 0);
    return buffer.byteArray();
  }

  private static byte[] bytes(final BufferWriter writer) {
    final var buffer = new UnsafeBuffer(new byte[writer.getLength()]);
    writer.write(buffer, 0);
    return buffer.byteArray();
  }

  static final class ReadRequest extends BrokerAdminRequest<BrokerPartitionConfigurationRecord> {
    ReadRequest() {
      super(ValueType.BROKER_PARTITION_CONFIGURATION, BrokerPartitionConfigurationIntent.QUERY);
      setClusterId("demo");
      setPartitionId(7);
    }

    @Override
    public BrokerPartitionConfigurationRecord getRequestWriter() {
      return new BrokerPartitionConfigurationRecord().setClusterId("demo").setPartitionId(7);
    }

    @Override
    protected BrokerPartitionConfigurationRecord toResponseDto(final DirectBuffer buffer) {
      final var value = new BrokerPartitionConfigurationRecord();
      value.wrap(buffer);
      return value;
    }
  }
}
