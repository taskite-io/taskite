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

import static org.junit.jupiter.api.Assertions.*;

import io.taskite.protocol.impl.record.value.broker.*;
import io.taskite.protocol.record.*;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.broker.*;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.util.BufferWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class ExecuteAdminCodecTest {
  @Test
  void roundTripsEveryCommandAndEventIntent() {
    for (final var type :
        List.of(ValueType.BROKER_PARTITION_CONFIGURATION, ValueType.BROKER_PARTITION_OPERATION)) {
      final int count = type == ValueType.BROKER_PARTITION_CONFIGURATION ? 4 : 16;
      for (short i = 0; i < count; i += 2) {
        final var command = Intent.fromProtocolValue(type, i);
        final var event = Intent.fromProtocolValue(type, (short) (i + 1));
        assertFalse(command.isEvent());
        assertTrue(event.isEvent());
        final var request =
            new ExecuteAdminRequest()
                .setClusterId("集群-a")
                .setPartitionId(7)
                .setValueType(type)
                .setIntent(command)
                .setValue(new UnsafeBuffer(new byte[] {1, 2, 3}), 0, 3);
        final var decoded = new ExecuteAdminRequest();
        final byte[] bytes = bytes(request);
        decoded.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
        assertEquals(command, decoded.getIntent());
        assertEquals(type, decoded.getValueType());
        assertEquals("集群-a", decoded.getClusterId());
        assertEquals(7, decoded.getPartitionId());
        assertEquals(3, decoded.getValue().capacity());
        final var response =
            new ExecuteAdminResponse()
                .setClusterId("集群-a")
                .setPartitionId(7)
                .setValueType(type)
                .setIntent(event)
                .setRecordType(RecordType.EVENT)
                .setValue(decoded.getValue(), 0, 3);
        final var result = new ExecuteAdminResponse();
        final byte[] reply = bytes(response);
        result.wrap(new UnsafeBuffer(reply), 0, reply.length);
        assertEquals(event, result.getIntent());
        assertEquals(RecordType.EVENT, result.getRecordType());
        assertNull(Intent.fromProtocolValue(type, (short) count));
      }
    }
  }

  @Test
  void preservesConfigurationLongPrecisionMembersAndPlannedVersusActualGeneration() {
    final long generation = 9007199254740993L;
    final var value =
        new BrokerPartitionConfigurationRecord(
                "集群-a",
                7,
                "b1",
                3,
                generation + 2,
                "digest",
                List.of(
                    new BrokerPartitionMember("b1", true, 100),
                    new BrokerPartitionMember("b2", false, 0)),
                6,
                generation,
                "计划",
                generation + 3)
            .setExpectedGeneration(generation - 1)
            .setGeneration(generation + 1)
            .setExpectedRecoveryEpoch(5);
    final var decoded = new BrokerPartitionConfigurationRecord();
    decoded.wrap(new UnsafeBuffer(bytes(value)));
    assertEquals(value.toJson(), decoded.toJson());
    assertEquals(generation, decoded.getExecutionGeneration());
    assertEquals(generation + 1, decoded.getGeneration());
    assertEquals(generation - 1, decoded.getExpectedGeneration());
    assertEquals(5, decoded.getExpectedRecoveryEpoch());
    assertEquals(6, decoded.getRecoveryEpoch());
  }

  @Test
  void preservesOperationParametersAndRejectsUnknownRecordFields() {
    final var value =
        new BrokerPartitionOperationRecord()
            .setClusterId("集群-a")
            .setPartitionId(7)
            .setOpId("op")
            .setTargetBroker("b2")
            .setPriority(100)
            .setMembers(List.of(new BrokerPartitionMember("b1", true, 100)))
            .setExpectedMembershipEpoch(9007199254740995L)
            .setExpectedConfigurationDigest("source")
            .setExpectedRecoveryEpoch(6)
            .setTargetRecoveryEpoch(7)
            .setFencingProofId("proof")
            .setExecutionGeneration(9007199254740993L)
            .setOwnerChangeId("successor")
            .setApplied(true);
    final var decoded = new BrokerPartitionOperationRecord();
    decoded.wrap(new UnsafeBuffer(bytes(value)));
    assertEquals(value.toJson(), decoded.toJson());
    assertThrows(
        RuntimeException.class,
        () ->
            decoded.wrap(
                new UnsafeBuffer("{\"operationType\":\"JOIN\"}".getBytes(StandardCharsets.UTF_8))));
  }

  @Test
  void rejectsEveryTruncationTrailingDataAndOversizedLength() {
    final byte[] frame = bytes(query());
    for (int length = 0; length < frame.length; length++) {
      final var truncated = Arrays.copyOf(frame, length);
      assertThrows(
          IllegalArgumentException.class,
          () -> new ExecuteAdminRequest().wrap(new UnsafeBuffer(truncated), 0, truncated.length));
    }
    final var trailing = Arrays.copyOf(frame, frame.length + 1);
    assertThrows(
        IllegalArgumentException.class,
        () -> new ExecuteAdminRequest().wrap(new UnsafeBuffer(trailing), 0, trailing.length));
    final var overflow = frame.clone();
    ByteBuffer.wrap(overflow)
        .order(ByteOrder.LITTLE_ENDIAN)
        .putInt(
            MessageHeaderEncoder.ENCODED_LENGTH + ExecuteAdminRequestEncoder.BLOCK_LENGTH,
            Integer.MAX_VALUE);
    assertThrows(
        IllegalArgumentException.class,
        () -> new ExecuteAdminRequest().wrap(new UnsafeBuffer(overflow), 0, overflow.length));
    final var response =
        new ExecuteAdminResponse()
            .setClusterId("demo")
            .setPartitionId(7)
            .setValueType(ValueType.BROKER_PARTITION_CONFIGURATION)
            .setIntent(BrokerPartitionConfigurationIntent.QUERIED);
    final var reply = bytes(response);
    for (int length = 0; length < reply.length; length++) {
      final var truncated = Arrays.copyOf(reply, length);
      assertThrows(
          IllegalArgumentException.class,
          () -> new ExecuteAdminResponse().wrap(new UnsafeBuffer(truncated), 0, truncated.length));
    }
  }

  @Test
  void rejectsChangedHeaderOldSchemaAndUnknownValueOrIntent() {
    for (final int field : List.of(0, 2, 4, 6)) {
      final var bytes = bytes(query());
      ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putShort(field, (short) 999);
      assertThrows(
          IllegalArgumentException.class,
          () -> new ExecuteAdminRequest().wrap(new UnsafeBuffer(bytes), 0, bytes.length));
    }
    for (final int version : List.of(10, 11, 12)) {
      final var bytes = bytes(query());
      ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putShort(6, (short) version);
      assertThrows(
          IllegalArgumentException.class,
          () -> new ExecuteAdminRequest().wrap(new UnsafeBuffer(bytes), 0, bytes.length));
    }
    for (final int index : List.of(12, 13)) {
      final var bytes = bytes(query());
      bytes[index] = (byte) 99;
      assertThrows(
          IllegalArgumentException.class,
          () -> new ExecuteAdminRequest().wrap(new UnsafeBuffer(bytes), 0, bytes.length));
    }
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ExecuteAdminResponse()
                .wrap(new UnsafeBuffer(bytes(query())), 0, bytes(query()).length));
  }

  @Test
  void rejectsInvalidIdentityBusinessValuesAndEventRequests() {
    assertThrows(IllegalArgumentException.class, () -> bytes(query().setClusterId(" ")));
    assertThrows(IllegalArgumentException.class, () -> bytes(query().setPartitionId(0)));
    assertThrows(IllegalArgumentException.class, () -> bytes(query().setValueType(ValueType.JOB)));
    assertThrows(
        IllegalArgumentException.class,
        () -> bytes(query().setIntent(BrokerPartitionConfigurationIntent.QUERIED)));
    assertThrows(
        IllegalArgumentException.class,
        () -> bytes(query().setIntent(BrokerPartitionOperationIntent.ADD)));
  }

  @Test
  void roundTripsCommandRejectionAndRejectsInvalidRecordType() {
    final var response =
        new ExecuteAdminResponse()
            .setClusterId("demo")
            .setPartitionId(7)
            .setValueType(ValueType.BROKER_PARTITION_CONFIGURATION)
            .setIntent(BrokerPartitionConfigurationIntent.QUERY)
            .setRecordType(RecordType.COMMAND_REJECTION)
            .setRejectionType(RejectionType.INVALID_STATE)
            .setRejectionReason("NO_QUORUM");
    final var bytes = bytes(response);
    final var decoded = new ExecuteAdminResponse();
    decoded.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
    assertEquals("NO_QUORUM", decoded.getRejectionReason());
    assertEquals(RejectionType.INVALID_STATE, decoded.getRejectionType());
    assertThrows(
        IllegalArgumentException.class, () -> bytes(response.setRecordType(RecordType.COMMAND)));
  }

  private static ExecuteAdminRequest query() {
    return new ExecuteAdminRequest()
        .setClusterId("demo")
        .setPartitionId(7)
        .setValueType(ValueType.BROKER_PARTITION_CONFIGURATION)
        .setIntent(BrokerPartitionConfigurationIntent.QUERY);
  }

  private static byte[] bytes(final BufferWriter writer) {
    final var buffer = new UnsafeBuffer(new byte[writer.getLength()]);
    writer.write(buffer, 0);
    return buffer.byteArray();
  }
}
