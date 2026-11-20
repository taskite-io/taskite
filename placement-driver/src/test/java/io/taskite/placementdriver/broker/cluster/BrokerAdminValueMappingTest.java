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
package io.taskite.placementdriver.broker.cluster;

import static org.junit.jupiter.api.Assertions.*;

import io.taskite.placementdriver.broker.request.*;
import io.taskite.protocol.impl.encoding.*;
import io.taskite.protocol.impl.record.value.broker.*;
import io.taskite.protocol.record.*;
import io.taskite.protocol.record.TopologyOperationType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.broker.*;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.topology.RaftConfigurationReference;
import io.taskite.topology.RecoveryFence;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class BrokerAdminValueMappingTest {
  @Test
  void serializesAllEightOperationsAsValueAndIntentAndPreservesFences() {
    for (final var intent : BrokerPartitionOperationIntent.values()) {
      if (intent.isEvent()) {
        continue;
      }
      final var request = operation(TopologyOperationType.valueOf(intent.name()));
      final var bytes = request.serializeRequest();
      final var frame = new ExecuteAdminRequest();
      frame.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
      assertEquals(intent, frame.getIntent());
      assertEquals(ValueType.BROKER_PARTITION_OPERATION, frame.getValueType());
      final var decoded = new BrokerPartitionOperationRecord();
      decoded.wrap(frame.getValue());
      assertEquals(request.getRequestWriter().toJson(), decoded.toJson());
      assertEquals(9007199254740993L, decoded.getExecutionGeneration());
      assertEquals(9007199254740995L, decoded.getExpectedMembershipEpoch());
      assertEquals(6, decoded.getExpectedRecoveryEpoch());
      assertEquals(7, decoded.getTargetRecoveryEpoch());
      assertEquals("proof", decoded.getFencingProofId());
      assertEquals("successor", decoded.getOwnerChangeId());
      assertEquals("b2", decoded.getTargetBroker());
      assertEquals(100, decoded.getPriority());
      assertFalse(decoded.toJson().contains("operationType"));
      final var reply = response(frame, decoded.setApplied(true));
      assertTrue(request.getResponse(new UnsafeBuffer(reply)).getResponse().getApplied());
    }
  }

  @Test
  void rejectsDifferentOperationResultIdentityAndUnappliedResult() {
    for (final var value :
        List.of(
            operation(TopologyOperationType.JOIN)
                .getRequestWriter()
                .setApplied(true)
                .setOpId("other"),
            operation(TopologyOperationType.JOIN)
                .getRequestWriter()
                .setApplied(true)
                .setClusterId("other"),
            operation(TopologyOperationType.JOIN)
                .getRequestWriter()
                .setApplied(true)
                .setPartitionId(8),
            operation(TopologyOperationType.JOIN).getRequestWriter().setApplied(false))) {
      final var request = operation(TopologyOperationType.JOIN);
      final var frame = new ExecuteAdminRequest();
      final var bytes = request.serializeRequest();
      frame.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
      assertThrows(
          IllegalArgumentException.class,
          () -> request.getResponse(new UnsafeBuffer(response(frame, value))));
    }
  }

  @Test
  void configurationRequestsSeparateExpectedAndActualFieldsAndValidateResponses() {
    for (final var request :
        List.of(
            new BrokerReadPartitionConfigurationRequest("demo", 7),
            new BrokerInstallPartitionAuthorityRequest(
                "demo", 7, 9007199254740993L, 9007199254740994L, "owner", 6))) {
      final var frame = new ExecuteAdminRequest();
      final var bytes = request.serializeRequest();
      frame.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
      final var value = new BrokerPartitionConfigurationRecord();
      value.wrap(frame.getValue());
      assertEquals("demo", value.getClusterId());
      assertEquals(7, value.getPartitionId());
      assertEquals(0, value.getExecutionGeneration());
      if (request instanceof BrokerInstallPartitionAuthorityRequest) {
        assertEquals(9007199254740993L, value.getExpectedGeneration());
        assertEquals(9007199254740994L, value.getGeneration());
        assertEquals(6, value.getExpectedRecoveryEpoch());
      }
      final var actual =
          new BrokerPartitionConfigurationRecord(
              "demo", 7, "b1", 3, 99, "digest", List.of(), 6, 9007199254740994L, "owner", 100);
      assertEquals(
          actual.toJson(),
          request.getResponse(new UnsafeBuffer(response(frame, actual))).getResponse().toJson());
      assertThrows(
          IllegalArgumentException.class,
          () -> request.getResponse(new UnsafeBuffer(response(frame, actual.setPartitionId(8)))));
    }
  }

  @Test
  void rejectsInvalidAuthorityArguments() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new BrokerInstallPartitionAuthorityRequest("demo", 7, 4, 4, "owner", 0));
    assertThrows(
        IllegalArgumentException.class,
        () -> new BrokerInstallPartitionAuthorityRequest("demo", 7, -1, 4, "owner", 0));
    assertThrows(
        IllegalArgumentException.class,
        () -> new BrokerInstallPartitionAuthorityRequest("demo", 7, 3, 4, "", 0));
    assertThrows(
        IllegalArgumentException.class,
        () -> new BrokerInstallPartitionAuthorityRequest("demo", 7, 3, 4, "owner", -1));
  }

  private static BrokerApplyTopologyOperationRequest operation(final TopologyOperationType type) {
    return new BrokerApplyTopologyOperationRequest(
        new BrokerApplyTopologyOperationRequest.Partition("demo", 7),
        new BrokerApplyTopologyOperationRequest.Operation("op-" + type, type, "b2", 100),
        new BrokerApplyTopologyOperationRequest.Configuration(
            List.of(
                new BrokerPartitionMember("b1", true, 100),
                new BrokerPartitionMember("b2", false, 0)),
            new RaftConfigurationReference(9007199254740995L, "source"),
            new RecoveryFence(6, 7, "proof")),
        new BrokerApplyTopologyOperationRequest.Authority(9007199254740993L, "successor"));
  }

  private static byte[] response(
      final ExecuteAdminRequest request, final UnifiedRecordValue value) {
    final var payload = new UnsafeBuffer(new byte[value.getLength()]);
    value.write(payload, 0);
    final var response =
        new ExecuteAdminResponse()
            .setClusterId(request.getClusterId())
            .setPartitionId(request.getPartitionId())
            .setValueType(request.getValueType())
            .setIntent(
                Intent.fromProtocolValue(
                    request.getValueType(), (short) (request.getIntent().value() + 1)))
            .setValue(payload, 0, payload.capacity());
    final var bytes = new UnsafeBuffer(new byte[response.getLength()]);
    response.write(bytes, 0);
    return bytes.byteArray();
  }
}
