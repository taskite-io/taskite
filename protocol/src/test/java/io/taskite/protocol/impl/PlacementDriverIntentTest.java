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
package io.taskite.protocol.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import java.util.List;
import java.util.Set;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class PlacementDriverIntentTest {
  @Test
  void topologyIntentsFollowClusterLifecycleOnTheWire() {
    assertLifecycle(
        ValueType.TOPOLOGY,
        List.of(
            "CREATE_CLUSTER",
            "CLUSTER_CREATED",
            "CONFIGURE_CLUSTER",
            "CLUSTER_CONFIGURED",
            "UPDATE",
            "UPDATED",
            "COORDINATE",
            "COORDINATED",
            "CANCEL_CLUSTER_CHANGE",
            "CLUSTER_CHANGE_CANCELLED",
            "DELETE_CLUSTER",
            "CLUSTER_DELETED",
            "QUERY",
            "QUERIED"),
        Set.of(
            "CLUSTER_CREATED",
            "CLUSTER_CONFIGURED",
            "UPDATED",
            "COORDINATED",
            "CLUSTER_CHANGE_CANCELLED",
            "CLUSTER_DELETED",
            "QUERIED"));
  }

  @Test
  void brokerRegistrationIntentsFollowRegistrationLifecycleOnTheWire() {
    assertLifecycle(
        ValueType.BROKER_REGISTRATION,
        List.of(
            "REGISTER", "REGISTERED",
            "RENEW", "RENEWED",
            "MARK_OFFLINE", "MARKED_OFFLINE",
            "DRAIN", "DRAINED",
            "DEREGISTER", "DEREGISTERED"),
        Set.of("REGISTERED", "RENEWED", "MARKED_OFFLINE", "DRAINED", "DEREGISTERED"));
  }

  @Test
  void commandDistributionIntentsFollowDistributionLifecycleOnTheWire() {
    assertLifecycle(
        ValueType.COMMAND_DISTRIBUTION,
        List.of(
            "START", "STARTED",
            "DISPATCH", "DISPATCHED",
            "RETRY", "RETRIED",
            "ACKNOWLEDGE", "ACKNOWLEDGED",
            "COMPLETE", "COMPLETED",
            "REDISTRIBUTE", "REDISTRIBUTED",
            "QUERY", "QUERIED",
            "LIST", "LISTED"),
        Set.of(
            "STARTED",
            "DISPATCHED",
            "RETRIED",
            "ACKNOWLEDGED",
            "COMPLETED",
            "REDISTRIBUTED",
            "QUERIED",
            "LISTED"));
  }

  @Test
  void memberChangeIntentsFollowChangeLifecycleOnTheWire() {
    assertLifecycle(
        ValueType.MEMBER_CHANGE,
        List.of(
            "REQUEST",
            "PLANNED",
            "EXECUTE",
            "EXECUTION_RESULT",
            "EXECUTION_UPDATED",
            "JOIN",
            "JOINED",
            "PROMOTE",
            "PROMOTED",
            "TRANSFER_LEADER",
            "LEADER_TRANSFERRED",
            "LEAVE",
            "LEFT",
            "ADVANCE",
            "ADVANCED",
            "CANCELLED"),
        Set.of(
            "PLANNED",
            "EXECUTION_UPDATED",
            "JOINED",
            "PROMOTED",
            "LEADER_TRANSFERRED",
            "LEFT",
            "ADVANCED",
            "CANCELLED"));
  }

  @Test
  void partitionChangeIntentsFollowChangeLifecycleOnTheWire() {
    assertLifecycle(
        ValueType.PARTITION_CHANGE,
        List.of(
            "SUBMIT",
            "SUBMITTED",
            "ADMIT",
            "ADMITTED",
            "AUTHORIZE",
            "AUTHORIZED",
            "BLOCK",
            "BLOCKED",
            "EXECUTE",
            "EXECUTION_RESULT",
            "EXECUTION_UPDATED",
            "CREATE",
            "CREATED",
            "JOIN",
            "JOINED",
            "PROMOTE",
            "PROMOTED",
            "CHANGE_PRIORITY",
            "PRIORITY_CHANGED",
            "TRANSFER_LEADER",
            "LEADER_TRANSFERRED",
            "STEP_DOWN_LEADER",
            "LEADER_STEPPED_DOWN",
            "LEAVE",
            "LEFT",
            "RESET_PEERS",
            "PEERS_RESET",
            "UPDATE_STATUS",
            "STATUS_UPDATED",
            "SUPERSEDE",
            "SUPERSEDING",
            "REPLACE",
            "REPLACED",
            "CANCELLED"),
        Set.of(
            "SUBMITTED",
            "ADMITTED",
            "AUTHORIZED",
            "BLOCKED",
            "EXECUTION_UPDATED",
            "CREATED",
            "JOINED",
            "PROMOTED",
            "PRIORITY_CHANGED",
            "LEADER_TRANSFERRED",
            "LEADER_STEPPED_DOWN",
            "LEFT",
            "PEERS_RESET",
            "STATUS_UPDATED",
            "SUPERSEDING",
            "REPLACED",
            "CANCELLED"));
  }

  private static void assertLifecycle(
      final ValueType type, final List<String> names, final Set<String> events) {
    for (short value = 0; value < names.size(); value++) {
      final String name = names.get(value);
      final Intent intent = Intent.fromProtocolValue(type, value);
      assertEquals(name, intent.name(), type + " wire value " + value);
      assertEquals(value, intent.value(), name);
      assertEquals(events.contains(name), intent.isEvent(), name);
      final RecordType recordType = events.contains(name) ? RecordType.EVENT : RecordType.COMMAND;

      // 原始编号和枚举两种写入方式都必须还原为同一事件或命令。
      for (final var source :
          List.of(
              new RecordMetadata().valueType(type).recordType(recordType).intentValue(value),
              new RecordMetadata()
                  .valueType(type)
                  .recordType(recordType)
                  .intentValue(Short.MIN_VALUE)
                  .intent(intent))) {
        final var buffer = new UnsafeBuffer(new byte[source.getLength()]);
        source.write(buffer, 0);
        final var restored = new RecordMetadata();
        restored.wrap(buffer, 0, buffer.capacity());
        assertSame(intent, restored.getIntent(), name);
        assertEquals(value, restored.getIntentValue(), name);
        assertEquals(recordType, restored.getRecordType(), name);
        assertEquals(type, restored.getValueType(), name);
      }
    }
    for (final short invalid :
        new short[] {Short.MIN_VALUE, -1, (short) names.size(), Short.MAX_VALUE}) {
      assertNull(Intent.fromProtocolValue(type, invalid), type + " invalid value " + invalid);
    }
    assertSame(Intent.UNKNOWN, Intent.fromProtocolValue(type, Intent.NULL_VAL));
  }
}
