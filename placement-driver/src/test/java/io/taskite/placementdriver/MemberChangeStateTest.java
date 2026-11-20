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
package io.taskite.placementdriver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.state.appliers.MemberChangeAdvancedApplier;
import io.taskite.placementdriver.state.appliers.MemberChangePlannedApplier;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperation;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationType;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class MemberChangeStateTest {

  @Test
  void mapsLifecycleProtocolValuesAndRoundTripsRecord() {
    assertEquals(
        MemberChangeIntent.REQUEST, Intent.fromProtocolValue(ValueType.MEMBER_CHANGE, (short) 0));
    assertEquals(
        MemberChangeIntent.CANCELLED,
        Intent.fromProtocolValue(ValueType.MEMBER_CHANGE, (short) 15));

    final MemberChangeRecord source = change(MemberChangeStatus.PENDING);
    final UnsafeBuffer buffer = new UnsafeBuffer(new byte[source.getLength()]);
    source.write(buffer, 0);
    final MemberChangeRecord restored = new MemberChangeRecord();
    restored.wrap(buffer, 0, buffer.capacity());

    assertEquals(MemberChangeScope.BROKER, restored.getScope());
    assertEquals("demo", restored.getResourceId());
    assertEquals("change-1", restored.getChangeId());
    assertEquals(List.of("broker-0", "broker-1", "broker-2"), restored.getSourceVoters());
    assertEquals(
        MemberChangeOperationType.MEMBER_JOIN, restored.getOperations().getFirst().getType());
  }

  @Test
  void persistsActiveChangeAndClearsItWhenTerminal(@TempDir final Path directory) {
    try (PartitionStorage storage = new PartitionStorage(directory.resolve("runtime"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      final var applier = new MemberChangePlannedApplier(state);

      applier.applyState(1, change(MemberChangeStatus.PENDING));
      assertEquals(
          "change-1",
          state
              .getMemberChangeState()
              .active(MemberChangeScope.BROKER, "demo")
              .orElseThrow()
              .getChangeId());

      new MemberChangeAdvancedApplier(state).applyState(1, change(MemberChangeStatus.COMPLETED));
      assertTrue(state.getMemberChangeState().active(MemberChangeScope.BROKER, "demo").isEmpty());
      assertEquals(
          MemberChangeStatus.COMPLETED,
          state
              .getMemberChangeState()
              .get(MemberChangeScope.BROKER, "demo", "change-1")
              .orElseThrow()
              .getStatus());
      assertEquals(
          List.of("change-1"),
          state.getMemberChangeState().recentChanges(MemberChangeScope.BROKER, "demo", 20).stream()
              .map(MemberChangeRecord::getChangeId)
              .toList());
      state
          .getMemberChangeState()
          .put(
              change(MemberChangeStatus.PENDING)
                  .setChangeId("change-2")
                  .setRequestedAtEpochMillis(2));
      assertEquals(
          List.of("change-2"),
          state.getMemberChangeState().recentChanges(MemberChangeScope.BROKER, "demo", 1).stream()
              .map(MemberChangeRecord::getChangeId)
              .toList());
    }
  }

  @Test
  void retainsOnlyConfiguredNumberOfCompletedChanges(@TempDir final Path directory) {
    try (PartitionStorage storage = new PartitionStorage(directory.resolve("retention"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage), () -> 0, () -> 0, 2);

      for (int index = 1; index <= 4; index++) {
        state
            .getMemberChangeState()
            .put(
                change(MemberChangeStatus.COMPLETED)
                    .setChangeId("change-" + index)
                    .setRequestedAtEpochMillis(index));
      }

      assertEquals(
          List.of("change-4", "change-3"),
          state.getMemberChangeState().recentChanges(MemberChangeScope.BROKER, "demo", 10).stream()
              .map(MemberChangeRecord::getChangeId)
              .toList());
      assertTrue(
          state.getMemberChangeState().get(MemberChangeScope.BROKER, "demo", "change-1").isEmpty());
      assertTrue(
          state.getMemberChangeState().get(MemberChangeScope.BROKER, "demo", "change-2").isEmpty());

      state
          .getMemberChangeState()
          .put(
              change(MemberChangeStatus.PENDING)
                  .setChangeId("change-5")
                  .setRequestedAtEpochMillis(5));
      assertEquals(
          List.of("change-5", "change-4", "change-3"),
          state.getMemberChangeState().recentChanges(MemberChangeScope.BROKER, "demo", 10).stream()
              .map(MemberChangeRecord::getChangeId)
              .toList());
    }
  }

  private static MemberChangeRecord change(final MemberChangeStatus status) {
    return new MemberChangeRecord()
        .setScope(MemberChangeScope.BROKER)
        .setResourceId("demo")
        .setChangeId("change-1")
        .setRequestedAdditions(List.of("broker-3"))
        .setSourceVoters(List.of("broker-0", "broker-1", "broker-2"))
        .setTargetCount(4)
        .setTargetVoters(List.of("broker-0", "broker-1", "broker-2", "broker-3"))
        .setTargetEpoch(2)
        .setStatus(status)
        .setOperations(
            List.of(
                new MemberChangeOperation()
                    .setOperationId("join-broker-3")
                    .setType(MemberChangeOperationType.MEMBER_JOIN)
                    .setMemberId("broker-3")));
  }
}
