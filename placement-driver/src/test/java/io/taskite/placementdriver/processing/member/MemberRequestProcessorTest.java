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
package io.taskite.placementdriver.processing.member;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService;
import io.taskite.placementdriver.processing.broker.topology.BrokerMemberChangePlanner;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.*;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TopologyOperationType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.*;
import io.taskite.topology.RecoveryFence;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

final class MemberRequestProcessorTest {
  @TempDir Path directory;

  @Test
  void rejectsEntireBatchBeforeWritingWhenLaterChildIsUnfrozen() {
    try (var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      final var fixture = new Fixture(state);
      final var first = child("first", true);
      final var second = child("second", false);
      final var planner = planner(new MemberChangePlan(planned(), List.of(first, second)));
      fixture.process(planner);
      verify(fixture.rejection)
          .appendRejection(
              eq(RejectionType.PROCESSING_ERROR),
              eq(
                  "UNFROZEN_OPERATION: Expected partition operation 'second:join' for member change 'change-1' and resource 'demo' to have a frozen configuration, but none was provided"));
      verify(fixture.response)
          .writeRejectionOnCommand(
              any(),
              eq(RejectionType.PROCESSING_ERROR),
              eq(
                  "UNFROZEN_OPERATION: Expected partition operation 'second:join' for member change 'change-1' and resource 'demo' to have a frozen configuration, but none was provided"));
      verifyNoInteractions(fixture.events);
      verify(fixture.response, never()).writeEventOnCommand(anyLong(), any(), any(), any());
      assertTrue(state.getBrokerTopologyChangeState().pendingChangePlans("demo").isEmpty());
      assertTrue(state.getMemberChangeState().active(MemberChangeScope.BROKER, "demo").isEmpty());
    }
  }

  @Test
  void acceptsFrozenChildrenAndWritesParentAfterEveryChild() {
    try (var storage = new PartitionStorage(directory)) {
      final var fixture =
          new Fixture(new MutableProcessingState(new StateStoreProcessingStore(storage)));
      final var first = child("first", true);
      final var second = child("second", true);
      final var parent = planned();
      fixture.process(planner(new MemberChangePlan(parent, List.of(first, second))));
      final var ordered = inOrder(fixture.events);
      ordered
          .verify(fixture.events)
          .appendFollowUpEvent(
              11L, ValueType.PARTITION_CHANGE, BrokerPartitionChangeIntent.SUBMITTED, first);
      ordered
          .verify(fixture.events)
          .appendFollowUpEvent(
              12L, ValueType.PARTITION_CHANGE, BrokerPartitionChangeIntent.SUBMITTED, second);
      ordered.verify(fixture.events).appendFollowUpEvent(1L, MemberChangeIntent.PLANNED, parent);
      verifyNoInteractions(fixture.rejection);
    }
  }

  @Test
  void realBrokerPlannerSubmitsOnlyFrozenBootstrapOperations() {
    try (var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      state
          .getBrokerClusterConfigState()
          .putClusterConfig(
              "demo",
              new BrokerClusterConfig(
                  1, 1, 1, List.of("broker-0"), BrokerClusterConfig.MANAGED_ROSTER_EPOCH));
      for (int i = 0; i < 2; i++) {
        state
            .getBrokerRegistrationState()
            .registerBroker(
                "demo",
                "broker-" + i,
                "127.0.0.1",
                26501 + i * 100,
                26502 + i * 100,
                "test",
                30000,
                Instant.EPOCH,
                List.of());
      }
      final var fixture = new Fixture(state);
      fixture.process(new BrokerMemberChangePlanner(new BrokerClusterChangePlanningService()));
      final var captured = ArgumentCaptor.forClass(PartitionChangeRecord.class);
      verify(fixture.events)
          .appendFollowUpEvent(
              anyLong(),
              eq(ValueType.PARTITION_CHANGE),
              eq(BrokerPartitionChangeIntent.SUBMITTED),
              captured.capture());
      assertFalse(captured.getValue().getOperations().isEmpty());
      captured
          .getValue()
          .getOperations()
          .forEach(operation -> assertNotNull(operation.configuration(), operation.opId()));
      verifyNoInteractions(fixture.rejection);
    }
  }

  @Test
  void convertsFreezeRejectionWithoutWritingAnyEvent() {
    try (var storage = new PartitionStorage(directory)) {
      final var fixture =
          new Fixture(new MutableProcessingState(new StateStoreProcessingStore(storage)));
      final MemberChangePlanner planner = mock(MemberChangePlanner.class);
      when(planner.scope()).thenReturn(MemberChangeScope.BROKER);
      when(planner.calculate(any(), any()))
          .thenThrow(
              new MemberChangeRejection(
                  RejectionType.INVALID_STATE, "INCOMPLETE_SOURCE_CONFIGURATION: a"));
      fixture.process(planner);
      verify(fixture.rejection)
          .appendRejection(RejectionType.INVALID_STATE, "INCOMPLETE_SOURCE_CONFIGURATION: a");
      verifyNoInteractions(fixture.events);
    }
  }

  @Test
  void placementDriverPlanWithoutChildrenStillAccepted() {
    try (var storage = new PartitionStorage(directory)) {
      final var fixture =
          new Fixture(new MutableProcessingState(new StateStoreProcessingStore(storage)));
      final var parent = planned().setScope(MemberChangeScope.PLACEMENT_DRIVER);
      final MemberChangePlanner planner = mock(MemberChangePlanner.class);
      when(planner.scope()).thenReturn(MemberChangeScope.PLACEMENT_DRIVER);
      when(planner.calculate(any(), any())).thenReturn(new MemberChangePlan(parent, List.of()));
      fixture.process(planner, request().setScope(MemberChangeScope.PLACEMENT_DRIVER));
      verify(fixture.events).appendFollowUpEvent(1L, MemberChangeIntent.PLANNED, parent);
      verifyNoInteractions(fixture.rejection);
    }
  }

  private static MemberChangePlanner planner(final MemberChangePlan plan) {
    final MemberChangePlanner planner = mock(MemberChangePlanner.class);
    when(planner.scope()).thenReturn(MemberChangeScope.BROKER);
    when(planner.calculate(any(), any())).thenReturn(plan);
    return planner;
  }

  private static PartitionChangeRecord child(final String id, final boolean frozen) {
    return new PartitionChangeRecord()
        .setClusterId("demo")
        .setChangeId(id)
        .setPartitionId(1)
        .setOperations(
            List.of(
                new PartitionChangeRecord.Operation(
                    id + ":join",
                    TopologyOperationType.JOIN.value(),
                    "broker-1",
                    0,
                    frozen
                        ? new PartitionChangeRecord.Configuration(
                            List.of(),
                            List.of(new PartitionChangeRecord.Member("broker-1", false, 0)),
                            12,
                            new RecoveryFence(7, 7, ""))
                        : null)));
  }

  private static MemberChangeRecord request() {
    return new MemberChangeRecord()
        .setScope(MemberChangeScope.BROKER)
        .setResourceId("demo")
        .setChangeId("change-1")
        .setRequestedAtEpochMillis(1000)
        .setRequestedCount(2);
  }

  private static MemberChangeRecord planned() {
    return request()
        .setStatus(MemberChangeStatus.PENDING)
        .setOperations(
            List.of(
                new MemberChangeOperation()
                    .setOperationId("join")
                    .setType(MemberChangeOperationType.MEMBER_JOIN)
                    .setMemberId("broker-1")));
  }

  private static final class Fixture {
    final StateWriter events = mock(StateWriter.class);
    final TypedRejectionWriter rejection = mock(TypedRejectionWriter.class);
    final TypedResponseWriter response = mock(TypedResponseWriter.class);
    final TypedRecordProcessorContext context = mock(TypedRecordProcessorContext.class);

    Fixture(final ProcessingState state) {
      final Writers writers = mock(Writers.class);
      when(writers.state()).thenReturn(events);
      when(writers.rejection()).thenReturn(rejection);
      when(writers.response()).thenReturn(response);
      when(context.getWriters()).thenReturn(writers);
      when(context.getProcessingState(ProcessingState.class)).thenReturn(state);
      final var keys = new AtomicLong(10);
      when(context.getKeyGenerator()).thenReturn(keys::incrementAndGet);
    }

    void process(final MemberChangePlanner planner) {
      process(planner, request());
    }

    void process(final MemberChangePlanner planner, final MemberChangeRecord request) {
      final TypedRecord<MemberChangeRecord> record = mock(TypedRecord.class);
      when(record.getKey()).thenReturn(1L);
      when(record.getValue()).thenReturn(request);
      new MemberRequestProcessor(context, List.of(planner)).processRecord(record);
    }
  }
}
