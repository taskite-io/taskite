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
package io.taskite.placementdriver.processing.broker.cluster;

import static org.junit.jupiter.api.Assertions.*;

import io.taskite.placementdriver.broker.cluster.*;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.appliers.PartitionChangeCancelledApplier;
import io.taskite.placementdriver.state.appliers.PlacementDriverEventAppliers;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerClusterChangeStatusRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.record.*;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeCancellationReason;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeStatus;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeType;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.state.*;
import io.taskite.streamplatform.api.writer.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class TopologyCancelClusterChangeProcessorTest {
  @TempDir Path directory;

  @Test
  void standaloneRecoveryPlanCancelsDurablyAndRetainsHistory() {
    final String id = "recover:broker-4:3:8cad8e25-9ef6-47e5-8c89-e8fbea386b9f";
    final Path database = directory.resolve("cancel");
    try (final PartitionStorage storage = new PartitionStorage(database)) {
      final var state = state(storage);
      final var plan = plan(id, BrokerPartitionChangeStatus.WAITING_READY);
      state.getBrokerTopologyChangeState().putChangePlan(plan);
      state.getBrokerTopologyChangeState().markPending(plan);
      assertTrue(query(state, id).isCancellable());
      assertEquals(ChangeType.PARTITION, query(state, id).getChangeType());
      final var capture = cancel(state, id);
      assertNull(capture.rejection, capture.reason);
      assertEquals(1, capture.events.size());
      assertEquals(BrokerPartitionChangeIntent.CANCELLED, capture.events.getFirst().intent());
      assertNotNull(capture.response);
      // The HTTP boundary expects the original ClusterConfigRecord, even for a partition event.
      assertInstanceOf(ClusterConfigRecord.class, capture.response);
      assertEquals(id, ((ClusterConfigRecord) capture.response).getChangeId());
      capture.events.forEach(
          event ->
              PlacementDriverEventAppliers.create(state)
                  .applyState(1, event.intent(), event.value(), 1));
      assertTrue(state.getBrokerTopologyChangeState().pendingChangePlans("demo").isEmpty());
      assertEquals(
          plan.operations(),
          state.getBrokerTopologyChangeState().changePlan("demo", id).orElseThrow().operations());
    }
    try (final PartitionStorage storage = new PartitionStorage(database)) {
      final var state = state(storage);
      assertTrue(
          state.getBrokerTopologyChangeState().changePlan("demo", id).orElseThrow().isTerminal());
      assertTrue(state.getBrokerTopologyChangeState().pendingChangePlans("demo").isEmpty());
    }
  }

  @Test
  void reconcilingPlanIsConflictRatherThanNotFound() {
    try (final PartitionStorage storage = new PartitionStorage(directory.resolve("superseding"))) {
      final var state = state(storage);
      final var plan = plan("recover:broker-4:3:id", BrokerPartitionChangeStatus.SUPERSEDING);
      state.getBrokerTopologyChangeState().putChangePlan(plan);
      state.getBrokerTopologyChangeState().markPending(plan);
      assertFalse(query(state, plan.changeId()).isCancellable());
      assertEquals(
          ChangeCancellationReason.CHANGE_RECONCILING,
          query(state, plan.changeId()).getCancellationReason());
      final var capture = cancel(state, plan.changeId());
      assertEquals(RejectionType.INVALID_STATE, capture.rejection);
      assertTrue(capture.reason.startsWith("CHANGE_RECONCILING:"), capture.reason);
      assertTrue(capture.events.isEmpty());
      assertEquals(
          plan, state.getBrokerTopologyChangeState().pendingChangePlans("demo").getFirst());
    }
  }

  @Test
  void mixedCountAndReplicationChangeRejectsUnsafeRollbackBeforeAppendingEvents() {
    try (final var storage = new PartitionStorage(directory.resolve("mixed-cancel"))) {
      final var state = state(storage);
      state
          .getBrokerClusterConfigState()
          .putClusterConfig(
              "demo", new BrokerClusterConfig(5, 1, 5, List.of("a", "b", "c", "d", "e"), 1));
      state
          .getMemberChangeState()
          .put(
              new MemberChangeRecord()
                  .setScope(MemberChangeScope.BROKER)
                  .setResourceId("demo")
                  .setChangeId("mixed")
                  .setSourceVoters(List.of("a", "b", "c"))
                  .setTargetCount(5)
                  .setTargetVoters(List.of("a", "b", "c", "d", "e"))
                  .setStatus(MemberChangeStatus.PENDING));
      final var rejected = cancel(state, "mixed");
      assertEquals(RejectionType.INVALID_STATE, rejected.rejection);
      assertTrue(rejected.reason.startsWith("UNSAFE_CONFIGURATION_CANCELLATION"));
      assertTrue(rejected.events.isEmpty());
      assertEquals(5, state.getBrokerClusterConfigState().clusterConfig("demo").clusterSize());
      assertFalse(query(state, "mixed").isCancellable());
      assertEquals(
          ChangeCancellationReason.UNSAFE_CONFIGURATION_CANCELLATION,
          query(state, "mixed").getCancellationReason());
      assertEquals(
          MemberChangeStatus.PENDING,
          state
              .getMemberChangeState()
              .get(MemberChangeScope.BROKER, "demo", "mixed")
              .orElseThrow()
              .getStatus());
      state
          .getBrokerClusterConfigState()
          .putClusterConfig(
              "demo", new BrokerClusterConfig(5, 1, 3, List.of("a", "b", "c", "d", "e"), 1));
      final var accepted = cancel(state, "mixed");
      assertNull(accepted.rejection);
      accepted.events.forEach(
          event ->
              PlacementDriverEventAppliers.create(state)
                  .applyState(1, event.intent(), event.value(), 1));
      assertEquals(3, state.getBrokerClusterConfigState().clusterConfig("demo").clusterSize());
      assertEquals(
          3, state.getBrokerClusterConfigState().clusterConfig("demo").replicationFactor());
    }
  }

  @Test
  void unknownChangeStillReturnsNotFound() {
    try (final PartitionStorage storage = new PartitionStorage(directory.resolve("unknown"))) {
      final var capture = cancel(state(storage), "unknown");
      assertEquals(RejectionType.NOT_FOUND, capture.rejection);
      assertEquals(
          "CHANGE_NOT_FOUND: Expected to cancel topology change 'unknown' in cluster 'demo', but no matching change was found",
          capture.reason);
      assertTrue(capture.events.isEmpty());
    }
  }

  @Test
  void supersededRootCannotCancelCurrentDescendant() {
    try (final PartitionStorage storage = new PartitionStorage(directory.resolve("root"))) {
      final var state = state(storage);
      final var old =
          plan("old", BrokerPartitionChangeStatus.SUPERSEDED)
              .withExecution(
                  new ChangePlan.Execution(1, 9, "", "next", "old", PlanBlockedReason.NONE, 0));
      final var next =
          plan("next", BrokerPartitionChangeStatus.WAITING_READY)
              .withExecution(
                  new ChangePlan.Execution(2, 12, "old", "", "old", PlanBlockedReason.NONE, 0));
      state.getBrokerTopologyChangeState().putChangePlan(old);
      state.getBrokerTopologyChangeState().putChangePlan(next);
      state.getBrokerTopologyChangeState().markPending(next);
      assertEquals(ChangeStatus.WAITING_READY, query(state, "old").getStatus());
      assertEquals(
          ChangeCancellationReason.CHANGE_SUPERSEDED, query(state, "old").getCancellationReason());
      final var capture = cancel(state, "old");
      assertEquals(RejectionType.INVALID_STATE, capture.rejection);
      assertEquals(
          "CHANGE_SUPERSEDED: Expected to cancel topology change 'old' in cluster 'demo', but its current plan state does not allow cancellation",
          capture.reason);
      assertTrue(capture.events.isEmpty());
      assertEquals(
          next, state.getBrokerTopologyChangeState().pendingChangePlans("demo").getFirst());
    }
  }

  @Test
  void recoverySuccessorRetainsPendingOwnershipEvenForDirectCancellationEvent() {
    try (final PartitionStorage storage = new PartitionStorage(directory.resolve("successor"))) {
      final var state = state(storage);
      final var next =
          plan("next", BrokerPartitionChangeStatus.IN_PROGRESS)
              .withExecution(
                  new ChangePlan.Execution(2, 12, "old", "", "old", PlanBlockedReason.NONE, 0));
      state.getBrokerTopologyChangeState().putChangePlan(next);
      state.getBrokerTopologyChangeState().markPending(next);
      assertEquals(
          ChangeCancellationReason.CHANGE_RECOVERY_REQUIRED,
          query(state, "next").getCancellationReason());
      final var capture = cancel(state, "next");
      assertEquals(RejectionType.INVALID_STATE, capture.rejection);
      assertEquals(
          "CHANGE_RECOVERY_REQUIRED: Expected to cancel topology change 'next' in cluster 'demo', but its current plan state does not allow cancellation",
          capture.reason);
      assertTrue(capture.events.isEmpty());
      new PartitionChangeCancelledApplier(state)
          .applyState(1, BrokerPartitionChangeRecordMapper.fromPlan(next));
      assertEquals(
          next, state.getBrokerTopologyChangeState().pendingChangePlans("demo").getFirst());
    }
  }

  @Test
  void aggregateCancellationStopsGoalButPreservesReconcilingPartition() {
    try (final PartitionStorage storage = new PartitionStorage(directory.resolve("aggregate"))) {
      final var state = state(storage);
      final var plan = plan("old", BrokerPartitionChangeStatus.SUPERSEDING);
      state.getBrokerTopologyChangeState().putChangePlan(plan);
      state.getBrokerTopologyChangeState().markPending(plan);
      state
          .getBrokerTopologyChangeState()
          .putClusterChangeOperation(
              new ClusterChangeOperation(
                  "demo",
                  "aggregate",
                  1,
                  5,
                  1,
                  3,
                  List.of(),
                  List.of(),
                  List.of(),
                  List.of(),
                  List.of("old"),
                  BrokerClusterChangeStatus.IN_PROGRESS,
                  100,
                  200));
      assertTrue(query(state, "aggregate").isCancellable());
      assertEquals(ChangeType.CLUSTER, query(state, "aggregate").getChangeType());
      final var capture = cancel(state, "aggregate");
      assertNull(capture.rejection);
      capture.events.forEach(
          event ->
              PlacementDriverEventAppliers.create(state)
                  .applyState(1, event.intent(), event.value(), 1));
      assertEquals(
          BrokerClusterChangeStatus.CANCELLED,
          state
              .getBrokerTopologyChangeState()
              .clusterChangeOperation("demo", "aggregate")
              .orElseThrow()
              .status());
      assertEquals(
          plan, state.getBrokerTopologyChangeState().pendingChangePlans("demo").getFirst());
      final var repeat = cancel(state, "aggregate");
      assertNull(repeat.rejection);
      assertTrue(repeat.events.isEmpty());
    }
  }

  @Test
  void memberChangeWithoutAggregateRemainsCancellable() {
    try (final PartitionStorage storage = new PartitionStorage(directory.resolve("member"))) {
      final var state = state(storage);
      state
          .getMemberChangeState()
          .put(
              new MemberChangeRecord()
                  .setScope(MemberChangeScope.BROKER)
                  .setResourceId("demo")
                  .setChangeId("member")
                  .setStatus(MemberChangeStatus.PENDING));
      assertEquals(ChangeType.MEMBER, query(state, "member").getChangeType());
      assertTrue(query(state, "member").isCancellable());
      final var capture = cancel(state, "member");
      assertNull(capture.rejection);
      capture.events.forEach(
          event ->
              PlacementDriverEventAppliers.create(state)
                  .applyState(1, event.intent(), event.value(), 1));
      assertEquals(
          MemberChangeStatus.CANCELLED,
          state
              .getMemberChangeState()
              .get(MemberChangeScope.BROKER, "demo", "member")
              .orElseThrow()
              .getStatus());
    }
  }

  @Test
  void groupCancellationRejectsBeforeWritingAnyEventsIfOnePlanIsReconciling() {
    try (final PartitionStorage storage = new PartitionStorage(directory.resolve("group"))) {
      final var state = state(storage);
      final var first = plan("admin:rebalance:group:3", BrokerPartitionChangeStatus.PENDING);
      final var second =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "admin:rebalance:group:4", 4),
                  new ChangePlan.Versions(1, 2),
                  List.of(TopologyOperation.leave(4, "broker-4"))),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.SUPERSEDING),
              ChangePlan.Deadline.NONE);
      state.getBrokerTopologyChangeState().putChangePlan(first);
      state.getBrokerTopologyChangeState().markPending(first);
      state.getBrokerTopologyChangeState().putChangePlan(second);
      state.getBrokerTopologyChangeState().markPending(second);
      assertEquals(
          ChangeCancellationReason.CHANGE_RECONCILING,
          query(state, "group").getCancellationReason());
      final var capture = cancel(state, "group");
      assertEquals(RejectionType.INVALID_STATE, capture.rejection);
      assertTrue(capture.events.isEmpty());
      state
          .getBrokerTopologyChangeState()
          .putChangePlan(second.withStatus(BrokerPartitionChangeStatus.PENDING));
      final var accepted = cancel(state, "group");
      assertNull(accepted.rejection);
      assertEquals(2, accepted.events.size());
    }
  }

  @Test
  void terminalAndUnownedPlansReturnExplicitConflict() {
    try (final PartitionStorage storage = new PartitionStorage(directory.resolve("not-current"))) {
      final var state = state(storage);
      final var finished = plan("completed", BrokerPartitionChangeStatus.COMPLETED);
      final var unowned = plan("unowned", BrokerPartitionChangeStatus.PENDING);
      state.getBrokerTopologyChangeState().putChangePlan(finished);
      state.getBrokerTopologyChangeState().putChangePlan(unowned);
      for (final String id : List.of("completed", "unowned")) {
        assertFalse(query(state, id).isCancellable());
        final var capture = cancel(state, id);
        assertEquals(RejectionType.INVALID_STATE, capture.rejection);
        assertEquals(
            "%s: Expected to cancel topology change '%s' in cluster 'demo', but its current plan state does not allow cancellation"
                .formatted(id.equals("completed") ? "CHANGE_TERMINAL" : "CHANGE_NOT_CURRENT", id),
            capture.reason);
        assertTrue(capture.events.isEmpty());
      }
    }
  }

  private static BrokerClusterChangeStatusRecord query(
      final MutableProcessingState state, final String id) {
    return (BrokerClusterChangeStatusRecord)
        new BrokerClusterChangeQueryBehavior(state)
            .createResponseValue(new TopologyQueryRecord().setClusterId("demo").setChangeId(id));
  }

  private static ChangePlan plan(final String id, final BrokerPartitionChangeStatus status) {
    return new ChangePlan(
        new ChangePlan.Definition(
            new ChangePlan.Identity("demo", id, 3),
            new ChangePlan.Versions(1, 2),
            List.of(TopologyOperation.leave(3, "broker-4"))),
        new ChangePlan.Progress(0, status),
        new ChangePlan.Deadline(100, 200),
        new ChangePlan.Execution(2, 12, "", "", id, PlanBlockedReason.NONE, 0));
  }

  private static MutableProcessingState state(final PartitionStorage storage) {
    return new MutableProcessingState(new StateStoreProcessingStore(storage));
  }

  private static Capture cancel(final MutableProcessingState state, final String id) {
    final var capture = new Capture();
    final var value = new ClusterConfigRecord().setClusterId("demo").setChangeId(id);
    new TopologyCancelClusterChangeProcessor(new Context(state, capture))
        .processRecord(
            new TypedRecord<>() {
              public long getKey() {
                return 1;
              }

              public ClusterConfigRecord getValue() {
                return value;
              }

              public String getRequestStreamId() {
                return "stream";
              }

              public String getRequestId() {
                return "request";
              }

              public int getLength() {
                return value.getLength();
              }
            });
    return capture;
  }

  private record Context(MutableProcessingState state, Capture capture)
      implements TypedRecordProcessorContext {
    public int getPartitionId() {
      return 1;
    }

    public KeyGenerator getKeyGenerator() {
      return () -> 1;
    }

    public Writers getWriters() {
      return capture;
    }

    public Object getProcessingState() {
      return state;
    }

    public EventAppliers getEventAppliers() {
      return PlacementDriverEventAppliers.create(state);
    }
  }

  private record Event(Intent intent, UnifiedRecordValue value) {}

  private static final class Capture implements Writers {
    final List<Event> events = new ArrayList<>();
    RejectionType rejection;
    String reason;
    UnifiedRecordValue response;

    public TypedCommandWriter command() {
      return (key, intent, value) -> {};
    }

    public StateWriter state() {
      return new StateWriter() {
        public void appendFollowUpEvent(long key, Intent intent, UnifiedRecordValue value) {
          events.add(new Event(intent, value));
        }

        public void appendFollowUpEvent(
            long key, Intent intent, UnifiedRecordValue value, int version) {
          appendFollowUpEvent(key, intent, value);
        }

        public void appendFollowUpEvent(
            long key, ValueType type, Intent intent, UnifiedRecordValue value) {
          appendFollowUpEvent(key, intent, value);
        }

        public void appendFollowUpEvent(
            long key, ValueType type, Intent intent, UnifiedRecordValue value, int version) {
          appendFollowUpEvent(key, intent, value);
        }
      };
    }

    public TypedRejectionWriter rejection() {
      return (type, message) -> {
        rejection = type;
        reason = message;
      };
    }

    public TypedResponseWriter response() {
      return new TypedResponseWriter() {
        public void writeEventOnCommand(
            long key, Intent intent, UnifiedRecordValue value, TypedRecord<?> command) {
          response = value;
        }

        public void writeRejectionOnCommand(
            TypedRecord<?> command, RejectionType type, String reason) {}
      };
    }

    public PostCommitActionWriter postCommit() {
      return action -> {};
    }
  }
}
