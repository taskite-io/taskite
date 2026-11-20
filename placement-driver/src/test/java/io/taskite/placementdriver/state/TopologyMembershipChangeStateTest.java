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
package io.taskite.placementdriver.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeStatus;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.ClusterChangeOperation;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperation;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationStatus;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationType;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberExecutionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionExecutionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyChangeTaskRecord.Scope;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyChangeTaskRecord.Status;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

final class TopologyMembershipChangeStateTest {
  @TempDir Path directory;

  @Test
  void retainsEachPartitionTaskAndFinalObservationAcrossRestart() {
    final var plan = plan("change");
    try (var storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      final var tasks = state.getTopologyMembershipChangeState();
      tasks.markPartition(plan, true);
      final var initial = tasks.partition("same:cluster", 1).orElseThrow();
      tasks.putPartition(
          new PartitionExecutionRecord.Schedule(
              initial.plan(), PartitionExecutionRecord.Phase.READ, 77, 100, 500, null, null));
      tasks.markPartition(plan.advanced(), true);
      assertEquals(3, tasks.tasks(Scope.BROKER_PARTITION, "same:cluster", "change").size());
      assertEquals(0, tasks.reserved(0, 256));
      assertEquals(1, tasks.partition("same:cluster", 1).orElseThrow().plan().operationIndex());
    }
    try (var storage = new PartitionStorage(directory)) {
      final var tasks = state(storage).getTopologyMembershipChangeState();
      final var completed =
          tasks.tasks(Scope.BROKER_PARTITION, "same:cluster", "change").stream()
              .filter(task -> task.key().operationId().equals(plan.operations().getFirst().opId()))
              .findFirst()
              .orElseThrow();
      assertEquals(Status.COMPLETED, completed.status());
      assertEquals(77, completed.partitionSchedule().attempt());
      tasks.markPartition(
          plan.advanced().advanced().withStatus(BrokerPartitionChangeStatus.IN_PROGRESS), true);
      assertEquals("", tasks.dueBefore(0, 10).getFirst().key().operationId());
      tasks.markPartition(
          plan.advanced().advanced().withStatus(BrokerPartitionChangeStatus.COMPLETED), true);
      assertTrue(tasks.partition("same:cluster", 1).isEmpty());
      assertEquals(-1, tasks.nextDueAt());
      assertEquals(3, tasks.tasks(Scope.BROKER_PARTITION, "same:cluster", "change").size());
      assertTrue(tasks.deletePlan(Scope.BROKER_PARTITION, "same:cluster", "change"));
      assertTrue(tasks.tasks(Scope.BROKER_PARTITION, "same:cluster", "change").isEmpty());
    }
  }

  @Test
  void isolatesScopesAndCountsBothTransportsInOneReservationIndex() {
    try (var storage = new PartitionStorage(directory)) {
      final var tasks = state(storage).getTopologyMembershipChangeState();
      tasks.markPartition(plan("shared"), true);
      tasks.markMember(member("shared", MemberChangeScope.BROKER), true);
      tasks.markMember(member("shared", MemberChangeScope.PLACEMENT_DRIVER), true);
      final var partition = tasks.partition("same:cluster", 1).orElseThrow();
      tasks.putPartition(
          new PartitionExecutionRecord.Schedule(
              partition.plan(), PartitionExecutionRecord.Phase.APPLY, 1, 10, 200, null, null));
      final var member =
          tasks.member(MemberChangeScope.PLACEMENT_DRIVER, "same:cluster").orElseThrow();
      tasks.putMember(
          MemberChangeScope.PLACEMENT_DRIVER,
          new MemberExecutionRecord.Schedule(
              member.operation(), MemberExecutionRecord.Phase.RPC, 2, 20, 300, 1, 10));
      assertEquals(3, tasks.dueBefore(100, 10).size());
      assertEquals(1, tasks.dueBefore(100, 1).size());
      assertEquals(2, tasks.reserved(0, 256));
      assertEquals(1, tasks.reserved(200, 256));
      assertEquals(0, tasks.reserved(300, 256));
      tasks.deleteResource(Scope.PLACEMENT_DRIVER_MEMBER, "same:cluster");
      assertTrue(tasks.member(MemberChangeScope.PLACEMENT_DRIVER, "same:cluster").isEmpty());
      assertTrue(tasks.member(MemberChangeScope.BROKER, "same:cluster").isPresent());
      assertTrue(tasks.partition("same:cluster", 1).isPresent());
      assertEquals(1, tasks.reserved(0, 256));
    }
  }

  @Test
  void staleScheduleCannotReplaceTheNextTaskOrActiveSuccessor() {
    try (var storage = new PartitionStorage(directory)) {
      final var tasks = state(storage).getTopologyMembershipChangeState();
      final var old = plan("old");
      tasks.markPartition(old, true);
      final var stale = tasks.partition("same:cluster", 1).orElseThrow();
      tasks.markPartition(old.advanced(), true);
      tasks.putPartition(stale.observe(9999));
      assertEquals(1, tasks.partition("same:cluster", 1).orElseThrow().plan().operationIndex());
      assertFalse(tasks.deletePlan(Scope.BROKER_PARTITION, "same:cluster", "old"));
      tasks.markPartition(plan("next"), true);
      tasks.markPartition(old.withStatus(BrokerPartitionChangeStatus.SUPERSEDED), false);
      assertTrue(tasks.deletePlan(Scope.BROKER_PARTITION, "same:cluster", "old"));
      assertEquals("next", tasks.partition("same:cluster", 1).orElseThrow().plan().changeId());
      assertEquals("next", tasks.dueBefore(0, 10).getFirst().key().changeId());
    }
  }

  @Test
  void rollsBackTaskHistoryPointersAndReservationsTogether() throws Exception {
    try (var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var tasks = new MutableProcessingState(store).getTopologyMembershipChangeState();
      tasks.markPartition(plan("change"), true);
      store.getCurrentTransaction().commit();
      final var original = tasks.partition("same:cluster", 1).orElseThrow();
      store.getCurrentTransaction();
      tasks.putPartition(
          new PartitionExecutionRecord.Schedule(
              original.plan(), PartitionExecutionRecord.Phase.READ, 9, 10, 100, null, null));
      tasks.markPartition(plan("change").advanced(), true);
      store.getCurrentTransaction().rollback();
      assertEquals(original, tasks.partition("same:cluster", 1).orElseThrow());
      assertEquals(1, tasks.dueBefore(0, 10).size());
      assertEquals(0, tasks.reserved(0, 256));
      assertEquals(Status.IN_PROGRESS, tasks.dueBefore(0, 10).getFirst().status());
    }
  }

  @ParameterizedTest
  @EnumSource(MemberChangeScope.class)
  void prunesMemberPlanAndItsTasksWhileRetainingActiveHistory(final MemberChangeScope scope) {
    try (var storage = new PartitionStorage(directory)) {
      final var state =
          new MutableProcessingState(new StateStoreProcessingStore(storage), () -> 0, () -> 0, 1);
      final var changes = state.getMemberChangeState();
      final var tasks = state.getTopologyMembershipChangeState();
      final Scope taskScope =
          scope == MemberChangeScope.BROKER ? Scope.BROKER_MEMBER : Scope.PLACEMENT_DRIVER_MEMBER;
      final var first = member("first", scope);
      changes.put(first);
      changes.setActive(first);
      final var firstEntry = tasks.member(scope, "same:cluster").orElseThrow();
      tasks.putMember(
          scope,
          new MemberExecutionRecord.Schedule(
              firstEntry.operation(), MemberExecutionRecord.Phase.RPC, 15, 10, 100, 1, 10));
      changes.put(first.copy().setStatus(MemberChangeStatus.CANCELLED));
      final var second = member("second", scope).setRequestedAtEpochMillis(2);
      changes.put(second);
      changes.setActive(second);
      changes.put(second.copy().setStatus(MemberChangeStatus.COMPLETED).setNextOperationIndex(1));
      final var active = member("active", scope).setRequestedAtEpochMillis(3);
      changes.put(active);
      changes.setActive(active);
      assertTrue(changes.get(scope, "same:cluster", "first").isEmpty());
      assertTrue(tasks.tasks(taskScope, "same:cluster", "first").isEmpty());
      assertEquals(2, tasks.tasks(taskScope, "same:cluster", "second").size());
      assertEquals(2, tasks.tasks(taskScope, "same:cluster", "active").size());
      assertEquals(0, tasks.reserved(0, 256));
      changes.deleteResource(scope, "same:cluster");
      assertEquals(-1, tasks.nextDueAt());
      assertTrue(tasks.tasks(taskScope, "same:cluster", "active").isEmpty());
    }
  }

  @Test
  void standaloneHistoryDeletesOnlyUnreferencedOperationDedupe() {
    try (var storage = new PartitionStorage(directory)) {
      final var state = limitedState(storage);
      final var changes = state.getBrokerTopologyChangeState();
      final var old =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("same:cluster", "old", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(
                      new TopologyOperation(
                          "old:next:shared", TopologyOperation.join(1, "a").type(), "a", 0),
                      TopologyOperation.join(1, "b"))),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.IN_PROGRESS),
              ChangePlan.Deadline.NONE);
      final var next =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("same:cluster", "old:next", 1),
                  new ChangePlan.Versions(1, 2),
                  old.operations()),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.IN_PROGRESS),
              ChangePlan.Deadline.NONE);
      changes.putChangePlan(old);
      changes.markOperationApplied("same:cluster", old.operations().getFirst().opId());
      changes.putChangePlan(old.advanced().advanced());
      changes.putChangePlan(next.advanced().advanced());
      assertTrue(changes.changePlan("same:cluster", "old").isEmpty());
      assertTrue(
          state
              .getTopologyMembershipChangeState()
              .tasks(Scope.BROKER_PARTITION, "same:cluster", "old")
              .isEmpty());
      assertTrue(changes.isOperationApplied("same:cluster", old.operations().getFirst().opId()));
      changes.deleteCluster("same:cluster");
      assertFalse(changes.isOperationApplied("same:cluster", old.operations().getFirst().opId()));
      assertEquals(-1, state.getTopologyMembershipChangeState().nextDueAt());
    }
  }

  @Test
  void clusterHistoryCascadeAndRollbackPreserveAllRecordsTogether() throws Exception {
    try (var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store, () -> 0, () -> 0, 1);
      final var changes = state.getBrokerTopologyChangeState();
      final var first = plan("first");
      changes.putClusterChangeOperation(
          cluster("parent-first", "first", 1, BrokerClusterChangeStatus.PENDING));
      changes.putChangePlan(first);
      changes.markOperationApplied("same:cluster", first.operations().getFirst().opId());
      changes.putChangePlan(first.advanced().advanced());
      changes.putClusterChangeOperation(
          cluster("parent-first", "first", 1, BrokerClusterChangeStatus.COMPLETED));
      store.getCurrentTransaction().commit();
      store.getCurrentTransaction();
      final var next = plan("next");
      changes.putClusterChangeOperation(
          cluster("parent-next", "next", 2, BrokerClusterChangeStatus.PENDING));
      changes.putChangePlan(next);
      changes.putChangePlan(next.advanced().advanced());
      changes.putClusterChangeOperation(
          cluster("parent-next", "next", 2, BrokerClusterChangeStatus.COMPLETED));
      assertTrue(changes.changePlan("same:cluster", "first").isEmpty());
      assertTrue(changes.clusterChangeOperation("same:cluster", "parent-first").isEmpty());
      assertTrue(
          state
              .getTopologyMembershipChangeState()
              .tasks(Scope.BROKER_PARTITION, "same:cluster", "first")
              .isEmpty());
      store.getCurrentTransaction().rollback();
      assertTrue(changes.changePlan("same:cluster", "first").isPresent());
      assertTrue(changes.clusterChangeOperation("same:cluster", "parent-first").isPresent());
      assertEquals(
          3,
          state
              .getTopologyMembershipChangeState()
              .tasks(Scope.BROKER_PARTITION, "same:cluster", "first")
              .size());
      assertTrue(changes.isOperationApplied("same:cluster", first.operations().getFirst().opId()));
      assertTrue(changes.changePlan("same:cluster", "next").isEmpty());
    }
  }

  @Test
  void terminalParentKeepsActiveSuccessorUntilTheWholeChainFinishes() {
    try (var storage = new PartitionStorage(directory)) {
      final var state = limitedState(storage);
      final var changes = state.getBrokerTopologyChangeState();
      final var root =
          plan("root")
              .withExecution(
                  new ChangePlan.Execution(
                      1, 1, "", "successor", "root", PlanBlockedReason.NONE, 0));
      final var successor =
          plan("successor")
              .withExecution(
                  new ChangePlan.Execution(2, 2, "root", "", "root", PlanBlockedReason.NONE, 0));
      changes.putClusterChangeOperation(
          cluster("parent", "root", 1, BrokerClusterChangeStatus.PENDING));
      changes.putChangePlan(root.withStatus(BrokerPartitionChangeStatus.SUPERSEDED));
      changes.putChangePlan(successor);
      changes.markPending(successor);
      changes.putClusterChangeOperation(
          cluster("parent", "root", 1, BrokerClusterChangeStatus.COMPLETED));
      changes.putClusterChangeOperation(
          cluster("new-parent", "no-children", 2, BrokerClusterChangeStatus.COMPLETED));
      assertTrue(changes.clusterChangeOperation("same:cluster", "parent").isPresent());
      assertTrue(changes.changePlan("same:cluster", "root").isPresent());
      assertEquals(
          "successor",
          state
              .getTopologyMembershipChangeState()
              .partition("same:cluster", 1)
              .orElseThrow()
              .plan()
              .changeId());
      changes.putChangePlan(successor.advanced().advanced());
      changes.removePending("same:cluster", 1);
      changes.putClusterChangeOperation(
          cluster("new-parent", "no-children", 2, BrokerClusterChangeStatus.COMPLETED));
      assertTrue(changes.clusterChangeOperation("same:cluster", "parent").isEmpty());
      assertTrue(changes.changePlan("same:cluster", "root").isEmpty());
      assertTrue(changes.changePlan("same:cluster", "successor").isEmpty());
      assertTrue(
          state
              .getTopologyMembershipChangeState()
              .tasks(Scope.BROKER_PARTITION, "same:cluster", "root")
              .isEmpty());
      assertEquals(-1, state.getTopologyMembershipChangeState().nextDueAt());
    }
  }

  @Test
  void retainedMemberReferenceProtectsPartitionHistoryFromClusterPruning() {
    try (var storage = new PartitionStorage(directory)) {
      final var state = limitedState(storage);
      final var changes = state.getBrokerTopologyChangeState();
      changes.putClusterChangeOperation(
          cluster("parent", "child", 1, BrokerClusterChangeStatus.PENDING));
      changes.putChangePlan(plan("child").advanced().advanced());
      final var member =
          member("member", MemberChangeScope.BROKER).setChildPartitionChangeIds(List.of("child"));
      state.getMemberChangeState().put(member);
      state.getMemberChangeState().setActive(member);
      changes.putClusterChangeOperation(
          cluster("parent", "child", 1, BrokerClusterChangeStatus.COMPLETED));
      changes.putClusterChangeOperation(
          cluster("new-parent", "empty", 2, BrokerClusterChangeStatus.COMPLETED));
      assertTrue(changes.clusterChangeOperation("same:cluster", "parent").isPresent());
      assertTrue(changes.changePlan("same:cluster", "child").isPresent());
      assertEquals(
          3,
          state
              .getTopologyMembershipChangeState()
              .tasks(Scope.BROKER_PARTITION, "same:cluster", "child")
              .size());
    }
  }

  @Test
  void memberHistoryCascadesChildPlansAndTheirUniqueOperationDedupe() {
    try (var storage = new PartitionStorage(directory)) {
      final var state = limitedState(storage);
      final var changes = state.getBrokerTopologyChangeState();
      final var members = state.getMemberChangeState();
      final var first =
          member("first", MemberChangeScope.BROKER).setChildPartitionChangeIds(List.of("child"));
      members.put(first);
      members.setActive(first);
      final var child = plan("child");
      changes.putChangePlan(child.advanced().advanced());
      changes.markOperationApplied("same:cluster", child.operations().getFirst().opId());
      members.put(first.copy().setStatus(MemberChangeStatus.COMPLETED).setNextOperationIndex(1));
      members.clearActive(MemberChangeScope.BROKER, "same:cluster");
      final var next = member("next", MemberChangeScope.BROKER).setRequestedAtEpochMillis(2);
      members.put(next.copy().setStatus(MemberChangeStatus.COMPLETED).setNextOperationIndex(1));
      assertTrue(members.get(MemberChangeScope.BROKER, "same:cluster", "first").isEmpty());
      assertTrue(changes.changePlan("same:cluster", "child").isEmpty());
      assertFalse(changes.isOperationApplied("same:cluster", child.operations().getFirst().opId()));
      assertTrue(
          state
              .getTopologyMembershipChangeState()
              .tasks(Scope.BROKER_PARTITION, "same:cluster", "child")
              .isEmpty());
      assertTrue(
          state
              .getTopologyMembershipChangeState()
              .tasks(Scope.BROKER_MEMBER, "same:cluster", "first")
              .isEmpty());
      assertEquals(-1, state.getTopologyMembershipChangeState().nextDueAt());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void pairedClusterAndMemberHistoryPrunesTogetherFromEitherParent(final boolean pruneFromCluster) {
    try (var storage = new PartitionStorage(directory)) {
      final var state = limitedState(storage);
      final var changes = state.getBrokerTopologyChangeState();
      final var members = state.getMemberChangeState();
      final var old =
          member("paired-old", MemberChangeScope.BROKER)
              .setChildPartitionChangeIds(List.of("child"));
      members.put(old);
      members.setActive(old);
      changes.putClusterChangeOperation(
          cluster("paired-old", "child", 1, BrokerClusterChangeStatus.PENDING));
      changes.putChangePlan(plan("child").advanced().advanced());
      changes.putClusterChangeOperation(
          cluster("paired-old", "child", 1, BrokerClusterChangeStatus.COMPLETED));
      members.put(old.copy().setNextOperationIndex(1).setStatus(MemberChangeStatus.COMPLETED));
      members.clearActive(MemberChangeScope.BROKER, "same:cluster");
      final var next = member("paired-next", MemberChangeScope.BROKER).setRequestedAtEpochMillis(2);
      members.put(next);
      members.setActive(next);
      changes.putClusterChangeOperation(
          cluster(
              "paired-next",
              "empty",
              2,
              pruneFromCluster
                  ? BrokerClusterChangeStatus.COMPLETED
                  : BrokerClusterChangeStatus.PENDING));
      if (!pruneFromCluster) {
        members.put(next.copy().setNextOperationIndex(1).setStatus(MemberChangeStatus.COMPLETED));
        members.clearActive(MemberChangeScope.BROKER, "same:cluster");
      }
      assertTrue(changes.clusterChangeOperation("same:cluster", "paired-old").isEmpty());
      assertTrue(members.get(MemberChangeScope.BROKER, "same:cluster", "paired-old").isEmpty());
      assertTrue(changes.changePlan("same:cluster", "child").isEmpty());
      assertTrue(
          state
              .getTopologyMembershipChangeState()
              .tasks(Scope.BROKER_MEMBER, "same:cluster", "paired-old")
              .isEmpty());
      assertTrue(
          state
              .getTopologyMembershipChangeState()
              .tasks(Scope.BROKER_PARTITION, "same:cluster", "child")
              .isEmpty());
      assertTrue(changes.clusterChangeOperation("same:cluster", "paired-next").isPresent());
      assertTrue(members.get(MemberChangeScope.BROKER, "same:cluster", "paired-next").isPresent());
      if (pruneFromCluster) {
        assertEquals(
            "paired-next",
            members.active(MemberChangeScope.BROKER, "same:cluster").orElseThrow().getChangeId());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void historyKeepsPairedParentsWhileEitherSideIsStillActive(final boolean activeMember) {
    try (var storage = new PartitionStorage(directory)) {
      final var state = limitedState(storage);
      final var changes = state.getBrokerTopologyChangeState();
      final var members = state.getMemberChangeState();
      final var old =
          member("paired-old", MemberChangeScope.BROKER)
              .setChildPartitionChangeIds(List.of("child"));
      members.put(old);
      members.setActive(old);
      changes.putClusterChangeOperation(
          cluster("paired-old", "child", 1, BrokerClusterChangeStatus.PENDING));
      changes.putChangePlan(plan("child").advanced().advanced());
      if (activeMember) {
        changes.putClusterChangeOperation(
            cluster("paired-old", "child", 1, BrokerClusterChangeStatus.COMPLETED));
        changes.putClusterChangeOperation(
            cluster("next", "empty", 2, BrokerClusterChangeStatus.COMPLETED));
      } else {
        members.put(old.copy().setStatus(MemberChangeStatus.COMPLETED).setNextOperationIndex(1));
        members.clearActive(MemberChangeScope.BROKER, "same:cluster");
        members.put(
            member("next", MemberChangeScope.BROKER)
                .setStatus(MemberChangeStatus.COMPLETED)
                .setRequestedAtEpochMillis(2)
                .setNextOperationIndex(1));
      }
      assertTrue(changes.clusterChangeOperation("same:cluster", "paired-old").isPresent());
      assertTrue(members.get(MemberChangeScope.BROKER, "same:cluster", "paired-old").isPresent());
      assertTrue(changes.changePlan("same:cluster", "child").isPresent());
      assertEquals(
          3,
          state
              .getTopologyMembershipChangeState()
              .tasks(Scope.BROKER_PARTITION, "same:cluster", "child")
              .size());
    }
  }

  private static MutableProcessingState limitedState(final PartitionStorage storage) {
    return new MutableProcessingState(new StateStoreProcessingStore(storage), () -> 0, () -> 0, 1);
  }

  private static ClusterChangeOperation cluster(
      final String id, final String child, final long at, final BrokerClusterChangeStatus status) {
    return new ClusterChangeOperation(
        "same:cluster",
        id,
        1,
        1,
        1,
        1,
        List.of("a"),
        List.of(),
        List.of("a"),
        List.of(
            new ClusterChangeOperation.ExpectedPartition(
                1, List.of("a"), List.of("a"), List.of(), "a", Map.of("a", 100))),
        List.of(child),
        status,
        at,
        at + 1);
  }

  private static MutableProcessingState state(final PartitionStorage storage) {
    return new MutableProcessingState(new StateStoreProcessingStore(storage));
  }

  private static ChangePlan plan(final String id) {
    return new ChangePlan(
        new ChangePlan.Definition(
            new ChangePlan.Identity("same:cluster", id, 1),
            new ChangePlan.Versions(1, 2),
            List.of(TopologyOperation.join(1, "a"), TopologyOperation.join(1, "b"))),
        new ChangePlan.Progress(0, BrokerPartitionChangeStatus.IN_PROGRESS),
        ChangePlan.Deadline.NONE);
  }

  private static MemberChangeRecord member(final String id, final MemberChangeScope scope) {
    return new MemberChangeRecord()
        .setScope(scope)
        .setResourceId("same:cluster")
        .setChangeId(id)
        .setRequestedAtEpochMillis(1)
        .setStatus(MemberChangeStatus.IN_PROGRESS)
        .setOperations(
            List.of(
                new MemberChangeOperation()
                    .setOperationId("member-join")
                    .setType(MemberChangeOperationType.MEMBER_ADD_LEARNER)
                    .setMemberId("a")
                    .setStatus(MemberChangeOperationStatus.IN_PROGRESS)));
  }
}
