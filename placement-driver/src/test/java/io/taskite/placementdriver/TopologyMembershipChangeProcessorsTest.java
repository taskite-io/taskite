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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.taskite.placementdriver.broker.cluster.BrokerOperationConfiguration;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.processing.topology.MemberExecuteProcessor;
import io.taskite.placementdriver.processing.topology.TopologyMembershipChangeBehavior;
import io.taskite.placementdriver.processing.topology.TopologyMembershipChangeExecutor;
import io.taskite.placementdriver.processing.topology.TopologyMembershipChangeProcessors;
import io.taskite.placementdriver.state.appliers.PlacementDriverEventAppliers;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperation;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationStatus;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationType;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyChangeTaskRecord.Scope;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyChangeTaskRecord.Status;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;
import io.taskite.streamplatform.api.writer.StateWriter;
import io.taskite.streamplatform.api.writer.TypedCommandWriter;
import io.taskite.streamplatform.api.writer.TypedRejectionWriter;
import io.taskite.streamplatform.api.writer.Writers;
import io.taskite.topology.BrokerPartitionElectionState;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import io.taskite.topology.RaftConfigurationReference;
import io.taskite.topology.RaftNodeState;
import io.taskite.topology.RecoveryFence;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

final class TopologyMembershipChangeProcessorsTest {
  @TempDir Path directory;

  @ParameterizedTest
  @CsvSource({
    "JOIN,JOIN,JOINED",
    "LEAVE,LEAVE,LEFT",
    "ADD,CREATE,CREATED",
    "PROMOTE_LEARNER,PROMOTE,PROMOTED",
    "RECONFIGURE_PRIORITY,CHANGE_PRIORITY,PRIORITY_CHANGED",
    "TRANSFER_LEADER,TRANSFER_LEADER,LEADER_TRANSFERRED",
    "STEP_DOWN_IF_NOT_PRIMARY,STEP_DOWN_LEADER,LEADER_STEPPED_DOWN",
    "RESET_PEERS,RESET_PEERS,PEERS_RESET"
  })
  void partitionOperationsRequireCurrentVerifiedEvidenceThenApplyOnlyTheirEvent(
      final TopologyOperationType type,
      final BrokerPartitionChangeIntent command,
      final BrokerPartitionChangeIntent event) {
    try (var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      final var fixture = new Fixture(state);
      final var source =
          List.of(
              new BrokerPartitionMember("a", true, 100),
              new BrokerPartitionMember("b", false, 0),
              new BrokerPartitionMember("c", true, 10));
      final List<BrokerPartitionMember> target =
          switch (type) {
            case JOIN, TRANSFER_LEADER, STEP_DOWN_IF_NOT_PRIMARY -> source;
            case LEAVE -> List.of(source.get(0), source.get(2));
            case ADD, PROMOTE_LEARNER ->
                List.of(source.get(0), new BrokerPartitionMember("b", true, 10), source.get(2));
            case RECONFIGURE_PRIORITY ->
                List.of(new BrokerPartitionMember("a", true, 90), source.get(1), source.get(2));
            case RESET_PEERS -> List.of(source.get(0));
          };
      final long epoch = type == TopologyOperationType.RESET_PEERS ? 1 : 0;
      final var operation =
          new TopologyOperation(
              "operation",
              type,
              switch (type) {
                case JOIN, LEAVE, ADD, PROMOTE_LEARNER -> "b";
                default -> "a";
              },
              type == TopologyOperationType.RESET_PEERS
                  ? 1
                  : type == TopologyOperationType.RECONFIGURE_PRIORITY ? 90 : 0,
              new BrokerOperationConfiguration(source, target, 5, new RecoveryFence(0, epoch, "")));
      final var plan =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "change", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(operation)),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.IN_PROGRESS),
              ChangePlan.Deadline.NONE);
      state.getBrokerTopologyChangeState().putChangePlan(plan);
      state.getBrokerTopologyChangeState().markPending(plan);
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(1, List.of("a", "c"), "a"),
                  new PartitionTopology.Membership(
                      List.of("a", "c"), List.of("b"), Map.of("a", 100, "b", 0, "c", 10), 0),
                  PartitionStatus.ACTIVE,
                  Map.of()));
      final var value = BrokerPartitionChangeRecordMapper.fromPlan(plan);
      fixture.process(ValueType.PARTITION_CHANGE, command, value);
      assertTrue(
          fixture.events.isEmpty(), "RPC success without a verified report must not complete");
      final var report =
          new BrokerPartitionState(
              1,
              new BrokerPartitionState.Status(
                  MemberRole.LEADER, 1, BrokerPartitionLifecycle.ACTIVE, 5, epoch),
              new BrokerPartitionState.Replication(5, 0, false),
              new BrokerPartitionState.Operation(
                  plan.nextOperation().opId(), BrokerTopologyOperationStatus.APPLIED),
              new BrokerPartitionElectionState(
                  new RaftConfigurationReference(5, operation.configuration().targetDigest("demo")),
                  new BrokerPartitionElectionState.Priorities(100, 100),
                  new BrokerPartitionElectionState.Observation(
                      RaftNodeState.STATE_LEADER, "a", true, 1)));
      state
          .getBrokerRegistrationState()
          .registerBroker(
              "demo",
              "a",
              "localhost",
              26001,
              26101,
              "test",
              30000,
              Instant.ofEpochMilli(1),
              List.of(report),
              "incarnation");
      fixture.process(
          ValueType.PARTITION_CHANGE,
          command,
          BrokerPartitionChangeRecordMapper.fromPlan(plan).setExecutionGeneration(99));
      assertTrue(fixture.events.isEmpty());
      fixture.process(
          ValueType.PARTITION_CHANGE,
          command == BrokerPartitionChangeIntent.JOIN
              ? BrokerPartitionChangeIntent.LEAVE
              : BrokerPartitionChangeIntent.JOIN,
          value);
      assertTrue(fixture.events.isEmpty(), "a different operation intent must be rejected");
      fixture.process(ValueType.PARTITION_CHANGE, command, value);
      assertEquals(1, fixture.events.size());
      assertEquals(event, fixture.events.getFirst().intent());
      assertEquals(
          0,
          state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "change")
              .orElseThrow()
              .nextOperationIndex(),
          "Processor cannot mutate state");
      fixture.apply();
      final var completed =
          state.getBrokerTopologyChangeState().changePlan("demo", "change").orElseThrow();
      assertEquals(BrokerPartitionChangeStatus.COMPLETED, completed.status());
      assertEquals(1, completed.nextOperationIndex());
      final var topology =
          state.getBrokerPartitionTopologyState().partitionTopology("demo", 1).orElseThrow();
      assertEquals(
          target.stream()
              .filter(BrokerPartitionMember::voter)
              .map(BrokerPartitionMember::brokerId)
              .collect(Collectors.toSet()),
          Set.copyOf(topology.currentVoters()));
      assertEquals(
          target.stream()
              .filter(member -> !member.voter())
              .map(BrokerPartitionMember::brokerId)
              .collect(Collectors.toSet()),
          Set.copyOf(topology.learners()));
      assertEquals(
          target.stream()
              .collect(
                  Collectors.toMap(
                      BrokerPartitionMember::brokerId, BrokerPartitionMember::priority)),
          topology.priorities());
      assertEquals(epoch, topology.recoveryEpoch());
      assertFalse(state.getBrokerTopologyChangeState().hasPendingPlanForPartition("demo", 1));
      assertEquals(
          Status.COMPLETED,
          state
              .getTopologyMembershipChangeState()
              .tasks(Scope.BROKER_PARTITION, "demo", "change")
              .getFirst()
              .status());
      fixture.process(ValueType.PARTITION_CHANGE, command, value);
      assertEquals(1, fixture.events.size(), "duplicate completion must not emit another event");
    }
  }

  @ParameterizedTest
  @EnumSource(MemberChangeOperationType.class)
  void memberOperationsPersistEachCompletionAndRejectStaleOrWrongIntents(
      final MemberChangeOperationType type) {
    for (final var scope : MemberChangeScope.values()) {
      try (var storage = new PartitionStorage(directory.resolve(scope.name()))) {
        final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
        final var fixture = new Fixture(state);
        final var change =
            new MemberChangeRecord()
                .setScope(scope)
                .setResourceId(
                    scope == MemberChangeScope.PLACEMENT_DRIVER
                        ? PlacementDriverCluster.CLUSTER_ID
                        : "demo")
                .setChangeId("change")
                .setSourceEpoch(1)
                .setTargetEpoch(2)
                .setStatus(MemberChangeStatus.IN_PROGRESS)
                .setOperations(
                    List.of(
                        new MemberChangeOperation()
                            .setOperationId("operation")
                            .setMemberId("a")
                            .setType(type)
                            .setStatus(MemberChangeOperationStatus.IN_PROGRESS)));
        state.getMemberChangeState().put(change);
        state.getMemberChangeState().setActive(change);
        final var completed =
            change.copy().setNextOperationIndex(1).setStatus(MemberChangeStatus.COMPLETED);
        completed.getOperations().getFirst().setStatus(MemberChangeOperationStatus.COMPLETED);
        final var command = TopologyMembershipChangeBehavior.command(type);
        fixture.process(ValueType.MEMBER_CHANGE, command, completed.copy().setTargetEpoch(99));
        assertTrue(fixture.events.isEmpty());
        fixture.process(
            ValueType.MEMBER_CHANGE,
            command == MemberChangeIntent.JOIN ? MemberChangeIntent.LEAVE : MemberChangeIntent.JOIN,
            completed);
        assertTrue(fixture.events.isEmpty());
        fixture.process(ValueType.MEMBER_CHANGE, command, completed);
        final var expected =
            switch (command) {
              case JOIN -> MemberChangeIntent.JOINED;
              case LEAVE -> MemberChangeIntent.LEFT;
              case PROMOTE -> MemberChangeIntent.PROMOTED;
              case TRANSFER_LEADER -> MemberChangeIntent.LEADER_TRANSFERRED;
              default -> throw new AssertionError(command);
            };
        assertEquals(1, fixture.events.size());
        assertEquals(expected, fixture.events.getFirst().intent());
        assertEquals(
            0,
            state
                .getMemberChangeState()
                .active(scope, change.getResourceId())
                .orElseThrow()
                .getNextOperationIndex());
        fixture.apply();
        assertTrue(state.getMemberChangeState().active(scope, change.getResourceId()).isEmpty());
        assertEquals(
            MemberChangeStatus.COMPLETED,
            state
                .getMemberChangeState()
                .get(scope, change.getResourceId(), "change")
                .orElseThrow()
                .getStatus());
        final var tasks =
            state
                .getTopologyMembershipChangeState()
                .tasks(
                    scope == MemberChangeScope.BROKER
                        ? Scope.BROKER_MEMBER
                        : Scope.PLACEMENT_DRIVER_MEMBER,
                    change.getResourceId(),
                    "change");
        assertEquals(2, tasks.size());
        assertTrue(tasks.stream().allMatch(task -> task.status() == Status.COMPLETED));
        fixture.process(ValueType.MEMBER_CHANGE, command, completed);
        assertEquals(1, fixture.events.size());
      }
    }
  }

  @Test
  void brokerParentWaitsForChildConvergenceAndCompletesMembersOneAtATime() {
    try (var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      final var fixture = new Fixture(state);
      final var change =
          new MemberChangeRecord()
              .setScope(MemberChangeScope.BROKER)
              .setResourceId("demo")
              .setChangeId("parent")
              .setStatus(MemberChangeStatus.IN_PROGRESS)
              .setChildPartitionChangeIds(List.of("child"))
              .setOperations(
                  List.of(
                      new MemberChangeOperation()
                          .setOperationId("join")
                          .setMemberId("a")
                          .setType(MemberChangeOperationType.MEMBER_JOIN)
                          .setStatus(MemberChangeOperationStatus.IN_PROGRESS),
                      new MemberChangeOperation()
                          .setOperationId("leave")
                          .setMemberId("b")
                          .setType(MemberChangeOperationType.MEMBER_LEAVE)
                          .setStatus(MemberChangeOperationStatus.PENDING)));
      state.getMemberChangeState().put(change);
      state.getMemberChangeState().setActive(change);
      final var child =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "child", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of()),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.IN_PROGRESS),
              ChangePlan.Deadline.NONE);
      state.getBrokerTopologyChangeState().putChangePlan(child);
      final var executor =
          new TopologyMembershipChangeExecutor(
              fixture.context,
              () -> true,
              null,
              null,
              Duration.ofSeconds(15),
              Duration.ofSeconds(15));
      executor.onRecovered(mock(ProcessingScheduleService.class));
      final var processor = new MemberExecuteProcessor(fixture.context, executor);
      processor.processRecord(fixture.record(change, 1000));
      fixture.apply();
      assertTrue(fixture.commands.isEmpty());
      state
          .getBrokerTopologyChangeState()
          .putChangePlan(child.withStatus(BrokerPartitionChangeStatus.COMPLETED));
      processor.processRecord(fixture.record(change, 2000));
      assertEquals(1, fixture.commands.size());
      assertEquals(MemberChangeIntent.JOIN, fixture.commands.getFirst().intent());
      fixture.apply();
      fixture.process(
          ValueType.MEMBER_CHANGE,
          fixture.commands.getFirst().intent(),
          (MemberChangeRecord) fixture.commands.getFirst().value());
      fixture.apply();
      final var halfway =
          state.getMemberChangeState().active(MemberChangeScope.BROKER, "demo").orElseThrow();
      assertEquals(1, halfway.getNextOperationIndex());
      assertEquals(MemberChangeStatus.IN_PROGRESS, halfway.getStatus());
      assertEquals(
          MemberChangeOperationStatus.COMPLETED, halfway.getOperations().getFirst().getStatus());
      processor.processRecord(fixture.record(halfway, 3000));
      assertEquals(2, fixture.commands.size());
      assertEquals(MemberChangeIntent.LEAVE, fixture.commands.getLast().intent());
      fixture.apply();
      fixture.process(
          ValueType.MEMBER_CHANGE,
          fixture.commands.getLast().intent(),
          (MemberChangeRecord) fixture.commands.getLast().value());
      fixture.apply();
      assertTrue(state.getMemberChangeState().active(MemberChangeScope.BROKER, "demo").isEmpty());
      final var tasks =
          state.getTopologyMembershipChangeState().tasks(Scope.BROKER_MEMBER, "demo", "parent");
      assertEquals(3, tasks.size());
      assertTrue(tasks.stream().allMatch(task -> task.status() == Status.COMPLETED));
      executor.onClose();
    }
  }

  private record Event(Intent intent, UnifiedRecordValue value) {}

  private static final class Fixture {
    final MutableProcessingState state;
    final List<Event> events = new ArrayList<>();
    final List<Event> commands = new ArrayList<>();
    final TypedRecordProcessors processors = TypedRecordProcessors.processors();
    final TypedRecordProcessorContext context = mock(TypedRecordProcessorContext.class);

    Fixture(final MutableProcessingState state) {
      this.state = state;
      final var writers = mock(Writers.class);
      final var stateWriter = mock(StateWriter.class);
      when(context.getProcessingState(ProcessingState.class)).thenReturn(state);
      when(context.getWriters()).thenReturn(writers);
      when(context.getKeyGenerator()).thenReturn(() -> 1);
      when(writers.state()).thenReturn(stateWriter);
      when(writers.rejection()).thenReturn(mock(TypedRejectionWriter.class));
      final var commandWriter = mock(TypedCommandWriter.class);
      when(writers.command()).thenReturn(commandWriter);
      doAnswer(
              call -> {
                commands.add(new Event(call.getArgument(2), call.getArgument(3)));
                return null;
              })
          .when(commandWriter)
          .appendFollowUpCommand(
              anyLong(), any(ValueType.class), any(Intent.class), any(UnifiedRecordValue.class));
      doAnswer(
              call -> {
                events.add(new Event(call.getArgument(1), call.getArgument(2)));
                return null;
              })
          .when(stateWriter)
          .appendFollowUpEvent(anyLong(), any(Intent.class), any(UnifiedRecordValue.class));
      TopologyMembershipChangeProcessors.register(processors, context);
    }

    <T extends UnifiedRecordValue> void process(
        final ValueType type, final Intent intent, final T value) {
      @SuppressWarnings("unchecked")
      final var processor = (TypedRecordProcessor<T>) processors.get(type, intent, 1);
      assertNotNull(processor);
      processor.processRecord(record(value, 0));
    }

    <T extends UnifiedRecordValue> TypedRecord<T> record(final T value, final long timestamp) {
      @SuppressWarnings("unchecked")
      final TypedRecord<T> record = mock(TypedRecord.class);
      when(record.getValue()).thenReturn(value);
      when(record.getTimestamp()).thenReturn(timestamp);
      return record;
    }

    void apply() {
      final var event = events.getLast();
      PlacementDriverEventAppliers.create(state).applyState(1, event.intent(), event.value(), 1);
    }
  }
}
