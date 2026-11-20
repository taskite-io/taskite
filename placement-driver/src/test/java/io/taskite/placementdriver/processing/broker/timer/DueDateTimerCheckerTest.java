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
package io.taskite.placementdriver.processing.broker.timer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.taskite.broker.client.BrokerClient;
import io.taskite.db.impl.DbCompositeKey;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbNil;
import io.taskite.placementdriver.processing.PlacementDriverProcessorDependencies;
import io.taskite.placementdriver.processing.PlacementDriverProcessors;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.common.TimerIntent;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService.CommandWriter;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class DueDateTimerCheckerTest {
  @TempDir Path directory;

  @Test
  void persistedTemplateKeepsSchedulingDuringRedistributionAndAfterHistoryRemoval()
      throws Exception {
    try (final var fixture = new TimerFixture(directory)) {
      fixture.complete(42, TimerFixture.once(), TimerFixture.NOW);
      fixture.now.set(TimerFixture.NOW + 1000);
      fixture.state.getCommandDistributionState().markPending(42, "demo", 1);
      fixture.checker.check(fixture.commandWriter);
      assertEquals(1, fixture.commands.size());
      fixture.commands.clear();
      fixture.state.getCommandDistributionState().deleteCluster("demo");
      fixture.checker.check(fixture.commandWriter);
      assertEquals(1, fixture.commands.size());
    }
  }

  @Test
  void scansOrderedIndexAndOnlyAppendsDueCommands() throws Exception {
    try (final var fixture = new TimerFixture(directory)) {
      fixture.distribution(42, TimerFixture.ordinary());
      for (final long key : List.of(9L, 2L, 7L)) {
        fixture
            .state
            .getTimerSubscriptionState()
            .put(key, timer(TimerFixture.NOW + (key == 7 ? 2000 : 1000)));
      }
      fixture.now.set(TimerFixture.NOW + 1000);
      assertEquals(TimerFixture.NOW + 2000, fixture.checker.check(fixture.commandWriter));
      assertEquals(
          List.of(2L, 9L), fixture.commands.stream().map(command -> command.key()).toList());
      assertEquals(3, fixture.state.getTimerSubscriptionState().subscriptions().size());
      assertTrue(fixture.requests.isEmpty(), "checker never sends Broker RPC");
      fixture.state.getTimerSubscriptionState().remove(7, timer(TimerFixture.NOW + 2000));
      fixture.commands.clear();
      assertEquals(-1, fixture.checker.check(fixture.commandWriter));
      assertEquals(
          List.of(2L, 9L), fixture.commands.stream().map(command -> command.key()).toList());
    }
  }

  @Test
  void backpressureReturnsFirstUnconsumedDeadlineWithoutDeletingState() throws Exception {
    try (final var fixture = new TimerFixture(directory)) {
      fixture.distribution(42, TimerFixture.ordinary());
      fixture.state.getTimerSubscriptionState().put(1, timer(TimerFixture.NOW));
      fixture.state.getTimerSubscriptionState().put(2, timer(TimerFixture.NOW + 1000));
      fixture.capacity.set(false);
      assertEquals(TimerFixture.NOW, fixture.checker.check(fixture.commandWriter));
      assertTrue(fixture.commands.isEmpty());
      fixture.capacity.set(true);
      assertEquals(TimerFixture.NOW + 1000, fixture.checker.check(fixture.commandWriter));
      assertEquals(1, fixture.commands.size());
      assertEquals(2, fixture.state.getTimerSubscriptionState().subscriptions().size());
    }
  }

  @Test
  void ordinaryDistributionDoesNotGateIndependentTimer() throws Exception {
    try (final var fixture = new TimerFixture(directory)) {
      fixture.distribution(42, TimerFixture.ordinary());
      fixture.state.getTimerSubscriptionState().put(1, timer(TimerFixture.NOW));
      fixture.state.getCommandDistributionState().markPending(42, "demo", 1);
      assertEquals(-1, fixture.checker.check(fixture.commandWriter));
      assertEquals(1, fixture.commands.size());
      fixture.commands.clear();
      fixture.state.getCommandDistributionState().acknowledge(42, "demo", 1);
      fixture.checker.check(fixture.commandWriter);
      assertEquals(1, fixture.commands.size());
    }
  }

  @Test
  void removalDeletesPrimaryAndDueDateEntriesAndRollbackRestoresBoth() throws Exception {
    try (final var fixture = new TimerFixture(directory)) {
      fixture.distribution(42, TimerFixture.ordinary());
      final var timer = timer(TimerFixture.NOW);
      fixture.state.getTimerSubscriptionState().put(1, timer);
      fixture.state.getTimerSubscriptionState().put(2, timer);
      final var index =
          fixture.store.createColumnFamily(
              ColumnFamilies.PD_TIMER_DUE_DATES,
              new DbCompositeKey<>(new DbLong(), new DbLong()),
              DbNil.INSTANCE);
      final List<Long> keys = new ArrayList<>();
      index.forEach((key, nil) -> keys.add(key.second().getValue()));
      assertEquals(List.of(1L, 2L), keys);
      fixture.store.getCurrentTransaction().commit();
      fixture.store.getCurrentTransaction();
      fixture.state.getTimerSubscriptionState().remove(1, timer);
      keys.clear();
      index.forEach((key, nil) -> keys.add(key.second().getValue()));
      assertEquals(List.of(2L), keys);
      assertTrue(fixture.state.getTimerSubscriptionState().get(1).isEmpty());
      fixture.store.getCurrentTransaction().rollback();
      keys.clear();
      index.forEach((key, nil) -> keys.add(key.second().getValue()));
      assertEquals(List.of(1L, 2L), keys);
      assertTrue(fixture.state.getTimerSubscriptionState().get(1).isPresent());
      fixture.state.getTimerSubscriptionState().deleteCluster("demo");
      index.forEach((key, nil) -> fail("cluster deletion left a due-date index entry"));
    }
  }

  @Test
  void storesTheDueDateDirectlyInTheIndex() throws Exception {
    try (final var fixture = new TimerFixture(directory)) {
      fixture.distribution(42, TimerFixture.ordinary());
      fixture.state.getTimerSubscriptionState().put(1, timer(TimerFixture.NOW));
      fixture.state.getTimerSubscriptionState().put(2, timer(TimerFixture.NOW + 1000));
      final var index =
          fixture.store.createColumnFamily(
              ColumnFamilies.PD_TIMER_DUE_DATES,
              new DbCompositeKey<>(new DbLong(), new DbLong()),
              DbNil.INSTANCE);
      final List<Long> dueDates = new ArrayList<>();
      index.forEach((key, nil) -> dueDates.add(key.first().getValue()));
      assertEquals(List.of(TimerFixture.NOW, TimerFixture.NOW + 1000), dueDates);
      assertEquals(TimerFixture.NOW + 1000, fixture.checker.check(fixture.commandWriter));
      assertEquals(List.of(1L), fixture.commands.stream().map(command -> command.key()).toList());
    }
  }

  @Test
  void epochZeroIsRetriedOnBackpressureInsteadOfBeingTreatedAsIdle() throws Exception {
    try (final var fixture = new TimerFixture(directory)) {
      fixture.distribution(42, TimerFixture.ordinary());
      fixture.state.getTimerSubscriptionState().put(1, timer(0));
      fixture.capacity.set(false);
      assertEquals(0, fixture.checker.check(fixture.commandWriter));
      fixture.capacity.set(true);
      assertEquals(-1, fixture.checker.check(fixture.commandWriter));
      assertEquals(1, fixture.commands.size());
    }
  }

  @Test
  void recoveryScansImmediatelyThenSchedulesOnlyTheEarliestDueDate() {
    final var state = mock(ProcessingState.class, RETURNS_DEEP_STUBS);
    final var now = new AtomicLong(1000);
    when(state.getTimerSubscriptionState().processTimersWithDueDateBefore(anyLong(), any()))
        .thenReturn(10000L, -1L);
    final var scheduling = new Scheduling();
    final var checker = new DueDateTimerChecker(state, now::get);
    checker.onRecovered(scheduling);
    assertEquals(List.of(Duration.ZERO), scheduling.delays);
    scheduling.fire(0);
    assertEquals(Duration.ofMillis(9000), scheduling.delays.get(1));
    checker.scheduleTimer(15000);
    assertEquals(2, scheduling.tasks.size());
    checker.scheduleTimer(2000);
    assertEquals(Duration.ofMillis(1000), scheduling.delays.get(2));
    now.set(2000);
    scheduling.fire(2);
    assertEquals(3, scheduling.tasks.size(), "empty state stops scheduling");
    checker.onClose();
    scheduling.fire(1);
    assertEquals(3, scheduling.tasks.size(), "cancelled task cannot reschedule");
  }

  @Test
  void queriesDoNotWakeAndOverdueTriggerUsesMinimumResolution() {
    final var state = mock(ProcessingState.class, RETURNS_DEEP_STUBS);
    when(state.getTimerSubscriptionState().processTimersWithDueDateBefore(anyLong(), any()))
        .thenReturn(-1L);
    final var scheduling = new Scheduling();
    final var checker = new DueDateTimerChecker(state, () -> 1000);
    checker.onRecovered(scheduling);
    scheduling.fire(0);
    final var command = mock(TypedRecord.class);
    when(command.getValueType()).thenReturn(ValueType.COMMAND_DISTRIBUTION);
    when(command.getIntent()).thenReturn(CommandDistributionIntent.QUERY);
    checker.onProcessed(command);
    when(command.getIntent()).thenReturn(CommandDistributionIntent.LIST);
    checker.onProcessed(command);
    assertEquals(1, scheduling.tasks.size());
    when(command.getValueType()).thenReturn(ValueType.TIMER);
    when(command.getIntent()).thenReturn(TimerIntent.TRIGGER);
    when(command.getValue()).thenReturn(timer(100));
    checker.onProcessed(command);
    assertEquals(Duration.ofMillis(100), scheduling.delays.get(1));
  }

  @Test
  void factoryRegistersCheckerAndTriggerProcessorWithoutOpeningClient() {
    final var context = mock(TypedRecordProcessorContext.class, RETURNS_DEEP_STUBS);
    final var state = mock(MutableProcessingState.class, RETURNS_DEEP_STUBS);
    when(context.getProcessingState(MutableProcessingState.class)).thenReturn(state);
    when(context.getProcessingState(ProcessingState.class)).thenReturn(state);
    @SuppressWarnings("unchecked")
    final Supplier<BrokerClient> clients = mock(Supplier.class);
    final var processors =
        PlacementDriverProcessors.createPlacementDriverProcessors(
            context, PlacementDriverProcessorDependencies.create(), clients, () -> false);
    assertEquals(
        1,
        processors.lifecycleListeners().stream()
            .filter(DueDateTimerChecker.class::isInstance)
            .count());
    assertEquals(
        1,
        processors.lifecycleListeners().stream()
            .filter(TimerTriggerProcessor.class::isInstance)
            .count());
    verifyNoInteractions(clients);
  }

  private static TimerSubscriptionRecord timer(final long dueDate) {
    return new TimerSubscriptionRecord()
        .setDueDate(dueDate)
        .setClusterId("demo")
        .setTenantId("tenant")
        .setRepetitions(1);
  }

  private static final class Scheduling implements ProcessingScheduleService {
    final List<Consumer<CommandWriter>> tasks = new ArrayList<>();
    final List<Duration> delays = new ArrayList<>();

    @Override
    public ScheduledTimer schedule(final Duration delay, final Consumer<CommandWriter> task) {
      delays.add(delay);
      tasks.add(task);
      return mock(ScheduledTimer.class);
    }

    void fire(final int index) {
      tasks.get(index).accept((key, type, intent, value) -> {});
    }
  }
}
