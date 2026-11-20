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
package io.taskite.placementdriver.processing.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.RegistrationPolicy;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.BrokerIntent;
import io.taskite.protocol.record.intent.placementdriver.WorkerIntent;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class RegistrationDeadlineCheckerTest {
  private static final Instant T0 = Instant.ofEpochMilli(1_000);
  private static final RegistrationPolicy REGISTRATION_POLICY =
      new RegistrationPolicy(
          Duration.ofMillis(100), Duration.ofMillis(200), Duration.ofMillis(300));

  @TempDir Path directory;

  @Test
  void drainsSimultaneouslyExpiredWorkersWithSingleCommandBudget() {
    try (var storage = new PartitionStorage(directory)) {
      final var state =
          new MutableProcessingState(
              new StateStoreProcessingStore(storage), () -> 0, () -> 0, 50, REGISTRATION_POLICY);
      for (int i = 0; i < 3; i++) {
        state
            .getWorkerRegistrationState()
            .registerWorker(worker("worker-" + i, 10_000), "tenant-a", T0);
      }
      final var scheduling = new Scheduling();
      final var checker = new RegistrationDeadlineChecker(state, REGISTRATION_POLICY, () -> 1_300);
      checker.onRecovered(scheduling);
      final List<String> expired = new ArrayList<>();
      for (int round = 0; round < 3; round++) {
        final List<WorkerRegistrationRecord> commands = new ArrayList<>();
        scheduling.fire(
            new ProcessingScheduleService.CommandWriter() {
              @Override
              public int remainingCapacity() {
                return 1 - commands.size();
              }

              @Override
              public void append(
                  long key, ValueType type, Intent intent, UnifiedRecordValue value) {
                assertTrue(commands.isEmpty());
                assertEquals(WorkerIntent.MARK_OFFLINE, intent);
                commands.add((WorkerRegistrationRecord) value);
              }
            });
        assertEquals(1, commands.size());
        assertEquals(Duration.ofMillis(100), scheduling.delay);
        final var command = commands.getFirst();
        assertEquals(T0.toEpochMilli(), command.getExpectedLastRenewalEpochMillis());
        expired.add(command.getWorkerId());
        final var registration =
            state.getWorkerRegistrationState().registrations().stream()
                .filter(worker -> worker.workerId().equals(command.getWorkerId()))
                .findFirst()
                .orElseThrow();
        state
            .getWorkerRegistrationState()
            .markWorkerOffline(registration.key(), Instant.ofEpochMilli(1_300));
      }
      assertEquals(3, expired.stream().distinct().count());
      scheduling.fire(
          (key, type, intent, value) -> {
            throw new AssertionError("Retention deadline is not due yet");
          });
      assertEquals(Duration.ofMillis(300), scheduling.delay);
      checker.onClose();
      assertTrue(scheduling.tasks.isEmpty());
    }
  }

  @Test
  void oneTimerEmitsAllBrokerWorkerAndRetentionCommandsWithFences() {
    try (var storage = new PartitionStorage(directory)) {
      final var state =
          new MutableProcessingState(
              new StateStoreProcessingStore(storage), () -> 0, () -> 0, 50, REGISTRATION_POLICY);
      state.getBrokerClusterState().remember("demo", 1);
      state
          .getBrokerRegistrationState()
          .registerBroker(
              "demo", "broker-0", "127.0.0.1", 26501, 26502, "1", 10_000, T0, List.of());
      state.getWorkerRegistrationState().registerWorker(worker("online", 10_000), "tenant-a", T0);
      state.getWorkerRegistrationState().registerWorker(worker("offline", 10_000), "tenant-a", T0);
      final var offline =
          state.getWorkerRegistrationState().registrations().stream()
              .filter(registration -> registration.workerId().equals("offline"))
              .findFirst()
              .orElseThrow();
      state.getWorkerRegistrationState().markWorkerOffline(offline.key(), T0);

      final var clock = new AtomicLong(1_300);
      final var scheduling = new Scheduling();
      final var checker = new RegistrationDeadlineChecker(state, REGISTRATION_POLICY, clock::get);
      checker.onRecovered(scheduling);
      assertEquals(1, scheduling.tasks.size(), "all registrations must share one timer");

      final List<WrittenCommand> commands = new ArrayList<>();
      scheduling.fire(
          (key, type, intent, value) -> commands.add(new WrittenCommand(type, intent, value)));

      assertEquals(3, commands.size());
      assertEquals(
          List.of(BrokerIntent.MARK_OFFLINE, WorkerIntent.MARK_OFFLINE, WorkerIntent.DELETE),
          commands.stream().map(WrittenCommand::intent).toList());
      final var broker = (BrokerRegistrationRecord) commands.get(0).value();
      assertEquals(T0.toEpochMilli(), broker.getExpectedLastRenewalEpochMillis());
      final var worker = (WorkerRegistrationRecord) commands.get(1).value();
      assertEquals(T0.toEpochMilli(), worker.getExpectedLastRenewalEpochMillis());
      final var deleted = (WorkerRegistrationRecord) commands.get(2).value();
      assertEquals(T0.toEpochMilli(), deleted.getExpectedStatusChangedAtEpochMillis());
      assertEquals(1, scheduling.tasks.size(), "checker must re-arm only one timer");

      checker.onClose();
      assertTrue(scheduling.tasks.isEmpty());
    }
  }

  @Test
  void recoveryWaitsForActualDeadlineAndStopsWhenRegistrationsAreRemoved() {
    final var policy =
        new RegistrationPolicy(
            Duration.ofSeconds(10), Duration.ofSeconds(20), Duration.ofSeconds(30));
    try (var storage = new PartitionStorage(directory)) {
      final var state =
          new MutableProcessingState(
              new StateStoreProcessingStore(storage), () -> 0, () -> 0, 50, policy);
      state.getWorkerRegistrationState().registerWorker(worker("worker", 10_000), "tenant-a", T0);
      final var clock = new AtomicLong(T0.toEpochMilli());
      final var scheduling = new Scheduling();
      final var checker = new RegistrationDeadlineChecker(state, policy, clock::get);
      checker.onRecovered(scheduling);
      assertEquals(Duration.ZERO, scheduling.delay);
      scheduling.fire(
          (key, type, intent, value) -> {
            throw new AssertionError("Lease deadline is not due yet");
          });
      assertEquals(Duration.ofSeconds(20), scheduling.delay);
      final var registration = state.getWorkerRegistrationState().registrations().getFirst();
      state.getWorkerRegistrationState().removeWorker(registration.key());
      clock.addAndGet(20_000);
      scheduling.fire(
          (key, type, intent, value) -> {
            throw new AssertionError("Removed registration must not produce commands");
          });
      assertTrue(scheduling.tasks.isEmpty());
      checker.onClose();
    }
  }

  @Test
  void exhaustedBudgetPreservesDueRegistrationForNextRound() {
    try (var storage = new PartitionStorage(directory)) {
      final var state =
          new MutableProcessingState(
              new StateStoreProcessingStore(storage), () -> 0, () -> 0, 50, REGISTRATION_POLICY);
      state.getWorkerRegistrationState().registerWorker(worker("worker", 10_000), "tenant-a", T0);
      final var scheduling = new Scheduling();
      final var checker = new RegistrationDeadlineChecker(state, REGISTRATION_POLICY, () -> 1_300);
      checker.onRecovered(scheduling);
      scheduling.fire(
          new ProcessingScheduleService.CommandWriter() {
            @Override
            public int remainingCapacity() {
              return 0;
            }

            @Override
            public void append(long key, ValueType type, Intent intent, UnifiedRecordValue value) {
              throw new AssertionError("Exhausted writer must not receive commands");
            }
          });
      assertEquals(Duration.ofMillis(100), scheduling.delay);
      assertEquals(1, state.getRegistrationDeadlineState().findDue(1_300, 10).size());
      final List<WrittenCommand> commands = new ArrayList<>();
      scheduling.fire(
          (key, type, intent, value) -> commands.add(new WrittenCommand(type, intent, value)));
      assertEquals(1, commands.size());
      assertEquals(WorkerIntent.MARK_OFFLINE, commands.getFirst().intent());
      checker.onClose();
    }
  }

  @Test
  void recoveryIsolatesAlreadyQueuedCallbacksFromPreviousGeneration() {
    try (var storage = new PartitionStorage(directory)) {
      final var state =
          new MutableProcessingState(
              new StateStoreProcessingStore(storage), () -> 0, () -> 0, 50, REGISTRATION_POLICY);
      state.getWorkerRegistrationState().registerWorker(worker("worker", 10_000), "tenant-a", T0);
      final var old = new Scheduling();
      final var checker = new RegistrationDeadlineChecker(state, REGISTRATION_POLICY, () -> 1_300);
      checker.onRecovered(old);
      final var queued = old.tasks.getFirst().action;
      checker.onClose();
      final var current = new Scheduling();
      checker.onRecovered(current);
      queued.accept(
          (key, type, intent, value) -> {
            throw new AssertionError("Old generation must not produce commands");
          });
      final List<WrittenCommand> commands = new ArrayList<>();
      current.fire(
          (key, type, intent, value) -> commands.add(new WrittenCommand(type, intent, value)));
      assertEquals(1, commands.size());
      assertTrue(old.tasks.isEmpty());
      checker.onClose();
    }
  }

  private static WorkerRegistrationRecord worker(
      final String workerId, final long renewalTimeoutMillis) {
    return new WorkerRegistrationRecord()
        .setClusterId("demo")
        .setTenantIds(List.of("tenant-a"))
        .setWorkerId(workerId)
        .setName("payments")
        .setJobType("payment")
        .setMaxJobsActive(32)
        .setConcurrency(4)
        .setRenewalTimeoutMillis(renewalTimeoutMillis);
  }

  private record WrittenCommand(ValueType type, Intent intent, UnifiedRecordValue value) {}

  private static final class Scheduling implements ProcessingScheduleService {
    private final ArrayDeque<Task> tasks = new ArrayDeque<>();
    private Duration delay;

    @Override
    public ScheduledTimer schedule(final Duration delay, final Consumer<CommandWriter> action) {
      this.delay = delay;
      final Task task = new Task(action);
      tasks.add(task);
      return task;
    }

    private void fire(final CommandWriter writer) {
      tasks.removeFirst().action.accept(writer);
    }

    private final class Task implements ScheduledTimer {
      private final Consumer<CommandWriter> action;

      private Task(final Consumer<CommandWriter> action) {
        this.action = action;
      }

      @Override
      public void cancel() {
        tasks.remove(this);
      }
    }
  }
}
