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
package io.taskite.engine.processing.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.engine.state.immutable.JobState.State;
import io.taskite.engine.state.job.DbJobState;
import io.taskite.protocol.impl.record.value.broker.JobBatchRecord;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.broker.JobBatchIntent;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService.CommandWriter;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.streamplatform.record.TypedRecordImpl;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class JobTimeoutCheckerTest {
  @TempDir Path directory;

  private PartitionStorage storage;
  private StateStoreProcessingStore store;
  private DbJobState jobs;

  @BeforeEach
  void setUp() {
    storage = new PartitionStorage(directory);
    store = new StateStoreProcessingStore(storage);
    jobs = new DbJobState(store);
    store.getCurrentTransaction();
  }

  @AfterEach
  void tearDown() throws Exception {
    store.getCurrentTransaction().rollback();
    storage.close();
  }

  @Test
  void recoveryScansImmediatelyAndStopsWhenOnlyBackoffsRemain() {
    jobs.update(2, job("tenant").setRetries(2).setRecurringTime(900), State.FAILED);
    final var scheduling = new Scheduling();
    final var checker = new JobTimeoutChecker(jobs, () -> 1000);
    checker.onRecovered(scheduling);
    assertEquals(Duration.ZERO, scheduling.tasks.getFirst().delay);
    scheduling.fire(0, new Writer(10));
    assertEquals(1, scheduling.tasks.size());
    checker.onClose();
  }

  @Test
  void futureDeadlineSleepsUntilDueAndNewWorkAdvancesWakeup() {
    jobs.activate(1, job("tenant").setDeadline(10_000));
    jobs.update(2, job("tenant").setRetries(2).setRecurringTime(900), State.FAILED);
    final var clock = new AtomicLong(1000);
    final var scheduling = new Scheduling();
    final var checker = new JobTimeoutChecker(jobs, clock::get);
    checker.onRecovered(scheduling);
    scheduling.fire(0, new Writer(10));
    assertEquals(Duration.ofSeconds(9), scheduling.tasks.get(1).delay);
    final var command = new TypedRecordImpl(1);
    final var metadata =
        new RecordMetadata()
            .recordType(RecordType.COMMAND)
            .valueType(ValueType.JOB_BATCH)
            .intent(JobBatchIntent.ACTIVATE);
    final var request = new JobBatchRecord();
    command.wrap(LoggedEvent.ofRecord(0, metadata, request), metadata, request);
    jobs.activate(42, job("tenant").setDeadline(1100).setWorkerId("worker").setWorkerLeaseToken(7));
    checker.onProcessed(command);
    assertTrue(scheduling.tasks.get(1).cancelled);
    assertEquals(Duration.ofMillis(100), scheduling.tasks.get(2).delay);
    clock.set(1100);
    final var writer = new Writer(1);
    scheduling.fire(2, writer);
    assertEquals(42, writer.commands.getFirst().key);
    assertEquals(JobIntent.TIME_OUT, writer.commands.getFirst().intent);
    assertEquals(Duration.ofMillis(100), scheduling.tasks.get(3).delay);
    checker.onClose();
  }

  @Test
  void singleCommandBudgetOnlyEmitsTimeoutAndPreservesFences() {
    jobs.activate(
        1, job("tenant-timeout").setDeadline(900).setWorkerId("worker").setWorkerLeaseToken(17));
    jobs.update(2, job("tenant-backoff").setRetries(2).setRecurringTime(800), State.FAILED);
    jobs.activate(3, job("tenant-timeout").setDeadline(950));
    final var scheduling = new Scheduling();
    final var checker = new JobTimeoutChecker(jobs, () -> 1000);
    checker.onRecovered(scheduling);
    final var timeout = new Writer(1);
    scheduling.fire(0, timeout);
    assertEquals(1, timeout.commands.size());
    final var timedOut = timeout.commands.getFirst();
    assertEquals(JobIntent.TIME_OUT, timedOut.intent);
    assertEquals("tenant-timeout", timedOut.value.getTenantId());
    assertEquals(900, timedOut.value.getDeadline());
    assertEquals("worker", timedOut.value.getWorkerId());
    assertEquals(17, timedOut.value.getWorkerLeaseToken());
    jobs.complete(timedOut.key, jobs.getJob(timedOut.key));
    final var next = new Writer(1);
    scheduling.fire(1, next);
    assertEquals(1, next.commands.size());
    assertEquals(3, next.commands.getFirst().key);
    assertEquals(JobIntent.TIME_OUT, next.commands.getFirst().intent);
    assertEquals(1, jobs.findBackedOffJobs(1000, 10).size());
    checker.onClose();
  }

  @Test
  void exhaustedBudgetPreservesDueJobAndKeepsDeadlineScheduled() {
    jobs.activate(1, job("tenant").setDeadline(900));
    final var scheduling = new Scheduling();
    final var checker = new JobTimeoutChecker(jobs, () -> 1000);
    checker.onRecovered(scheduling);
    final var writer = new Writer(0);
    scheduling.fire(0, writer);
    assertTrue(writer.commands.isEmpty());
    assertEquals(
        List.of(1L), jobs.findTimedOutJobs(1000, 10).stream().map(job -> job.key()).toList());
    assertEquals(Duration.ofMillis(100), scheduling.tasks.get(1).delay);
    final var next = new Writer(1);
    scheduling.fire(1, next);
    assertEquals(1, next.commands.size());
    assertEquals(JobIntent.TIME_OUT, next.commands.getFirst().intent);
    checker.onClose();
  }

  @Test
  void closeAndRecoveryIgnoreCallbacksFromPreviousGeneration() {
    jobs.activate(1, job("tenant").setDeadline(900));
    final var old = new Scheduling();
    final var checker = new JobTimeoutChecker(jobs, () -> 1000);
    checker.onRecovered(old);
    checker.onClose();
    final var current = new Scheduling();
    checker.onRecovered(current);
    final var stale = new Writer(10);
    old.fire(0, stale);
    assertTrue(stale.commands.isEmpty());
    assertEquals(1, old.tasks.size());
    final var writer = new Writer(10);
    current.fire(0, writer);
    assertEquals(1, writer.commands.size());
    checker.onClose();
    current.fire(1, stale);
    assertTrue(stale.commands.isEmpty());
    assertEquals(2, current.tasks.size());
  }

  @Test
  void boundsTimeoutScanAndStopsAfterTimeoutsAreApplied() {
    for (long key = 1; key <= 101; key++) {
      jobs.activate(key, job("tenant").setDeadline(900));
      jobs.update(1000 + key, job("tenant").setRetries(2).setRecurringTime(800), State.FAILED);
    }
    final var scheduling = new Scheduling();
    final var checker = new JobTimeoutChecker(jobs, () -> 1000);
    checker.onRecovered(scheduling);
    final var first = new Writer(1000);
    scheduling.fire(0, first);
    assertEquals(100, first.commands.size());
    assertTrue(first.commands.stream().allMatch(command -> command.intent == JobIntent.TIME_OUT));
    for (final var command : first.commands) {
      jobs.complete(command.key, jobs.getJob(command.key));
    }
    final var second = new Writer(1000);
    scheduling.fire(1, second);
    assertEquals(1, second.commands.size());
    for (final var command : second.commands) {
      jobs.complete(command.key, jobs.getJob(command.key));
    }
    final var empty = new Writer(1000);
    scheduling.fire(2, empty);
    assertTrue(empty.commands.isEmpty());
    assertEquals(3, scheduling.tasks.size());
    assertEquals(101, jobs.findBackedOffJobs(1000, 200).size());
    checker.onClose();
  }

  private static JobRecord job(final String tenantId) {
    return new JobRecord().setJobType("work").setTenantId(tenantId).setVariablesJson("{}");
  }

  private record Command(long key, Intent intent, JobRecord value) {}

  private static final class Writer implements CommandWriter {
    private final int capacity;
    private final List<Command> commands = new ArrayList<>();

    private Writer(final int capacity) {
      this.capacity = capacity;
    }

    @Override
    public int remainingCapacity() {
      return capacity - commands.size();
    }

    @Override
    public void append(
        final long key, final ValueType type, final Intent intent, final UnifiedRecordValue value) {
      assertTrue(hasRemainingCapacity());
      assertEquals(ValueType.JOB, type);
      commands.add(new Command(key, intent, (JobRecord) value));
    }
  }

  private static final class Scheduling implements ProcessingScheduleService {
    private final List<Task> tasks = new ArrayList<>();

    @Override
    public ScheduledTimer schedule(final Duration delay, final Consumer<CommandWriter> action) {
      final var task = new Task(delay, action);
      tasks.add(task);
      return task;
    }

    private void fire(final int index, final CommandWriter writer) {
      tasks.get(index).action.accept(writer);
    }
  }

  private static final class Task implements ScheduledTimer {
    private final Duration delay;
    private final Consumer<CommandWriter> action;
    private boolean cancelled;

    private Task(final Duration delay, final Consumer<CommandWriter> action) {
      this.delay = delay;
      this.action = action;
    }

    @Override
    public void cancel() {
      cancelled = true;
    }
  }
}
