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
package io.taskite.streamplatform.scheduler;

import static org.junit.jupiter.api.Assertions.*;

import io.taskite.scheduler.ScheduledTimer;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService.CommandWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

final class DueDateCheckerTest {
  @Test
  void startsAfterRecoveryAndRemainsIdleWithoutDeadlines() {
    final var calls = new AtomicInteger();
    final var scheduling = new Scheduling();
    final var checker =
        new DueDateChecker(
            Duration.ofMillis(100),
            commands -> {
              calls.incrementAndGet();
              return -1;
            });
    checker.schedule(-1);
    assertTrue(scheduling.tasks.isEmpty());
    checker.onRecovered(scheduling);
    assertEquals(Duration.ZERO, scheduling.tasks.getFirst().delay);
    scheduling.fire(0);
    assertEquals(1, calls.get());
    assertEquals(1, scheduling.tasks.size());
    checker.onClose();
  }

  @Test
  void retainsEarliestDeadlineAndIgnoresCancelledCallback() {
    final var now = new AtomicLong(1000);
    final var next = new AtomicLong(5000);
    final var calls = new AtomicInteger();
    final var scheduling = new Scheduling();
    final var checker =
        new DueDateChecker(
            Duration.ofMillis(100),
            commands -> {
              calls.incrementAndGet();
              return next.get();
            },
            now::get);
    checker.onRecovered(scheduling);
    scheduling.fire(0);
    checker.schedule(6000);
    checker.schedule(4950);
    assertEquals(2, scheduling.tasks.size());
    checker.schedule(2000);
    assertTrue(scheduling.tasks.get(1).cancelled);
    assertEquals(Duration.ofMillis(1000), scheduling.tasks.get(2).delay);
    scheduling.fire(1); // Simulate a callback already queued before cancellation.
    assertEquals(1, calls.get());
    next.set(-1);
    scheduling.fire(2);
    assertEquals(2, calls.get());
    assertEquals(3, scheduling.tasks.size());
  }

  @Test
  void yieldsAtResolutionForOverdueWorkAndAllowsImmediateWakeup() {
    final var scheduling = new Scheduling();
    final var checker = new DueDateChecker(Duration.ofMillis(100), commands -> 500, () -> 1000);
    checker.onRecovered(scheduling);
    scheduling.fire(0);
    assertEquals(Duration.ofMillis(100), scheduling.tasks.get(1).delay);
    checker.schedule(-1);
    assertTrue(scheduling.tasks.get(1).cancelled);
    assertEquals(Duration.ZERO, scheduling.tasks.get(2).delay);
    checker.schedule(-1);
    assertEquals(3, scheduling.tasks.size());
  }

  @Test
  void isolatesPreviousGenerationAndCanRecoverAgain() {
    final var calls = new AtomicInteger();
    final var old = new Scheduling();
    final var current = new Scheduling();
    final var checker =
        new DueDateChecker(
            Duration.ofMillis(100),
            commands -> {
              calls.incrementAndGet();
              return 2000;
            },
            () -> 1000);
    checker.onRecovered(old);
    checker.onClose();
    checker.onRecovered(current);
    old.fire(0);
    assertEquals(0, calls.get());
    current.fire(0);
    assertEquals(1, calls.get());
    assertEquals(2, current.tasks.size());
    checker.onClose();
    current.fire(1);
    assertEquals(1, calls.get());
    checker.schedule(-1);
    assertEquals(2, current.tasks.size());
  }

  @Test
  void preservesEarlierWakeupRequestedDuringVisitor() {
    final var scheduling = new Scheduling();
    final DueDateChecker[] checker = new DueDateChecker[1];
    checker[0] =
        new DueDateChecker(
            Duration.ofMillis(100),
            commands -> {
              checker[0].schedule(2000);
              return 5000;
            },
            () -> 1000);
    checker[0].onRecovered(scheduling);
    scheduling.fire(0);
    assertEquals(2, scheduling.tasks.size());
    assertEquals(Duration.ofMillis(1000), scheduling.tasks.get(1).delay);
  }

  @Test
  void closeDuringVisitorPreventsRescheduling() {
    final var scheduling = new Scheduling();
    final DueDateChecker[] checker = new DueDateChecker[1];
    checker[0] =
        new DueDateChecker(
            Duration.ofMillis(100),
            commands -> {
              checker[0].onClose();
              return 5000;
            },
            () -> 1000);
    checker[0].onRecovered(scheduling);
    scheduling.fire(0);
    assertEquals(1, scheduling.tasks.size());
  }

  @Test
  void acceptsConcurrentWakeupsWhileVisitorRunsWithoutHoldingCheckerLock() throws Exception {
    final var scheduling = new Scheduling();
    final DueDateChecker[] checker = new DueDateChecker[1];
    checker[0] =
        new DueDateChecker(
            Duration.ofMillis(100),
            commands -> {
              final var thread = Thread.ofPlatform().start(() -> checker[0].schedule(2000));
              try {
                thread.join(Duration.ofSeconds(2));
                assertFalse(thread.isAlive(), "visitor must not hold the scheduling lock");
              } catch (final InterruptedException error) {
                throw new RuntimeException(error);
              }
              return 5000;
            },
            () -> 1000);
    checker[0].onRecovered(scheduling);
    scheduling.fire(0);
    assertEquals(2, scheduling.tasks.size());
    assertEquals(Duration.ofMillis(1000), scheduling.tasks.get(1).delay);
  }

  private static final class Scheduling implements ProcessingScheduleService {
    private final List<Task> tasks = new ArrayList<>();

    @Override
    public ScheduledTimer schedule(final Duration delay, final Consumer<CommandWriter> action) {
      final var task = new Task(delay, action);
      tasks.add(task);
      return task;
    }

    void fire(final int index) {
      tasks.get(index).action.accept((key, type, intent, value) -> {});
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
