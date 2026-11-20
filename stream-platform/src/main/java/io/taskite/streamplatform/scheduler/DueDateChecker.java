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

import io.taskite.scheduler.ScheduledTimer;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService.CommandWriter;
import io.taskite.streamplatform.api.processor.StreamProcessorLifecycleAware;
import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

/** Schedules a committed-state visitor which returns its next absolute deadline, or -1 if idle. */
public final class DueDateChecker implements StreamProcessorLifecycleAware {
  private final long resolutionMillis;
  private final ToLongFunction<CommandWriter> visitor;
  private final LongSupplier clock;
  private ProcessingScheduleService scheduling;
  private Pending pending;
  private long generation;

  public DueDateChecker(final Duration resolution, final ToLongFunction<CommandWriter> visitor) {
    this(resolution, visitor, System::currentTimeMillis);
  }

  public DueDateChecker(
      final Duration resolution,
      final ToLongFunction<CommandWriter> visitor,
      final LongSupplier clock) {
    resolutionMillis = resolution.toMillis();
    if (resolutionMillis <= 0) {
      throw new IllegalArgumentException("Due date resolution must be at least one millisecond");
    }
    this.visitor = Objects.requireNonNull(visitor);
    this.clock = Objects.requireNonNull(clock);
  }

  @Override
  public synchronized void onRecovered(final ProcessingScheduleService scheduling) {
    onClose();
    this.scheduling = Objects.requireNonNull(scheduling);
    schedule(-1);
  }

  @Override
  public synchronized void onClose() {
    generation++;
    scheduling = null;
    if (pending != null) {
      pending.timer.cancel();
      pending = null;
    }
  }

  /** Requests a check at an absolute deadline; -1 requests an immediate check. */
  public synchronized void schedule(final long dueDate) {
    if (scheduling == null) {
      return;
    }
    final long now = clock.getAsLong();
    final long earliest =
        now > Long.MAX_VALUE - resolutionMillis ? Long.MAX_VALUE : now + resolutionMillis;
    final long next = dueDate == -1 ? now : Math.max(earliest, dueDate);
    if (pending != null
        && (next >= pending.dueDate
            || (dueDate != -1 && pending.dueDate - next < resolutionMillis))) {
      return;
    }
    if (pending != null) {
      pending.timer.cancel();
    }
    final Pending task = new Pending(next, generation);
    pending = task;
    task.timer = scheduling.schedule(Duration.ofMillis(next - now), writer -> check(task, writer));
  }

  private void check(final Pending task, final CommandWriter writer) {
    synchronized (this) {
      if (scheduling == null || pending != task || task.generation != generation) {
        return;
      }
      pending = null;
    }
    final long next = visitor.applyAsLong(writer);
    synchronized (this) {
      if (scheduling != null && task.generation == generation && next != -1) {
        schedule(next);
      }
    }
  }

  private static final class Pending {
    private final long dueDate;
    private final long generation;
    private ScheduledTimer timer;

    private Pending(final long dueDate, final long generation) {
      this.dueDate = dueDate;
      this.generation = generation;
    }
  }
}
