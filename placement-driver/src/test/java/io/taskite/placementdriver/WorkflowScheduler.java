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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.writer.PostCommitActionWriter;
import io.taskite.streamplatform.api.writer.TypedCommandWriter;
import io.taskite.streamplatform.api.writer.Writers;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

/** Executes collected commands only after a read task, and RPCs only after command processing. */
public final class WorkflowScheduler implements ProcessingScheduleService {
  public final AtomicLong clock;
  public final List<Task> tasks = new ArrayList<>();
  public final List<Task> history = new ArrayList<>();
  private final ArrayDeque<LoggedEvent> commands = new ArrayDeque<>();
  private final List<Runnable> postCommit = new ArrayList<>();
  public final Writers writers = mock(Writers.class);
  public Consumer<LoggedEvent> processor = ignored -> {};
  public Predicate<LoggedEvent> admission = ignored -> true;
  public int capacity = Integer.MAX_VALUE;

  public WorkflowScheduler(final AtomicLong clock) {
    this.clock = clock;
    final var actions = mock(PostCommitActionWriter.class);
    when(writers.postCommit()).thenReturn(actions);
    final var followUps = mock(TypedCommandWriter.class);
    when(writers.command()).thenReturn(followUps);
    doAnswer(
            invocation -> {
              new RecordingCommandWriter(commands::add)
                  .append(
                      invocation.getArgument(0),
                      invocation.getArgument(1),
                      invocation.getArgument(2),
                      invocation.getArgument(3));
              return null;
            })
        .when(followUps)
        .appendFollowUpCommand(
            anyLong(), any(ValueType.class), any(Intent.class), any(UnifiedRecordValue.class));
    doAnswer(
            invocation -> {
              postCommit.add(invocation.getArgument(0));
              return null;
            })
        .when(actions)
        .append(any(Runnable.class));
  }

  @Override
  public ScheduledTimer schedule(final Duration delay, final Consumer<CommandWriter> action) {
    final var task = new Task(clock.get() + delay.toMillis(), action);
    tasks.add(task);
    history.add(task);
    return task;
  }

  public void runReady() {
    for (int count = 0; count < 1000; count++) {
      final var next = tasks.stream().min(Comparator.comparingLong(task -> task.due)).orElse(null);
      if (next == null || next.due > clock.get()) {
        return;
      }
      tasks.remove(next);
      next.action.accept(
          new CommandWriter() {
            private int appended;

            @Override
            public int remainingCapacity() {
              return capacity - appended;
            }

            @Override
            public boolean tryAppend(
                final long key,
                final ValueType type,
                final Intent intent,
                final UnifiedRecordValue value) {
              if (!hasRemainingCapacity()) {
                return false;
              }
              final boolean[] accepted = {false};
              new RecordingCommandWriter(
                      event -> {
                        if (admission.test(event)) {
                          commands.add(event);
                          appended++;
                          accepted[0] = true;
                        }
                      })
                  .append(key, type, intent, value);
              return accepted[0];
            }

            @Override
            public void append(
                final long key,
                final ValueType type,
                final Intent intent,
                final UnifiedRecordValue value) {
              if (!tryAppend(key, type, intent, value)) {
                throw new AssertionError("Command exceeded the scheduled writer budget");
              }
            }
          });
      while (!commands.isEmpty()) {
        processor.accept(commands.removeFirst());
        final var actions = List.copyOf(postCommit);
        postCommit.clear();
        actions.forEach(Runnable::run);
      }
    }
    throw new AssertionError("Workflow did not yield its processing budget");
  }

  public final class Task implements ScheduledTimer {
    public final long due;
    public final Consumer<CommandWriter> action;
    public boolean canceled;

    private Task(final long due, final Consumer<CommandWriter> action) {
      this.due = due;
      this.action = action;
    }

    @Override
    public void cancel() {
      canceled = true;
      tasks.remove(this);
    }
  }
}
