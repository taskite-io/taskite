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
package io.taskite.gateway;

import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/** Deterministic mailbox and cancellable duration timers for Gateway tests. */
public final class ManualConcurrencyControl implements ConcurrencyControl {
  private final ArrayDeque<Runnable> mailbox = new ArrayDeque<>();
  private final PriorityQueue<Timer> timers =
      new PriorityQueue<>(Comparator.comparingLong(timer -> timer.due));
  private boolean running;
  private long now;

  @Override
  public void run(Runnable action) {
    mailbox.add(action);
    if (running) {
      return;
    }
    running = true;
    try {
      while (!mailbox.isEmpty()) {
        mailbox.removeFirst().run();
      }
    } finally {
      running = false;
    }
  }

  @Override
  public <T> void runOnCompletion(ActorFuture<T> future, BiConsumer<T, Throwable> callback) {
    future.onComplete((value, error) -> run(() -> callback.accept(value, error)));
  }

  @Override
  public <T> ActorFuture<T> call(Callable<T> callable) {
    final var future = new CompletableActorFuture<T>();
    run(
        () -> {
          try {
            future.complete(callable.call());
          } catch (Exception error) {
            future.completeExceptionally(error);
          }
        });
    return future;
  }

  @Override
  public ScheduledTimer schedule(Duration delay, Runnable action) {
    final var timer = new Timer(Math.addExact(now, delay.toMillis()), action);
    timers.add(timer);
    return timer;
  }

  public void advance(long millis) {
    final long until = Math.addExact(now, millis);
    while (!timers.isEmpty() && timers.peek().due <= until) {
      final var timer = timers.remove();
      now = timer.due;
      if (!timer.cancelled) {
        run(timer.action);
      }
    }
    now = until;
  }

  public long pendingTimers() {
    return timers.stream().filter(timer -> !timer.cancelled).count();
  }

  private static final class Timer implements ScheduledTimer {
    private final long due;
    private final Runnable action;
    private boolean cancelled;

    private Timer(long due, Runnable action) {
      this.due = due;
      this.action = action;
    }

    @Override
    public void cancel() {
      cancelled = true;
    }
  }
}
