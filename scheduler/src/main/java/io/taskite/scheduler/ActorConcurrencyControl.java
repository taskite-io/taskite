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
package io.taskite.scheduler;

import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import org.apache.pekko.actor.typed.ActorRef;

/** Adapter that implements ConcurrencyControl interface using actor-based concurrency. */
public final class ActorConcurrencyControl implements ConcurrencyControl {

  private final ActorRef<Object> actorRef;
  private final ActorTimerQueue timers;
  private boolean closed;

  ActorConcurrencyControl(final ActorRef<Object> actorRef, final ActorTimerQueue timers) {
    this.actorRef = actorRef;
    this.timers = timers;
  }

  @Override
  public void run(final Runnable action) {
    actorRef.tell(action);
  }

  @Override
  public <T> ActorFuture<T> call(final Callable<T> callable) {
    final CompletableActorFuture<T> future = new CompletableActorFuture<>();
    actorRef.tell(new CallableTask<>(callable, future));
    return future;
  }

  @Override
  public <T> void runOnCompletion(
      final ActorFuture<T> future, final BiConsumer<T, Throwable> callback) {
    future.onComplete(
        (value, error) -> {
          final Runnable callbackTask = () -> callback.accept(value, error);
          actorRef.tell(callbackTask);
        });
  }

  @Override
  public synchronized ScheduledTimer schedule(final Duration delay, final Runnable runnable) {
    return closed ? () -> {} : timers.schedule(this, delay, actorRef::tell, runnable);
  }

  synchronized void close() {
    closed = true;
    timers.cancelTimers(this);
  }

  private static final class CallableTask<T> implements Runnable {
    private final Callable<T> callable;
    private final CompletableActorFuture<T> future;

    private CallableTask(final Callable<T> callable, final CompletableActorFuture<T> future) {
      this.callable = callable;
      this.future = future;
    }

    @Override
    public void run() {
      try {
        future.complete(callable.call());
      } catch (final Throwable error) {
        future.completeExceptionally(error);
      }
    }
  }
}
