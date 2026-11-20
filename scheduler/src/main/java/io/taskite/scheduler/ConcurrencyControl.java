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
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

/**
 * Control interface to schedule tasks or follow-up tasks such that different tasks scheduled via
 * the same {@code ConcurrencyControl} object are never executed concurrently
 */
public interface ConcurrencyControl extends Executor {

  /**
   * Schedules a callback to be invoked after the future has completed
   *
   * @param future the future whose completion is awaited
   * @param callback the callback to call after the future has completed
   * @param <T> result type of the future
   */
  <T> void runOnCompletion(final ActorFuture<T> future, final BiConsumer<T, Throwable> callback);

  /**
   * Schedules an action to be invoked (must be called from an actor thread)
   *
   * @param action action to be invoked
   */
  void run(final Runnable action);

  /**
   * Schedules a callable to be executed
   *
   * @param callable callable to be executed
   * @return a future with the result
   * @param <T> type of the result
   */
  <T> ActorFuture<T> call(final Callable<T> callable);

  /** Schedule a task to be executed after a delay */
  ScheduledTimer schedule(final Duration delay, final Runnable runnable);

  /**
   * Create a new future object
   *
   * @param <V> value type of future
   * @return new future object
   */
  default <V> ActorFuture<V> createFuture() {
    return new CompletableActorFuture<>();
  }

  /**
   * Create a new completed future object
   *
   * @param <V> value type of future
   * @return new completed future object
   */
  default <V> ActorFuture<V> createCompletedFuture() {
    return CompletableActorFuture.completed(null);
  }

  @Override
  default void execute(final Runnable command) {
    run(command);
  }
}
