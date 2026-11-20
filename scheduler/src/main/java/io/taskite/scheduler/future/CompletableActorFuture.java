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
package io.taskite.scheduler.future;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;

public final class CompletableActorFuture<V> implements ActorFuture<V> {
  private final CompletableFuture<V> delegate;
  private volatile Throwable exception;

  public CompletableActorFuture() {
    this(new CompletableFuture<>());
  }

  private CompletableActorFuture(final CompletableFuture<V> delegate) {
    this.delegate = delegate;
  }

  public static <V> CompletableActorFuture<V> completed(final V value) {
    final var future = new CompletableActorFuture<V>();
    future.complete(value);
    return future;
  }

  public static <V> CompletableActorFuture<V> completedExceptionally(final Throwable throwable) {
    final var future = new CompletableActorFuture<V>();
    future.completeExceptionally(throwable);
    return future;
  }

  @Override
  public boolean cancel(final boolean mayInterruptIfRunning) {
    return delegate.cancel(mayInterruptIfRunning);
  }

  @Override
  public boolean isCancelled() {
    return delegate.isCancelled();
  }

  @Override
  public boolean isDone() {
    return delegate.isDone();
  }

  @Override
  public V get() throws InterruptedException, ExecutionException {
    return delegate.get();
  }

  @Override
  public V get(final long timeout, final TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return delegate.get(timeout, unit);
  }

  @Override
  public void complete(final V value) {
    if (!delegate.complete(value)) {
      throw new IllegalStateException("Future already completed");
    }
  }

  @Override
  public void completeExceptionally(final String failure, final Throwable throwable) {
    exception = Objects.requireNonNullElseGet(throwable, () -> new RuntimeException(failure));
    if (!delegate.completeExceptionally(exception)) {
      throw new IllegalStateException("Future already completed", exception);
    }
  }

  @Override
  public void completeExceptionally(final Throwable throwable) {
    final var error = Objects.requireNonNull(throwable, "throwable");
    completeExceptionally(error.getMessage(), error);
  }

  @Override
  public V join() {
    return delegate.join();
  }

  @Override
  public V join(final long timeout, final TimeUnit timeUnit) {
    try {
      return get(timeout, timeUnit);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (final ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onComplete(final BiConsumer<V, Throwable> consumer) {
    onComplete(consumer, Runnable::run);
  }

  @Override
  public void onComplete(final BiConsumer<V, Throwable> consumer, final Executor executor) {
    delegate.whenCompleteAsync(consumer, executor);
  }

  @Override
  public boolean isCompletedExceptionally() {
    return delegate.isCompletedExceptionally();
  }

  @Override
  public Throwable getException() {
    return exception;
  }

  @Override
  public ActorFuture<V> andThen(final Function<V, ActorFuture<V>> next, final Executor executor) {
    final var chained = new CompletableActorFuture<V>();
    onComplete(
        (value, error) -> {
          if (error != null) {
            chained.completeExceptionally(error);
            return;
          }

          next.apply(value)
              .onComplete(
                  (nextValue, nextError) -> {
                    if (nextError != null) {
                      chained.completeExceptionally(nextError);
                    } else {
                      chained.complete(nextValue);
                    }
                  },
                  executor);
        },
        executor);
    return chained;
  }
}
