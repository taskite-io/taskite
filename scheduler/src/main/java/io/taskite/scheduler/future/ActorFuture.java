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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

public interface ActorFuture<V> extends Future<V>, BiConsumer<V, Throwable> {
  void complete(V value);

  void completeExceptionally(String failure, Throwable throwable);

  void completeExceptionally(Throwable throwable);

  V join();

  V join(long timeout, TimeUnit timeUnit);

  void onComplete(BiConsumer<V, Throwable> consumer);

  void onComplete(BiConsumer<V, Throwable> consumer, Executor executor);

  boolean isCompletedExceptionally();

  Throwable getException();

  @Override
  default void accept(final V value, final Throwable throwable) {
    if (throwable == null) {
      complete(value);
    } else {
      completeExceptionally(throwable);
    }
  }

  default void whenComplete(final BiConsumer<V, Throwable> consumer) {
    onComplete(consumer, Runnable::run);
  }

  default CompletableFuture<V> toCompletableFuture() {
    final var future = new CompletableFuture<V>();
    onComplete(
        (value, error) -> {
          if (error == null) {
            future.complete(value);
          } else {
            future.completeExceptionally(error);
          }
        },
        Runnable::run);
    return future;
  }

  default ActorFuture<V> andThen(final Supplier<ActorFuture<V>> next, final Executor executor) {
    return andThen(ignored -> next.get(), executor);
  }

  ActorFuture<V> andThen(Function<V, ActorFuture<V>> next, Executor executor);

  default <U> ActorFuture<U> thenApply(final Function<? super V, ? extends U> mapper) {
    final var result = new CompletableActorFuture<U>();
    onComplete(
        (value, error) -> {
          if (error != null) {
            result.completeExceptionally(error);
            return;
          }
          try {
            result.complete(mapper.apply(value));
          } catch (final Throwable mapperError) {
            result.completeExceptionally(mapperError);
          }
        },
        Runnable::run);
    return result;
  }

  default <U> ActorFuture<U> thenCompose(
      final Function<? super V, ? extends ActorFuture<U>> mapper) {
    final var result = new CompletableActorFuture<U>();
    onComplete(
        (value, error) -> {
          if (error != null) {
            result.completeExceptionally(error);
            return;
          }
          try {
            mapper
                .apply(value)
                .onComplete(
                    (nextValue, nextError) -> {
                      if (nextError != null) {
                        result.completeExceptionally(nextError);
                      } else {
                        result.complete(nextValue);
                      }
                    },
                    Runnable::run);
          } catch (final Throwable mapperError) {
            result.completeExceptionally(mapperError);
          }
        },
        Runnable::run);
    return result;
  }
}
