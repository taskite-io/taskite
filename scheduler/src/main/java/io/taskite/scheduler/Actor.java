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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Actor implements AutoCloseable, ActorClosable, ConcurrencyControl {
  public static final String ACTOR_PROP_NAME = "actor-name";

  private static final Logger LOG = LoggerFactory.getLogger(Actor.class);
  private static final int MAX_CLOSE_TIMEOUT = 300;

  private volatile ConcurrencyControl concurrencyControl;
  private volatile Runnable closeHandler;
  private volatile Map<String, String> context;
  private final CompletableActorFuture<Void> closeFuture = new CompletableActorFuture<>();

  protected Map<String, String> createContext() {
    final var actorContext = new HashMap<String, String>();
    actorContext.put(ACTOR_PROP_NAME, getName());
    return actorContext;
  }

  public String getName() {
    return getClass().getSimpleName();
  }

  public Map<String, String> getContext() {
    if (context == null) {
      context = Collections.unmodifiableMap(createContext());
    }
    return context;
  }

  public boolean isActorClosed() {
    return closeFuture.isDone();
  }

  protected void onActorStarting() {}

  protected void onActorStarted() {}

  protected void onActorClosing() {}

  protected void onActorClosed() {}

  protected void onActorCloseRequested() {}

  protected void handleFailure(final Throwable failure) {
    LOG.error(
        "Uncaught exception in actor '{}': {}",
        getName(),
        failure == null ? "unknown error" : failure.getMessage(),
        failure);
  }

  public void onActorFailed() {}

  @Override
  public void close() {
    closeAsync().join(MAX_CLOSE_TIMEOUT, TimeUnit.SECONDS);
  }

  @Override
  public ActorFuture<Void> closeAsync() {
    if (closeFuture.isDone()) {
      return closeFuture;
    }
    final var handler = closeHandler;
    if (handler == null) {
      return CompletableActorFuture.completed(null);
    }

    handler.run();
    return closeFuture;
  }

  @Override
  public <T> void runOnCompletion(
      final ActorFuture<T> future, final BiConsumer<T, Throwable> callback) {
    requireControl().runOnCompletion(future, callback);
  }

  @Override
  public void run(final Runnable action) {
    requireControl().run(action);
  }

  @Override
  public <T> ActorFuture<T> call(final Callable<T> callable) {
    return requireControl().call(callable);
  }

  @Override
  public ScheduledTimer schedule(final Duration delay, final Runnable runnable) {
    return requireControl().schedule(delay, runnable);
  }

  static String buildActorName(final String name, final int partitionId) {
    return "%s-%d".formatted(name, partitionId);
  }

  static Actor wrap(final Consumer<ConcurrencyControl> consumer) {
    return new Actor() {
      @Override
      public String getName() {
        return consumer.toString();
      }

      @Override
      protected void onActorStarted() {
        consumer.accept(this);
      }
    };
  }

  public static ActorBuilder newActor() {
    return new ActorBuilder();
  }

  void bind(final ConcurrencyControl control, final Runnable closeAction) {
    concurrencyControl = Objects.requireNonNull(control);
    closeHandler = Objects.requireNonNull(closeAction);
  }

  void completeClose() {
    if (!closeFuture.isDone()) {
      closeFuture.complete(null);
    }
  }

  void failClose(final Throwable error) {
    if (!closeFuture.isDone()) {
      closeFuture.completeExceptionally(error);
    }
  }

  private ConcurrencyControl requireControl() {
    final var current = concurrencyControl;
    if (current == null) {
      throw new IllegalStateException("Actor is not bound to a scheduler");
    }
    return current;
  }

  public static final class ActorBuilder {
    private String name;
    private Consumer<ConcurrencyControl> actorStartedHandler;

    public ActorBuilder name(final String value) {
      name = value;
      return this;
    }

    public ActorBuilder actorStartedHandler(final Consumer<ConcurrencyControl> handler) {
      actorStartedHandler = handler;
      return this;
    }

    public Actor build() {
      final Consumer<ConcurrencyControl> wrapper =
          new Consumer<>() {
            @Override
            public String toString() {
              if (name != null) {
                return name;
              }
              return actorStartedHandler == null
                  ? super.toString()
                  : actorStartedHandler.getClass().getName();
            }

            @Override
            public void accept(final ConcurrencyControl control) {
              if (actorStartedHandler != null) {
                actorStartedHandler.accept(control);
              }
            }
          };

      return Actor.wrap(wrapper);
    }
  }
}
