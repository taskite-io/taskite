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
package io.taskite.streamplatform.runtime.transition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.snapshot.SnapshotDirector;
import io.taskite.snapshot.SnapshotMetrics;
import io.taskite.snapshot.SnapshotPolicy;
import io.taskite.snapshot.SnapshotProgress;
import io.taskite.snapshot.SnapshotProgressSource;
import io.taskite.snapshot.SnapshotProgressSubscription;
import io.taskite.snapshot.SnapshotResult;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

final class SnapshotDirectorTransitionStepTest {

  @Test
  void installsDirectorOnlyForLeaderAndRemovesItBeforeLeavingLeader() {
    final AtomicInteger created = new AtomicInteger();
    final TestProgressSource progress = new TestProgressSource();
    final SnapshotDirectorTransitionStep step =
        new SnapshotDirectorTransitionStep(
            () -> {
              created.incrementAndGet();
              return director(progress);
            });
    final PartitionTransitionContext context = new PartitionTransitionContext(1, null);

    step.transitionTo(context, 1, PartitionRole.FOLLOWER).join();
    assertNull(context.getSnapshotDirector());
    assertEquals(0, created.get());

    step.transitionTo(context, 2, PartitionRole.LEADER).join();
    assertNotNull(context.getSnapshotDirector());
    assertEquals(1, created.get());
    assertEquals(1, progress.listeners.get());

    step.onNewRaftRole(context, PartitionRole.FOLLOWER);
    assertTrue(context.getSnapshotDirector().forceSnapshot().isCompletedExceptionally());
    step.prepareTransition(context, 2, PartitionRole.FOLLOWER).join();

    assertNull(context.getSnapshotDirector());
    assertEquals(0, progress.listeners.get());
  }

  private static SnapshotDirector director(final TestProgressSource progress) {
    return new SnapshotDirector(
        1,
        SnapshotPolicy.of(Duration.ZERO, 1024),
        progress,
        () -> CompletableActorFuture.completed(SnapshotResult.persisted(1, 1, 64)),
        SnapshotMetrics.NOOP,
        new DirectControl());
  }

  private static final class TestProgressSource implements SnapshotProgressSource {
    private final AtomicInteger listeners = new AtomicInteger();

    @Override
    public SnapshotProgress current() {
      return SnapshotProgress.EMPTY;
    }

    @Override
    public SnapshotProgressSubscription subscribe(final Consumer<SnapshotProgress> listener) {
      listeners.incrementAndGet();
      final AtomicBoolean closed = new AtomicBoolean();
      return () -> {
        if (closed.compareAndSet(false, true)) {
          listeners.decrementAndGet();
        }
      };
    }
  }

  private static final class DirectControl implements ConcurrencyControl {
    @Override
    public <T> void runOnCompletion(
        final ActorFuture<T> future, final BiConsumer<T, Throwable> callback) {
      future.whenComplete(callback);
    }

    @Override
    public void run(final Runnable action) {
      action.run();
    }

    @Override
    public <T> ActorFuture<T> call(final Callable<T> callable) {
      try {
        return CompletableActorFuture.completed(callable.call());
      } catch (final Exception error) {
        return CompletableActorFuture.completedExceptionally(error);
      }
    }

    @Override
    public ScheduledTimer schedule(final Duration delay, final Runnable runnable) {
      return () -> {};
    }
  }
}
