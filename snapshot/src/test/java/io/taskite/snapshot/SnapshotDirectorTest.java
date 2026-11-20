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
package io.taskite.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

final class SnapshotDirectorTest {

  @Test
  void triggersOnlyAfterUncoveredBytesReachThreshold() {
    final DirectControl control = new DirectControl();
    final TestProgressSource progress = new TestProgressSource();
    final AtomicInteger snapshots = new AtomicInteger();
    final SnapshotDirector director =
        director(
            control,
            progress,
            () -> {
              snapshots.incrementAndGet();
              progress.update(new SnapshotProgress(2, 2, 0, 0));
              return CompletableActorFuture.completed(SnapshotResult.persisted(2, 1, 128));
            });

    director.start();
    progress.update(new SnapshotProgress(1, 0, 99, 99));
    assertEquals(0, snapshots.get());

    progress.update(new SnapshotProgress(2, 0, 100, 100));
    assertEquals(1, snapshots.get());
  }

  @Test
  void coalescesTriggersWhileSnapshotIsInFlight() {
    final DirectControl control = new DirectControl();
    final TestProgressSource progress = new TestProgressSource();
    final AtomicInteger snapshots = new AtomicInteger();
    final CompletableActorFuture<SnapshotResult> inFlight = new CompletableActorFuture<>();
    final SnapshotDirector director =
        director(
            control,
            progress,
            () -> {
              snapshots.incrementAndGet();
              return inFlight;
            });

    director.start();
    progress.update(new SnapshotProgress(1, 0, 100, 100));
    progress.update(new SnapshotProgress(2, 0, 200, 200));

    assertEquals(1, snapshots.get());

    progress.update(new SnapshotProgress(2, 2, 0, 0));
    inFlight.complete(SnapshotResult.persisted(2, 1, 128));
    assertEquals(1, snapshots.get());
  }

  @Test
  void retriesFailuresWithBoundedBackoff() {
    final DirectControl control = new DirectControl();
    final TestProgressSource progress = new TestProgressSource();
    final AtomicInteger snapshots = new AtomicInteger();
    final SnapshotDirector director =
        director(
            control,
            progress,
            () -> {
              snapshots.incrementAndGet();
              return CompletableActorFuture.completedExceptionally(
                  new IllegalStateException("snapshot unavailable"));
            });

    director.start();
    progress.update(new SnapshotProgress(1, 0, 100, 100));
    for (int i = 0; i < 7; i++) {
      control.runNextScheduled();
    }

    assertEquals(8, snapshots.get());
    assertEquals(
        List.of(
            Duration.ofSeconds(1),
            Duration.ofSeconds(2),
            Duration.ofSeconds(4),
            Duration.ofSeconds(8),
            Duration.ofSeconds(16),
            Duration.ofSeconds(32),
            Duration.ofSeconds(60),
            Duration.ofSeconds(60)),
        control.retryDelays());
  }

  @Test
  void retriesSkippedSizeTriggerWithoutSpinning() {
    final DirectControl control = new DirectControl();
    final TestProgressSource progress = new TestProgressSource();
    final AtomicInteger snapshots = new AtomicInteger();
    final SnapshotDirector director =
        director(
            control,
            progress,
            () -> {
              if (snapshots.incrementAndGet() == 1) {
                return CompletableActorFuture.completed(SnapshotResult.skipped(0, 0));
              }
              progress.update(new SnapshotProgress(1, 1, 0, 0));
              return CompletableActorFuture.completed(SnapshotResult.persisted(1, 1, 64));
            });

    director.start();
    progress.update(new SnapshotProgress(1, 0, 100, 100));

    assertEquals(1, snapshots.get());
    assertEquals(List.of(Duration.ofSeconds(1)), control.retryDelays());

    control.runNextScheduled();

    assertEquals(2, snapshots.get());
  }

  @Test
  void closeRemovesProgressSubscriptionAndTimers() {
    final DirectControl control = new DirectControl();
    final TestProgressSource progress = new TestProgressSource();
    final AtomicInteger snapshots = new AtomicInteger();
    final SnapshotDirector director =
        director(
            control,
            progress,
            () -> {
              snapshots.incrementAndGet();
              return CompletableActorFuture.completed(SnapshotResult.persisted(1, 1, 64));
            });

    director.start();
    director.close();
    progress.update(new SnapshotProgress(1, 0, 100, 100));

    assertEquals(0, snapshots.get());
    assertEquals(0, progress.listenerCount());
    assertTrue(control.timers.stream().allMatch(timer -> timer.cancelled));
  }

  private static SnapshotDirector director(
      final DirectControl control,
      final TestProgressSource progress,
      final SnapshotOperation operation) {
    return new SnapshotDirector(
        1,
        new SnapshotPolicy(Duration.ZERO, 100, Duration.ofSeconds(1), Duration.ofMinutes(1)),
        progress,
        operation,
        SnapshotMetrics.NOOP,
        control);
  }

  private static final class TestProgressSource implements SnapshotProgressSource {
    private final List<Consumer<SnapshotProgress>> listeners = new CopyOnWriteArrayList<>();
    private volatile SnapshotProgress progress = SnapshotProgress.EMPTY;

    @Override
    public SnapshotProgress current() {
      return progress;
    }

    @Override
    public SnapshotProgressSubscription subscribe(final Consumer<SnapshotProgress> listener) {
      listeners.add(listener);
      return () -> listeners.remove(listener);
    }

    private void update(final SnapshotProgress updated) {
      progress = updated;
      listeners.forEach(listener -> listener.accept(updated));
    }

    private int listenerCount() {
      return listeners.size();
    }
  }

  private static final class DirectControl implements ConcurrencyControl {
    private final List<Duration> scheduledDelays = new ArrayList<>();
    private final Deque<TestTimer> timers = new ArrayDeque<>();

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
      final TestTimer timer = new TestTimer(runnable);
      scheduledDelays.add(delay);
      timers.addLast(timer);
      return timer;
    }

    private void runNextScheduled() {
      while (!timers.isEmpty()) {
        final TestTimer timer = timers.removeFirst();
        if (!timer.cancelled) {
          timer.runnable.run();
          return;
        }
      }
      throw new AssertionError("no scheduled task");
    }

    private List<Duration> retryDelays() {
      return List.copyOf(scheduledDelays);
    }
  }

  private static final class TestTimer implements ScheduledTimer {
    private final Runnable runnable;
    private boolean cancelled;

    private TestTimer(final Runnable runnable) {
      this.runnable = runnable;
    }

    @Override
    public void cancel() {
      cancelled = true;
    }
  }
}
