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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.agrona.DeadlineTimerWheel;

/** Shared duration timers. Expirations are delivered to mailboxes, never executed on the ticker. */
final class ActorTimerQueue implements AutoCloseable {
  private static final int POLL_LIMIT = 1024;
  private final LongSupplier clock;
  private final DeadlineTimerWheel wheel;
  private final Map<Long, Timer> timers = new HashMap<>();
  private final Map<Object, Set<Timer>> owners = new HashMap<>();
  private boolean closed;

  ActorTimerQueue() {
    this(() -> TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
  }

  ActorTimerQueue(final LongSupplier clock) {
    this.clock = clock;
    wheel = new DeadlineTimerWheel(TimeUnit.MILLISECONDS, clock.getAsLong(), 1, 32);
  }

  synchronized ScheduledTimer schedule(
      final Object owner,
      final Duration delay,
      final Consumer<Runnable> mailbox,
      final Runnable action) {
    if (closed) {
      return () -> {};
    }
    final long now = clock.getAsLong();
    if (wheel.timerCount() == 0) {
      wheel.currentTickTime(now);
    }
    final long millis = Math.max(0, delay.toMillis());
    final long deadline = now > Long.MAX_VALUE - millis ? Long.MAX_VALUE - 1 : now + millis;
    final Timer timer = new Timer(owner, mailbox, action);
    timer.id = wheel.scheduleTimer(deadline);
    timers.put(timer.id, timer);
    owners.computeIfAbsent(owner, ignored -> new HashSet<>()).add(timer);
    return timer;
  }

  void poll() {
    final var expired = new ArrayList<Timer>();
    synchronized (this) {
      if (closed || wheel.timerCount() == 0) {
        return;
      }
      final long now = clock.getAsLong();
      int ticks = 0;
      long previousTick;
      do {
        previousTick = wheel.currentTickTime();
        wheel.poll(
            now,
            (unit, timestamp, id) -> {
              final Timer timer = timers.remove(id);
              timer.dispatched = true;
              expired.add(timer);
              return true;
            },
            POLL_LIMIT - expired.size());
      } while (wheel.timerCount() > 0
          && wheel.currentTickTime() != previousTick
          && wheel.currentTickTime() - wheel.tickResolution() <= now
          && expired.size() < POLL_LIMIT
          && ++ticks < POLL_LIMIT);
    }
    expired.forEach(timer -> timer.mailbox.accept(timer));
  }

  synchronized void cancelTimers(final Object owner) {
    final Set<Timer> owned = owners.get(owner);
    if (owned != null) {
      new ArrayList<>(owned).forEach(Timer::cancel);
    }
  }

  @Override
  public synchronized void close() {
    closed = true;
    owners.values().forEach(owned -> owned.forEach(timer -> timer.cancelled = true));
    owners.clear();
    timers.clear();
    wheel.clear();
  }

  private final class Timer implements ScheduledTimer, Runnable {
    private final Object owner;
    private final Consumer<Runnable> mailbox;
    private final Runnable action;
    private long id;
    private boolean dispatched;
    private boolean cancelled;

    private Timer(final Object owner, final Consumer<Runnable> mailbox, final Runnable action) {
      this.owner = owner;
      this.mailbox = mailbox;
      this.action = action;
    }

    @Override
    public void cancel() {
      synchronized (ActorTimerQueue.this) {
        if (cancelled) {
          return;
        }
        cancelled = true;
        if (!dispatched) {
          timers.remove(id);
          wheel.cancelTimer(id);
        }
        removeOwner();
      }
    }

    @Override
    public void run() {
      synchronized (ActorTimerQueue.this) {
        if (cancelled) {
          return;
        }
        cancelled = true;
        removeOwner();
      }
      action.run();
    }

    private void removeOwner() {
      final Set<Timer> owned = owners.get(owner);
      if (owned != null) {
        owned.remove(this);
        if (owned.isEmpty()) {
          owners.remove(owner);
        }
      }
    }
  }
}
