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

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

final class ActorTimerQueueTest {
  private final AtomicLong clock = new AtomicLong(100);
  private final ActorTimerQueue timers = new ActorTimerQueue(clock::get);
  private final ArrayDeque<Runnable> mailbox = new ArrayDeque<>();
  private final AtomicInteger calls = new AtomicInteger();

  @Test
  void shouldDeliverOnceOnlyAfterDeadline() {
    timers.schedule(this, Duration.ofMillis(10), mailbox::add, calls::incrementAndGet);
    clock.set(109);
    timers.poll();
    assertTrue(mailbox.isEmpty());
    clock.set(110);
    timers.poll();
    assertEquals(0, calls.get());
    mailbox.remove().run();
    timers.poll();
    assertEquals(1, calls.get());
    assertTrue(mailbox.isEmpty());
  }

  @Test
  void shouldCancelBeforeExpiryAndAfterMailboxDelivery() {
    final var before =
        timers.schedule(this, Duration.ofMillis(1), mailbox::add, calls::incrementAndGet);
    before.cancel();
    final var queued =
        timers.schedule(this, Duration.ofMillis(1), mailbox::add, calls::incrementAndGet);
    clock.set(102);
    timers.poll();
    assertEquals(1, mailbox.size());
    queued.cancel();
    mailbox.remove().run();
    assertEquals(0, calls.get());
  }

  @Test
  void shouldNotCancelReusedWheelSlotFromOldHandle() {
    final var old = timers.schedule(this, Duration.ZERO, mailbox::add, calls::incrementAndGet);
    timers.poll();
    timers.schedule(this, Duration.ZERO, mailbox::add, calls::incrementAndGet);
    old.cancel();
    timers.poll();
    mailbox.forEach(Runnable::run);
    assertEquals(1, calls.get());
  }

  @Test
  void shouldSkipLongIdlePeriodAndBoundExpiryBatch() {
    clock.addAndGet(Duration.ofDays(30).toMillis());
    for (int i = 0; i < 2500; i++) {
      timers.schedule(this, Duration.ZERO, mailbox::add, calls::incrementAndGet);
    }
    timers.poll();
    assertEquals(1024, mailbox.size());
    timers.poll();
    timers.poll();
    mailbox.forEach(Runnable::run);
    assertEquals(2500, calls.get());
  }

  @Test
  void shouldCancelOnlyClosingOwnerAndAllOnShutdown() {
    final Object other = new Object();
    timers.schedule(this, Duration.ZERO, mailbox::add, calls::incrementAndGet);
    timers.schedule(other, Duration.ZERO, mailbox::add, calls::incrementAndGet);
    timers.poll();
    timers.cancelTimers(this);
    mailbox.forEach(Runnable::run);
    assertEquals(1, calls.get());
    mailbox.clear();
    timers.schedule(other, Duration.ZERO, mailbox::add, calls::incrementAndGet);
    timers.poll();
    timers.close();
    mailbox.forEach(Runnable::run);
    timers.schedule(other, Duration.ZERO, mailbox::add, calls::incrementAndGet);
    timers.poll();
    assertEquals(1, calls.get());
  }

  @Test
  void shouldRunThroughActorAndCancelOnActorClose() throws Exception {
    try (var scheduler =
        ActorScheduler.newActorScheduler().setCpuBoundActorThreadCount(1).build()) {
      scheduler.start();
      final var fired = new CompletableFuture<Void>();
      final var actor = Actor.newActor().name("timer-test").build();
      scheduler.submitActor(actor).join();
      actor.schedule(Duration.ofMillis(1), () -> fired.complete(null));
      fired.get(5, TimeUnit.SECONDS);
      actor.schedule(Duration.ofMillis(50), calls::incrementAndGet);
      actor.close();
      // A second actor supplies a real timer barrier after the closed actor's deadline.
      final var barrier = new CompletableFuture<Void>();
      final var witness = Actor.newActor().name("timer-witness").build();
      scheduler.submitActor(witness).join();
      witness.schedule(Duration.ofMillis(100), () -> barrier.complete(null));
      barrier.get(5, TimeUnit.SECONDS);
      assertEquals(0, calls.get());
    }
  }
}
