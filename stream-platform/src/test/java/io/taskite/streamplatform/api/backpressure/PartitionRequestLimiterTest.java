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
package io.taskite.streamplatform.api.backpressure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

final class PartitionRequestLimiterTest {
  private final AtomicLong clock = new AtomicLong();

  @Test
  void partitionsHaveIndependentWindowsAndPermitsCompleteOnlyOnce() {
    final var first = limiter(BackpressureConfiguration.fixed(1));
    final var second = limiter(BackpressureConfiguration.fixed(1));
    final var permit = first.tryAcquire("command").orElseThrow();
    assertTrue(first.tryAcquire("command").isEmpty());
    assertTrue(second.tryAcquire("command").isPresent());
    permit.success();
    permit.drop();
    permit.ignore();
    assertEquals(0, first.inFlight());
    assertEquals(1, first.limit());
    assertEquals(1, second.inFlight());
  }

  @Test
  void exemptionsAndDisabledAdmissionDoNotConsumeOrSample() {
    for (final boolean enabled : List.of(true, false)) {
      final var config =
          new BackpressureConfiguration(
              enabled, BackpressureConfiguration.Algorithm.AIMD, null, null, null);
      final var limiter =
          new PartitionRequestLimiter<String>(config, "management"::equals, clock::get);
      limiter.tryAcquire("command").orElseThrow();
      final int counted = limiter.inFlight();
      for (int i = 0; i < 200; i++) {
        clock.addAndGet(Duration.ofSeconds(1).toNanos());
        limiter.tryAcquire("management").orElseThrow().drop();
      }
      assertEquals(counted, limiter.inFlight());
      assertEquals(100, limiter.limit());
      if (!enabled) {
        assertEquals(0, counted);
      }
    }
  }

  @Test
  void aimdIncreasesUnderLoadAndBacksOffOnSlowOrFailedCompletion() {
    final var limiter =
        limiter(
            new BackpressureConfiguration(
                true,
                BackpressureConfiguration.Algorithm.AIMD,
                new BackpressureConfiguration.Aimd(Duration.ofMillis(200), 2, 1, 3, 0.5),
                null,
                null));
    limiter.tryAcquire("command").orElseThrow().success();
    assertEquals(3, limiter.limit());
    limiter.tryAcquire("command").orElseThrow().success();
    assertEquals(3, limiter.limit());
    final var slow = limiter.tryAcquire("command").orElseThrow();
    clock.addAndGet(Duration.ofMillis(201).toNanos());
    slow.success();
    assertEquals(1, limiter.limit());
    limiter.tryAcquire("command").orElseThrow().drop();
    assertEquals(1, limiter.limit());
  }

  @Test
  void vegasColdStartAndDroppedRequestsUseLibraryFeedback() {
    final var limiter =
        limiter(
            new BackpressureConfiguration(
                true,
                BackpressureConfiguration.Algorithm.VEGAS,
                null,
                new BackpressureConfiguration.Vegas(20, 3, 6, 20),
                null));
    final var fast = limiter.tryAcquire("command").orElseThrow();
    clock.addAndGet(10);
    fast.success();
    // The library first establishes a baseline without growing the window.
    assertEquals(20, limiter.limit());
    for (int i = 0; i < 10; i++) {
      final var dropped = limiter.tryAcquire("command").orElseThrow();
      clock.addAndGet(100);
      dropped.drop();
      assertTrue(limiter.limit() >= 1 && limiter.limit() <= 20);
    }
    assertTrue(limiter.limit() < 20);
    assertEquals(0, limiter.inFlight());
  }

  @Test
  void invalidClockSamplesReleaseLibraryPermitsWithoutUpdatingWindow() {
    final var limiter =
        new PartitionRequestLimiter<String>(
            new BackpressureConfiguration(
                true,
                BackpressureConfiguration.Algorithm.VEGAS,
                null,
                new BackpressureConfiguration.Vegas(1, 3, 6, 20),
                null),
            ignored -> false,
            clock::get);
    final var zeroRtt = limiter.tryAcquire("command").orElseThrow();
    zeroRtt.success();
    assertEquals(0, limiter.inFlight());
    final var negativeRtt = limiter.tryAcquire("command").orElseThrow();
    clock.set(-10);
    negativeRtt.drop();
    assertEquals(1, limiter.limit());
    assertEquals(0, limiter.inFlight());
    limiter.tryAcquire("command").orElseThrow().ignore();
    assertEquals(0, limiter.inFlight());
  }

  @Test
  void resetFencesOldCallbacksAndRestoresInitialWindow() {
    final var limiter = limiter(BackpressureConfiguration.defaults());
    final var old = limiter.tryAcquire("command").orElseThrow();
    limiter.reset();
    final var current = limiter.tryAcquire("command").orElseThrow();
    old.drop();
    assertEquals(100, limiter.limit());
    assertEquals(1, limiter.inFlight());
    current.ignore();
    assertEquals(0, limiter.inFlight());
    assertEquals(100, limiter.limit());
  }

  @Test
  void concurrentAdmissionNeverExceedsWindow() throws Exception {
    final var limiter = limiter(BackpressureConfiguration.fixed(7));
    final var start = new CountDownLatch(1);
    try (var executor = Executors.newFixedThreadPool(16)) {
      final var attempts =
          new ArrayList<
              java.util.concurrent.Future<
                  java.util.Optional<PartitionRequestLimiter<String>.Permit>>>();
      for (int i = 0; i < 100; i++) {
        attempts.add(
            executor.submit(
                () -> {
                  assertTrue(start.await(5, TimeUnit.SECONDS));
                  return limiter.tryAcquire("command");
                }));
      }
      start.countDown();
      final var acquired = new ArrayList<PartitionRequestLimiter<String>.Permit>();
      for (final var attempt : attempts) {
        attempt.get(5, TimeUnit.SECONDS).ifPresent(acquired::add);
      }
      assertEquals(7, acquired.size());
      assertEquals(7, limiter.inFlight());
      final var completions = new ArrayList<java.util.concurrent.Future<?>>();
      for (final var permit : acquired) {
        completions.add(executor.submit(permit::success));
        completions.add(executor.submit(permit::drop));
      }
      for (final var completion : completions) {
        completion.get(5, TimeUnit.SECONDS);
      }
      assertEquals(0, limiter.inFlight());
    }
  }

  private PartitionRequestLimiter<String> limiter(final BackpressureConfiguration config) {
    return new PartitionRequestLimiter<>(config, ignored -> false, clock::getAndIncrement);
  }
}
