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
package io.taskite.streamplatform.runtime;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.cluster.raft.RaftPartition;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.scheduler.Actor;
import io.taskite.scheduler.ScheduledTimer;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import org.junit.jupiter.api.Test;

final class RaftRecoveryBarrierTest {
  @Test
  void leaderWaitsForFsmCallbackEvenWhenPublicCommittedIndexIsStillZero() {
    final Fixture fixture = new Fixture();
    fixture.term = 0;
    final var recovered =
        RaftRecoveryBarrier.await(fixture.partition, fixture.lifecycle, () -> false);
    assertFalse(recovered.isDone());
    fixture.term = 2;
    fixture.tick();
    assertTrue(recovered.isDone());
  }

  @Test
  void waitsForRecoveredPrefixBeforeOpeningStartup() {
    final Fixture fixture = new Fixture();
    fixture.committed = 14;
    fixture.applied = 1;
    final var recovered =
        RaftRecoveryBarrier.await(fixture.partition, fixture.lifecycle, () -> false);
    assertFalse(recovered.isDone());
    fixture.applied = 13;
    fixture.tick();
    assertFalse(recovered.isDone());
    fixture.applied = 14;
    fixture.tick();
    assertTrue(recovered.isDone());
    assertFalse(recovered.isCompletedExceptionally());
    assertTrue(fixture.timers.isEmpty());
  }

  @Test
  void capturesStartupBaselineWithoutWaitingForLaterAppends() {
    final Fixture fixture = new Fixture();
    fixture.committed = 14;
    final var recovered =
        RaftRecoveryBarrier.await(fixture.partition, fixture.lifecycle, () -> false);
    fixture.committed = 20;
    fixture.applied = 14;
    fixture.tick();
    assertTrue(recovered.isDone());
    assertFalse(recovered.isCompletedExceptionally());
  }

  @Test
  void shutdownEndsPendingRecoveryExceptionally() {
    final Fixture fixture = new Fixture();
    fixture.committed = 14;
    final var recovered =
        RaftRecoveryBarrier.await(fixture.partition, fixture.lifecycle, () -> fixture.closed);
    fixture.closed = true;
    fixture.tick();
    assertTrue(recovered.isCompletedExceptionally());
    assertTrue(fixture.timers.isEmpty());
  }

  private static final class Fixture {
    private long committed;
    private long applied;
    private boolean closed;
    private long term = 1;
    private final Deque<Runnable> timers = new ArrayDeque<>();
    private final RaftPartition partition =
        (RaftPartition)
            Proxy.newProxyInstance(
                RaftPartition.class.getClassLoader(),
                new Class<?>[] {RaftPartition.class},
                (proxy, method, args) ->
                    switch (method.getName()) {
                      case "committedIndex" -> committed;
                      case "lastAppliedIndex" -> applied;
                      case "partitionId" -> 1;
                      case "role" -> RaftRole.LEADER;
                      case "term" -> term;
                      default -> throw new UnsupportedOperationException(method.getName());
                    });
    private final Actor lifecycle =
        new Actor() {
          @Override
          public ScheduledTimer schedule(final Duration delay, final Runnable action) {
            timers.addLast(action);
            return () -> timers.remove(action);
          }
        };

    private void tick() {
      timers.removeFirst().run();
    }
  }
}
