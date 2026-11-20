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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

final class ActorSchedulerTest {
  @Test
  void removesClosedActorsFromRegistry() throws Exception {
    final ActorScheduler scheduler =
        ActorScheduler.newActorScheduler().setCpuBoundActorThreadCount(1).build();
    scheduler.start();
    final Actor actor = Actor.newActor().name("short-lived-request").build();

    try {
      scheduler.submitActor(actor).join();
      assertEquals(1, scheduler.registeredActorCount());

      actor.closeAsync().join();
      awaitRegistrySize(scheduler, 0);
    } finally {
      scheduler.stop().join();
    }
  }

  @Test
  void removesActorsThatFailDuringStartup() throws Exception {
    final ActorScheduler scheduler =
        ActorScheduler.newActorScheduler().setCpuBoundActorThreadCount(1).build();
    scheduler.start();
    final Actor actor =
        Actor.newActor()
            .name("failed-request")
            .actorStartedHandler(
                ignored -> {
                  throw new IllegalStateException("failed");
                })
            .build();

    try {
      assertThrows(RuntimeException.class, () -> scheduler.submitActor(actor).join());
      awaitRegistrySize(scheduler, 0);
    } finally {
      scheduler.stop().join();
    }
  }

  private static void awaitRegistrySize(final ActorScheduler scheduler, final int expected)
      throws InterruptedException {
    final long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (scheduler.registeredActorCount() != expected && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(expected, scheduler.registeredActorCount());
  }
}
