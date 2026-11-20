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
package io.taskite.gateway.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.scheduler.Actor;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class GatewayRuntimeTest {
  @Test
  void managementThreadsBoundConcurrentRequestActorExecution() throws Exception {
    final GatewayRuntime runtime = new GatewayRuntime(2);
    runtime.start().join();
    final CountDownLatch entered = new CountDownLatch(2);
    final CountDownLatch release = new CountDownLatch(1);
    final Actor first = blockingActor("gateway-request-test-1", entered, release);
    final Actor second = blockingActor("gateway-request-test-2", entered, release);

    try {
      final var firstStarted = runtime.scheduler().submitActor(first);
      final var secondStarted = runtime.scheduler().submitActor(second);

      assertEquals(2, runtime.managementThreads());
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      release.countDown();
      firstStarted.join();
      secondStarted.join();
    } finally {
      release.countDown();
      first.closeAsync().join();
      second.closeAsync().join();
      runtime.stop().join();
    }
  }

  private static Actor blockingActor(
      final String name, final CountDownLatch entered, final CountDownLatch release) {
    return Actor.newActor()
        .name(name)
        .actorStartedHandler(
            ignored -> {
              entered.countDown();
              try {
                release.await();
              } catch (final InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("request actor interrupted", error);
              }
            })
        .build();
  }
}
