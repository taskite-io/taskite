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
package io.taskite.broker.registry;

import static org.junit.jupiter.api.Assertions.*;

import io.grpc.Status;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.taskite.broker.BrokerConfig;
import io.taskite.broker.common.ExponentialBackoff;
import io.taskite.broker.common.PlacementDriverBackoff;
import io.taskite.broker.runtime.BrokerRuntime;
import io.taskite.placementdriver.client.Client;
import io.taskite.placementdriver.protocol.DeregisterBrokerResponse;
import io.taskite.placementdriver.protocol.RegisterBrokerResponse;
import io.taskite.placementdriver.protocol.RenewBrokerResponse;
import io.taskite.scheduler.ActorScheduler;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class BrokerRegistrationBackpressureTest {
  private static final RuntimeException BACKPRESSURE =
      Status.RESOURCE_EXHAUSTED.withDescription("BACKPRESSURE: queue is full").asRuntimeException();

  @Test
  void backoffUsesConfigurationCapsAndResetsOnlyOnSuccess() {
    for (final long maximum : new long[] {30, 200}) {
      final var backoff = new PlacementDriverBackoff(registration(maximum));
      long ceiling = 10;
      for (int i = 0; i < 20; i++) {
        final long delay = backoff.nextDelay(BACKPRESSURE).toMillis();
        assertEquals(ceiling, delay);
        ceiling = Math.min(ceiling * 2, Math.min(maximum, 100));
      }
      assertEquals(
          Duration.ofMillis(100),
          backoff.nextDelay(Status.RESOURCE_EXHAUSTED.asRuntimeException()));
      assertEquals(Duration.ofMillis(100), backoff.nextDelay(null));
      assertEquals(10, backoff.nextDelay(BACKPRESSURE).toMillis());
    }
  }

  @Test
  void builderSupportsReferenceDefaultsSymmetricJitterAndStrictCaps() {
    final var defaults = ExponentialBackoff.builder().jitterFactor(0).build();
    assertEquals(50, defaults.nextDelay(0));
    assertEquals(80, defaults.nextDelay(50));
    assertEquals(5_000, defaults.nextDelay(Long.MAX_VALUE));
    final var negativeJitter = ExponentialBackoff.builder().random(fixedRandom(0)).build();
    final var positiveJitter = ExponentialBackoff.builder().random(fixedRandom(0.999999)).build();
    assertEquals(45, negativeJitter.nextDelay(0));
    assertEquals(55, positiveJitter.nextDelay(0));
    assertEquals(5_000, positiveJitter.nextDelay(5_000), "jitter must not exceed maxDelay");

    final var configured =
        ExponentialBackoff.builder()
            .minDelay(20)
            .maxDelay(100)
            .backoffFactor(3)
            .jitterFactor(0)
            .random(new Random(17))
            .build();
    assertEquals(20, configured.nextDelay(0));
    assertEquals(60, configured.nextDelay(20));
    assertEquals(100, configured.nextDelay(60));
    final var afterBuilderMutation = ExponentialBackoff.builder().minDelay(10).jitterFactor(0);
    final var built = afterBuilderMutation.build();
    afterBuilderMutation.minDelay(100);
    assertEquals(10, built.nextDelay(0), "built policy must retain its immutable configuration");
    assertThrows(NullPointerException.class, () -> ExponentialBackoff.builder().random(null));
    for (final double invalid : new double[] {-1, 0.5, Double.NaN, Double.POSITIVE_INFINITY}) {
      assertThrows(
          IllegalArgumentException.class,
          () -> ExponentialBackoff.builder().backoffFactor(invalid).build());
    }
    for (final double invalid : new double[] {-0.1, 1.1, Double.NaN, Double.POSITIVE_INFINITY}) {
      assertThrows(
          IllegalArgumentException.class,
          () -> ExponentialBackoff.builder().jitterFactor(invalid).build());
    }
  }

  @Test
  void symmetricJitterCannotCrossTheHeartbeatCap() {
    final var config =
        new BrokerConfig.Registration(
            "127.0.0.1:26501",
            Duration.ofMillis(10),
            Duration.ofSeconds(30),
            Duration.ofSeconds(1),
            new BrokerConfig.Backpressure(Duration.ofMillis(50), Duration.ofSeconds(5), 3, 1));
    final var backoff = new PlacementDriverBackoff(config);
    for (int i = 0; i < 200; i++) {
      final long delay = backoff.nextDelay(BACKPRESSURE).toMillis();
      assertTrue(
          delay >= 1 && delay <= 10, "jitter must respect the heartbeat even below minDelay");
    }
  }

  private static Random fixedRandom(final double value) {
    return new Random(17) {
      @Override
      public double nextDouble() {
        return value;
      }
    };
  }

  @Test
  void reportsCannotBypassBackoffAndRegistrationWaitsForOutstandingRenewal(
      @TempDir final Path directory) throws Exception {
    final var config =
        new BrokerConfig(
            new BrokerConfig.Identity("test", "b0", "test"),
            new BrokerConfig.Node(new BrokerConfig.Network("127.0.0.1", 12501, 12502), directory),
            registration(30),
            new BrokerConfig.Engine(
                new BrokerConfig.Snapshot(Duration.ofMinutes(15), 128L * 1024 * 1024),
                new BrokerConfig.Consensus(Duration.ofSeconds(1)),
                BrokerConfig.defaultProcessing()));
    final var scheduler = ActorScheduler.newActorScheduler().build();
    final var meters = new SimpleMeterRegistry();
    scheduler.start();
    final List<String> calls = new ArrayList<>();
    final List<CompletableActorFuture<?>> pending = new ArrayList<>();
    final List<RetryTimer> timers = new ArrayList<>();
    final Client client =
        (Client)
            Proxy.newProxyInstance(
                Client.class.getClassLoader(),
                new Class<?>[] {Client.class},
                (proxy, method, args) -> {
                  switch (method.getName()) {
                    case "registerBroker", "renewBroker":
                      calls.add(method.getName());
                      final var future = new CompletableActorFuture<>();
                      pending.add(future);
                      return future;
                    case "deregisterBroker":
                      return CompletableActorFuture.completed(
                          DeregisterBrokerResponse.getDefaultInstance());
                    case "topology":
                      return new CompletableActorFuture<>();
                    case "watchBrokerTopology":
                      return (AutoCloseable) () -> {};
                    default:
                      throw new AssertionError(method.getName());
                  }
                });
    final ConcurrencyControl control =
        (ConcurrencyControl)
            Proxy.newProxyInstance(
                ConcurrencyControl.class.getClassLoader(),
                new Class<?>[] {ConcurrencyControl.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("run")) {
                    ((Runnable) args[0]).run();
                    return null;
                  }
                  if (method.getName().equals("schedule")) {
                    final var timer = new RetryTimer((Duration) args[0], (Runnable) args[1]);
                    timers.add(timer);
                    return timer;
                  }
                  throw new AssertionError(method.getName());
                });
    try (final var runtime = new BrokerRuntime(config, 0, scheduler, meters);
        final var registration =
            new BrokerRegistrationActor(
                new BrokerRegistrationActor.Configuration(config, "test-incarnation"),
                new BrokerRegistrationActor.Context(runtime, control, client, () -> {}))) {
      final var registered = BrokerRegistrationActor.class.getDeclaredField("registered");
      registered.setAccessible(true);
      registered.setBoolean(registration, true);
      report(registration, true);
      pending.getFirst().completeExceptionally(BACKPRESSURE);
      final var firstTimer = timers.getLast();
      assertEquals(Duration.ofMillis(10), firstTimer.delay);
      report(registration, false);
      report(registration, true);
      assertEquals(List.of("renewBroker"), calls, "extra reports must wait for the retry timer");
      firstTimer.run();
      assertEquals(List.of("renewBroker", "renewBroker"), calls);
      final var lost = BrokerRegistrationActor.class.getDeclaredMethod("onRegistrationLost");
      lost.setAccessible(true);
      lost.invoke(registration);
      assertEquals(2, calls.size(), "REGISTER must wait for the outstanding RENEW");
      complete(pending.get(1), RenewBrokerResponse.getDefaultInstance());
      assertEquals(List.of("renewBroker", "renewBroker", "registerBroker"), calls);
      pending.get(2).completeExceptionally(BACKPRESSURE);
      final var registerTimer = timers.getLast();
      assertEquals(Duration.ofMillis(10), registerTimer.delay);
      report(registration, true);
      assertEquals(3, calls.size());
      registerTimer.run();
      assertEquals("registerBroker", calls.getLast(), "rejected REGISTER must remain REGISTER");
      complete(pending.get(3), RegisterBrokerResponse.getDefaultInstance());
      assertTrue(
          timers.stream().anyMatch(timer -> timer.delay.equals(Duration.ofMillis(100))),
          "success must restore the configured heartbeat");
    } finally {
      scheduler.stop().get(5, TimeUnit.SECONDS);
      meters.close();
    }
  }

  private static BrokerConfig.Registration registration(final long maxMillis) {
    return new BrokerConfig.Registration(
        "127.0.0.1:26501",
        Duration.ofMillis(100),
        Duration.ofSeconds(30),
        Duration.ofSeconds(5),
        new BrokerConfig.Backpressure(Duration.ofMillis(10), Duration.ofMillis(maxMillis), 2, 0));
  }

  private static void report(final BrokerRegistrationActor registration, final boolean periodic)
      throws Exception {
    final var method =
        BrokerRegistrationActor.class.getDeclaredMethod("reportPartitions", boolean.class);
    method.setAccessible(true);
    method.invoke(registration, periodic);
  }

  @SuppressWarnings("unchecked")
  private static void complete(final CompletableActorFuture<?> future, final Object value) {
    ((CompletableActorFuture<Object>) future).complete(value);
  }

  private static final class RetryTimer implements ScheduledTimer {
    private final Duration delay;
    private final Runnable action;
    private boolean cancelled;

    private RetryTimer(final Duration delay, final Runnable action) {
      this.delay = delay;
      this.action = action;
    }

    @Override
    public void cancel() {
      cancelled = true;
    }

    private void run() {
      assertFalse(cancelled);
      action.run();
    }
  }
}
