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
package io.taskite.broker.bootstrap;

import static org.junit.jupiter.api.Assertions.*;

import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.taskite.broker.Broker;
import io.taskite.broker.BrokerConfig;
import io.taskite.placementdriver.client.Client;
import io.taskite.placementdriver.protocol.GetClusterConfigRequest;
import io.taskite.placementdriver.protocol.GetClusterConfigResponse;
import io.taskite.placementdriver.protocol.PlacementDriverGrpc;
import io.taskite.placementdriver.protocol.RegisterBrokerRequest;
import io.taskite.placementdriver.protocol.RegisterBrokerResponse;
import io.taskite.placementdriver.protocol.RenewBrokerRequest;
import io.taskite.placementdriver.protocol.RenewBrokerResponse;
import io.taskite.scheduler.Actor;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Initialization renewal must run independently from a slow partition startup actor. */
final class BrokerInitializationLeaseTest {
  @Test
  void initializationRetriesBackpressureAndHandoverWaitsForTheInFlightRenewal(
      @TempDir final Path directory) throws Exception {
    final var calls = new AtomicInteger();
    final var renewed = new CountDownLatch(1);
    final var pending = new CompletableActorFuture<RenewBrokerResponse>();
    final var config =
        new BrokerConfig(
            new BrokerConfig.Identity("lease", "b0", "test"),
            new BrokerConfig.Node(new BrokerConfig.Network("127.0.0.1", 12501, 12502), directory),
            new BrokerConfig.Registration(
                "127.0.0.1:26501",
                Duration.ofMillis(100),
                Duration.ofSeconds(30),
                Duration.ofSeconds(1),
                new BrokerConfig.Backpressure(Duration.ofMillis(10), Duration.ofMillis(40), 2, 0)),
            new BrokerConfig.Engine(
                new BrokerConfig.Snapshot(Duration.ofMinutes(15), 128L * 1024 * 1024),
                new BrokerConfig.Consensus(Duration.ofMillis(500)),
                BrokerConfig.defaultProcessing()));
    try (final var context = new BrokerStartupContext(config)) {
      context.placementDriverClient(
          (Client)
              Proxy.newProxyInstance(
                  Client.class.getClassLoader(),
                  new Class<?>[] {Client.class},
                  (proxy, method, args) -> {
                    if (method.getName().equals("close")) {
                      return null;
                    }
                    assertEquals("renewBroker", method.getName());
                    if (calls.incrementAndGet() == 1) {
                      return CompletableActorFuture.completedExceptionally(
                          Status.RESOURCE_EXHAUSTED
                              .withDescription("BACKPRESSURE: queue is full")
                              .asRuntimeException());
                    }
                    renewed.countDown();
                    return pending;
                  }));
      context.scheduler().start();
      context.markSchedulerStarted();
      try {
        context
            .scheduler()
            .submitActor((Actor) context.concurrencyControl())
            .get(5, TimeUnit.SECONDS);
        context
            .concurrencyControl()
            .call(
                () -> {
                  context.maintainInitializationLease();
                  return null;
                })
            .get(5, TimeUnit.SECONDS);
        assertTrue(renewed.await(2, TimeUnit.SECONDS), "one backpressure must trigger a retry");
        final var handover = context.finishInitializationLease();
        assertFalse(handover.isDone(), "runtime cannot start before the outstanding renewal ends");
        pending.complete(RenewBrokerResponse.getDefaultInstance());
        handover.get(2, TimeUnit.SECONDS);
        assertEquals(2, calls.get());
        context.requireInitializationLease();
      } finally {
        context.close();
        context.scheduler().stop().get(5, TimeUnit.SECONDS);
        context.markSchedulerStopped();
      }
    }
  }

  @Test
  void brokerStartFailsWhenLeaseIsRevokedDuringConfigurationRetry(@TempDir final Path directory)
      throws Exception {
    final var configurationCalls = new AtomicInteger();
    final var revoked = new AtomicBoolean();
    final var service =
        new PlacementDriverGrpc.PlacementDriverImplBase() {
          @Override
          public void registerBroker(
              final RegisterBrokerRequest request,
              final StreamObserver<RegisterBrokerResponse> observer) {
            observer.onNext(RegisterBrokerResponse.getDefaultInstance());
            observer.onCompleted();
          }

          @Override
          public void getClusterConfig(
              final GetClusterConfigRequest request,
              final StreamObserver<GetClusterConfigResponse> observer) {
            configurationCalls.incrementAndGet();
            observer.onError(
                Status.UNAVAILABLE.withDescription("retry configuration").asRuntimeException());
          }

          @Override
          public void renewBroker(
              final RenewBrokerRequest request,
              final StreamObserver<RenewBrokerResponse> observer) {
            assertTrue(request.getPreservePartitionObservations());
            if (configurationCalls.get() > 0) {
              revoked.set(true);
              observer.onError(
                  Status.FAILED_PRECONDITION
                      .withDescription("STALE_BROKER_INCARNATION")
                      .asRuntimeException());
            } else {
              observer.onNext(RenewBrokerResponse.getDefaultInstance());
              observer.onCompleted();
            }
          }
        };
    final var server = ServerBuilder.forPort(0).addService(service).build().start();
    final var config =
        new BrokerConfig(
            new BrokerConfig.Identity("lease", "retry-b0", "test"),
            new BrokerConfig.Node(new BrokerConfig.Network("127.0.0.1", 12511, 12512), directory),
            new BrokerConfig.Registration(
                "127.0.0.1:" + server.getPort(),
                Duration.ofMillis(200),
                Duration.ofSeconds(30),
                Duration.ofSeconds(1)),
            new BrokerConfig.Engine(
                new BrokerConfig.Snapshot(Duration.ofMinutes(15), 128L * 1024 * 1024),
                new BrokerConfig.Consensus(Duration.ofMillis(500)),
                BrokerConfig.defaultProcessing()));
    final var broker = new Broker(config);
    try {
      final var startup = broker.start();
      final var error =
          assertThrows(ExecutionException.class, () -> startup.get(5, TimeUnit.SECONDS));
      assertTrue(error.getCause().getMessage().contains("STALE_BROKER_INCARNATION"));
      assertTrue(revoked.get());
      assertEquals(1, configurationCalls.get(), "shutdown precedes the 1s configuration retry");
      broker.stop().get(5, TimeUnit.SECONDS);
      assertFalse(broker.ready());
    } finally {
      broker.stop().get(5, TimeUnit.SECONDS);
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void initializationKeepsLeaseDuringSlowStartupAndStopsOnAuthoritativeRevocation(
      @TempDir final Path directory) throws Exception {
    final var renewed = new AtomicInteger();
    final var revoke = new AtomicBoolean();
    final var stopped = new AtomicBoolean();
    final var config =
        new BrokerConfig(
            new BrokerConfig.Identity("lease", "b0", "test"),
            new BrokerConfig.Node(new BrokerConfig.Network("127.0.0.1", 12501, 12502), directory),
            new BrokerConfig.Registration(
                "127.0.0.1:12601",
                Duration.ofMillis(20),
                Duration.ofSeconds(1),
                Duration.ofMillis(20)),
            new BrokerConfig.Engine(
                new BrokerConfig.Snapshot(Duration.ofMinutes(15), 128L * 1024 * 1024),
                new BrokerConfig.Consensus(Duration.ofMillis(500)),
                BrokerConfig.defaultProcessing()));
    try (final var context = new BrokerStartupContext(config, () -> stopped.set(true))) {
      final Client client =
          (Client)
              Proxy.newProxyInstance(
                  Client.class.getClassLoader(),
                  new Class<?>[] {Client.class},
                  (proxy, method, args) -> {
                    if (method.getName().equals("close")) {
                      return null;
                    }
                    assertEquals("renewBroker", method.getName());
                    final var request = (RenewBrokerRequest) args[0];
                    assertTrue(request.getPreservePartitionObservations());
                    assertEquals(context.incarnationId(), request.getIncarnationId());
                    renewed.incrementAndGet();
                    return revoke.get()
                        ? CompletableActorFuture.completedExceptionally(
                            new IllegalStateException("STALE_BROKER_INCARNATION"))
                        : CompletableActorFuture.completed(
                            RenewBrokerResponse.getDefaultInstance());
                  });
      context.placementDriverClient(client);
      context.scheduler().start();
      context.markSchedulerStarted();
      final Actor startup = (Actor) context.concurrencyControl();
      context.scheduler().submitActor(startup).get(5, TimeUnit.SECONDS);
      startup
          .call(
              () -> {
                context.maintainInitializationLease();
                return null;
              })
          .get(5, TimeUnit.SECONDS);
      startup
          .call(
              () -> {
                Thread.sleep(250);
                return null;
              })
          .get(5, TimeUnit.SECONDS);
      assertTrue(renewed.get() >= 2, "independent actor must renew during blocked startup");
      revoke.set(true);
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (!stopped.get() && System.nanoTime() < deadline) Thread.sleep(10);
      assertTrue(stopped.get());
      assertThrows(IllegalStateException.class, context::requireInitializationLease);
      context.close();
      context.scheduler().stop().get(5, TimeUnit.SECONDS);
      context.markSchedulerStopped();
    }
  }
}
