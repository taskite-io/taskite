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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alipay.sofa.jraft.util.Endpoint;
import io.taskite.cluster.raft.RaftPartitionConfig;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.jraft.JRaftPartition;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
import io.taskite.scheduler.Actor;
import io.taskite.scheduler.ActorScheduler;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class StreamPartitionStartupTest {

  @Test
  void startupWaitsForBothActorsBeforeRegisteringRaftGroup(@TempDir final Path dataDirectory)
      throws Exception {
    final int raftPort = freePort();
    final StreamPartitionConfig config =
        StreamPartitionConfig.builder("startup-test", 1, "broker-1", dataDirectory)
            .peers(List.of(new RaftPeer("broker-1", "127.0.0.1", raftPort, true)))
            .electionTimeout(Duration.ofSeconds(1))
            .build();
    final JRaftServer server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
    final JRaftPartition raftPartition =
        JRaftPartition.builder(configToRaftConfig(config), server, dataDirectory).build();
    final ActorScheduler scheduler =
        ActorScheduler.newActorScheduler().setCpuBoundActorThreadCount(1).build();
    scheduler.start();
    final Actor lifecycle = noOpActor();
    scheduler.submitActor(lifecycle).get(5, TimeUnit.SECONDS);
    final StreamPartition partition = partition(config, raftPartition, lifecycle);
    final CompletableActorFuture<Void> partitionActorUp = new CompletableActorFuture<>();
    final CompletableActorFuture<Void> lifecycleActorUp = new CompletableActorFuture<>();

    try {
      partition.start(partitionActorUp, lifecycleActorUp);
      final ActorFuture<Void> startup = partition.startupFuture();

      assertFalse(startup.isDone());
      assertFalse(server.isStarted());

      partitionActorUp.complete(null);
      assertFalse(startup.isDone());
      assertFalse(server.isStarted());

      lifecycleActorUp.complete(null);
      startup.get(5, TimeUnit.SECONDS);
      assertTrue(startup.isDone());
      assertFalse(startup.isCompletedExceptionally());
      assertTrue(server.isStarted());
      assertNotNull(raftPartition.node());
    } finally {
      raftPartition.close();
      server.close();
      scheduler.stop().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void actorStartupFailureIsPropagated() {
    final StreamPartitionConfig config =
        StreamPartitionConfig.builder("startup-test", 1, "broker-1", Path.of("target")).build();
    final StreamPartition partition = partition(config, null);
    final IllegalStateException failure = new IllegalStateException("actor failed");

    partition.start(
        CompletableActorFuture.completedExceptionally(failure), new CompletableActorFuture<>());
    final ActorFuture<Void> startup = partition.startupFuture();

    assertTrue(startup.isCompletedExceptionally());
    assertSame(failure, startup.getException());
  }

  private static StreamPartition partition(
      final StreamPartitionConfig config, final JRaftPartition raftPartition) {
    return partition(config, raftPartition, noOpActor());
  }

  private static StreamPartition partition(
      final StreamPartitionConfig config,
      final JRaftPartition raftPartition,
      final Actor lifecycle) {
    return new StreamPartition(
        config,
        raftPartition,
        new StreamPartitionRuntime(
            new StreamPartitionRuntime.Actors(noOpActor(), lifecycle, null),
            new StreamPartitionRuntime.Processing(null, null, null, null, null),
            () -> 0L));
  }

  private static Actor noOpActor() {
    return new Actor() {
      @Override
      public String getName() {
        return "startup-test-actor";
      }

      @Override
      public void run(final Runnable action) {
        // Startup only needs to verify that the initial lifecycle transition was accepted.
      }
    };
  }

  private static RaftPartitionConfig configToRaftConfig(final StreamPartitionConfig config) {
    return RaftPartitionConfig.builder(
            config.clusterId(), config.partitionId(), config.localMemberId())
        .peers(config.peers())
        .dataDirectory(config.dataDirectory())
        .electionTimeout(config.electionTimeout())
        .build();
  }

  private static int freePort() throws IOException {
    try (final ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
