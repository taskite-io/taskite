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

import static io.taskite.topology.RaftNodeState.valueOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.taskite.broker.BrokerConfig;
import io.taskite.broker.runtime.BrokerRuntime;
import io.taskite.cluster.raft.RaftNodeState;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.scheduler.ActorScheduler;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.streamplatform.runtime.transition.PartitionRole;
import io.taskite.streamplatform.runtime.transition.PartitionTransitionContext;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.ClusterTopology;
import io.taskite.topology.MemberRole;
import io.taskite.topology.Partition;
import io.taskite.topology.PartitionStatus;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class BrokerPartitionStateCollectorTest {
  @Test
  void processorRecoversWhenSnapshotCompactsTheEntireBusinessTail(@TempDir final Path directory)
      throws Exception {
    final int raftPort;
    try (final ServerSocket socket = new ServerSocket(0)) {
      raftPort = socket.getLocalPort();
    }
    final var config =
        new BrokerConfig(
            new BrokerConfig.Identity("demo", "broker-0", "test"),
            new BrokerConfig.Node(
                new BrokerConfig.Network("127.0.0.1", raftPort == 26501 ? 26502 : 26501, raftPort),
                directory),
            new BrokerConfig.Registration(
                "", Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofSeconds(1)),
            new BrokerConfig.Engine(
                new BrokerConfig.Snapshot(
                    Duration.ofMinutes(5), BrokerConfig.DEFAULT_SNAPSHOT_LOG_SIZE_THRESHOLD_BYTES),
                new BrokerConfig.Consensus(Duration.ofMillis(500)),
                BrokerConfig.defaultProcessing()));
    final var scheduler = ActorScheduler.newActorScheduler().build();
    final var registry = new SimpleMeterRegistry();
    scheduler.start();
    try (var runtime = new BrokerRuntime(config, 0, scheduler, registry)) {
      runtime.start();
      runtime.updateTopology(
          new ClusterTopology(
              "demo",
              1,
              1,
              1,
              List.of(
                  new Partition(
                      1,
                      new Partition.Leadership("broker-0", 1),
                      new Partition.Membership(List.of(), List.of(), Map.of("broker-0", 100), 0),
                      PartitionStatus.ACTIVE)),
              Map.of(
                  "broker-0",
                  new BrokerEndpoint(
                      "broker-0", "127.0.0.1", config.network().commandPort(), raftPort)),
              Instant.EPOCH));
      final var group = runtime.raftGroups().group(1).orElseThrow();
      final var collector = new BrokerPartitionStateCollector(config, runtime);
      await(() -> collector.collectPartitionStates().getFirst().readyForPromotion());
      group
          .submit(
              LoggedEvent.ofRecord(
                  123,
                  new RecordMetadata()
                      .recordVersion(1)
                      .recordType(RecordType.COMMAND)
                      .valueType(ValueType.JOB)
                      .intent(JobIntent.COMPLETE),
                  new JobRecord()))
          .join(5, TimeUnit.SECONDS);
      await(group::businessReady);
      final long tail = group.raftPartition().committedPosition();
      assertTrue(tail > 1, "command and rejection must occupy separate positions");
      group.takeSnapshotIfLeader().join(10, TimeUnit.SECONDS);
      for (long generation = 1; generation <= 2; generation++) {
        group
            .raftPartition()
            .installExecutionAuthority(generation - 1, generation, "snapshot-test")
            .join(10, TimeUnit.SECONDS);
        group.takeSnapshotIfLeader().join(10, TimeUnit.SECONDS);
      }
      await(
          () -> {
            try (final var reader = group.raftPartition().openLogReader()) {
              reader.seekToFirst();
              return !reader.hasNext();
            }
          });
      final var field = group.getClass().getDeclaredField("transitionContext");
      field.setAccessible(true);
      final var transition = (PartitionTransitionContext) field.get(group);
      final var previous = transition.getStreamProcessor();
      final var roleChanged = group.getClass().getDeclaredMethod("onRoleChange", RaftRole.class);
      roleChanged.setAccessible(true);
      roleChanged.invoke(group, RaftRole.LEADER);
      await(
          () ->
              transition.getStreamProcessor() != null
                  && transition.getStreamProcessor() != previous);
      await(() -> collector.collectPartitionStates().getFirst().readyForPromotion());
      assertTrue(group.serviceReady());
      assertTrue(group.businessReady());
      assertEquals(tail, group.raftPartition().committedPosition());
      assertEquals(
          BrokerPartitionLifecycle.ACTIVE,
          collector.collectPartitionStates().getFirst().lifecycle());
    } finally {
      scheduler.stop().get(5, TimeUnit.SECONDS);
      registry.close();
    }
  }

  @Test
  void runtimeObservationNamesAreRepresentableAtTheReportingBoundary() {
    for (final var state : RaftNodeState.values()) {
      assertEquals(state.name(), valueOf(state.name()).name());
    }
    for (final var role : PartitionRole.values()) {
      assertEquals(role.name(), MemberRole.valueOf(role.name()).name());
    }
  }

  @Test
  void committedBusinessBacklogDoesNotRemoveRecoveredVoterFromHealthyQuorum(
      @TempDir final Path directory) throws Exception {
    final int raftPort;
    try (final ServerSocket socket = new ServerSocket(0)) {
      raftPort = socket.getLocalPort();
    }
    final var config =
        new BrokerConfig(
            new BrokerConfig.Identity("demo", "broker-0", "test"),
            new BrokerConfig.Node(
                new BrokerConfig.Network("127.0.0.1", raftPort == 26501 ? 26502 : 26501, raftPort),
                directory),
            new BrokerConfig.Registration(
                "", Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofSeconds(1)),
            new BrokerConfig.Engine(
                new BrokerConfig.Snapshot(
                    BrokerConfig.DEFAULT_SNAPSHOT_INTERVAL,
                    BrokerConfig.DEFAULT_SNAPSHOT_LOG_SIZE_THRESHOLD_BYTES),
                new BrokerConfig.Consensus(Duration.ofMillis(500)),
                BrokerConfig.defaultProcessing()));
    final var scheduler = ActorScheduler.newActorScheduler().build();
    final var release = new CountDownLatch(1);
    final var registry = new SimpleMeterRegistry();
    scheduler.start();
    try (var runtime = new BrokerRuntime(config, 0, scheduler, registry)) {
      runtime.start();
      runtime.updateTopology(
          new ClusterTopology(
              "demo",
              1,
              1,
              1,
              List.of(
                  new Partition(
                      1,
                      new Partition.Leadership("broker-0", 1),
                      new Partition.Membership(List.of(), List.of(), Map.of("broker-0", 100), 0),
                      PartitionStatus.ACTIVE)),
              Map.of(
                  "broker-0",
                  new BrokerEndpoint(
                      "broker-0", "127.0.0.1", config.network().commandPort(), raftPort)),
              Instant.EPOCH));
      final var group = runtime.raftGroups().group(1).orElseThrow();
      final var collector = new BrokerPartitionStateCollector(config, runtime);
      await(() -> collector.collectPartitionStates().getFirst().readyForPromotion());
      final var field = group.getClass().getDeclaredField("transitionContext");
      field.setAccessible(true);
      final var transition = (PartitionTransitionContext) field.get(group);
      final var blocked = new CountDownLatch(1);
      transition
          .getStreamProcessor()
          .run(
              () -> {
                blocked.countDown();
                try {
                  if (!release.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError("processor not released");
                  }
                } catch (final InterruptedException error) {
                  Thread.currentThread().interrupt();
                  throw new AssertionError(error);
                }
              });
      assertTrue(blocked.await(5, TimeUnit.SECONDS));
      try {
        final long before = group.raftPartition().committedPosition();
        group.submit(
            LoggedEvent.ofRecord(
                123,
                new RecordMetadata()
                    .recordVersion(1)
                    .recordType(RecordType.COMMAND)
                    .valueType(ValueType.JOB)
                    .intent(JobIntent.COMPLETE),
                new JobRecord()));
        await(
            () ->
                group.raftPartition().committedPosition() > before
                    && group.lastAppliedIndex() == group.lastIndex());

        final var report = collector.collectPartitionStates().getFirst();
        assertEquals(MemberRole.LEADER, report.role());
        assertEquals(0, report.lag());
        assertFalse(group.businessReady(), "business processor has not consumed the command");
        assertFalse(report.readyForPromotion(), "promotion still requires business catch-up");
        assertTrue(group.serviceReady());
        assertTrue(report.election().serviceReady());
        assertEquals(BrokerPartitionLifecycle.ACTIVE, report.lifecycle());
      } finally {
        release.countDown();
      }
      await(() -> collector.collectPartitionStates().getFirst().readyForPromotion());

      final var fault = group.getClass().getDeclaredMethod("onFault", String.class);
      fault.setAccessible(true);
      fault.invoke(group, "test fault");
      final var faulted = collector.collectPartitionStates().getFirst();
      assertEquals(BrokerPartitionLifecycle.FAULTED, faulted.lifecycle());
      assertFalse(faulted.election().serviceReady());
      assertFalse(faulted.readyForPromotion());
    } finally {
      release.countDown();
      scheduler.stop().get(5, TimeUnit.SECONDS);
      registry.close();
    }
  }

  private static void await(final BooleanSupplier condition) throws Exception {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertTrue(condition.getAsBoolean(), "condition did not converge within 10s");
  }
}
