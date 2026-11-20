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
package io.taskite.broker.partition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alipay.sofa.jraft.util.Endpoint;
import io.micrometer.core.instrument.Metrics;
import io.taskite.broker.BrokerConfig;
import io.taskite.broker.state.BrokerPartitionOperationManifestStore;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.JRaftPartition;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.TopologyOperationType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.scheduler.Actor;
import io.taskite.scheduler.ActorScheduler;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.streamplatform.runtime.transition.PartitionTransitionContext;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.ClusterTopology;
import io.taskite.topology.Partition;
import io.taskite.topology.PartitionStatus;
import io.taskite.topology.RaftConfigurationDigest;
import io.taskite.topology.RaftConfigurationReference;
import io.taskite.topology.RecoveryFence;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class BrokerPartitionChangeExecutorTest {
  @Test
  void firstAddCreatesUnassignedPartitionAndFencesRedelivery(@TempDir final Path directory)
      throws Exception {
    final int raftPort = freePort();
    final var scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    try (var server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
        var groups =
            new BrokerPartitionManager(
                managerConfig("broker-0", raftPort, directory, Duration.ofMillis(500)),
                server,
                scheduler,
                Metrics.globalRegistry)) {
      final var empty = topology(1, List.of(), List.of(), raftPort);
      final var creating =
          new ClusterTopology(
              "demo",
              1,
              1,
              3,
              List.of(
                  new Partition(
                      1,
                      new Partition.Leadership("", 0),
                      new Partition.Membership(List.of(), List.of(), Map.of(), 0),
                      PartitionStatus.CREATING)),
              empty.brokers(),
              Instant.EPOCH);
      groups.reconcile(creating);
      final var actor = Actor.newActor().name("first-add-test").build();
      scheduler.submitActor(actor).get(5, TimeUnit.SECONDS);
      final var executor = new JRaftBrokerPartitionChangeExecutor(groups, actor);
      final var configuration =
          new BrokerPartitionOperation.Configuration(
              List.of(new BrokerPartitionMember("broker-0", true, 100)),
              new RaftConfigurationReference(0, RaftConfigurationDigest.of(List.of())),
              new RecoveryFence(0, 0, ""));
      final var request =
          new BrokerPartitionOperation(
              new BrokerPartitionOperation.Partition("demo", 1),
              new BrokerPartitionOperation.Operation(
                  "initial-add", TopologyOperationType.ADD, "broker-0", 0),
              configuration);
      for (final var invalid :
          List.of(
              new BrokerPartitionOperation(
                  request.partition(),
                  new BrokerPartitionOperation.Operation(
                      "wrong-target", TopologyOperationType.ADD, "broker-1", 0),
                  configuration),
              new BrokerPartitionOperation(
                  request.partition(),
                  request.operation(),
                  configuration,
                  new BrokerPartitionOperation.Authority(1, "later-plan")),
              new BrokerPartitionOperation(
                  request.partition(),
                  request.operation(),
                  new BrokerPartitionOperation.Configuration(
                      configuration.members(),
                      new RaftConfigurationReference(1, "existing-configuration"),
                      configuration.recoveryFence())))) {
        assertThrows(
            ExecutionException.class, () -> executor.apply(invalid).get(5, TimeUnit.SECONDS));
        assertTrue(groups.group(1).isEmpty());
      }
      executor.apply(request).get(15, TimeUnit.SECONDS);
      final var created = groups.group(1).orElseThrow();
      groups.reconcile(creating);
      executor.apply(request).get(5, TimeUnit.SECONDS);
      assertEquals(created, groups.group(1).orElseThrow());
      assertEquals(
          BrokerPartitionOperationStatus.APPLIED,
          executor.latestOperation(1).orElseThrow().status());
      await(created::businessReady);
      final var installed =
          executor.installAuthority("demo", 1, 0, 1, "replacement", 0).get(15, TimeUnit.SECONDS);
      final var stale =
          assertThrows(
              ExecutionException.class, () -> executor.apply(request).get(5, TimeUnit.SECONDS));
      assertTrue(stale.getCause().getMessage().contains("PARTITION_OPERATION_SUPERSEDED"));
      assertEquals(
          installed.configurationDigest(), created.raftPartition().appliedConfigurationDigest());
      assertEquals(created, groups.group(1).orElseThrow());
    } finally {
      scheduler.stop().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void installedAuthorityRejectsOldMembershipAndBootstrapRequests(@TempDir final Path directory)
      throws Exception {
    final int raftPort = freePort();
    final var scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    try (var server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
        var groups =
            new BrokerPartitionManager(
                managerConfig("broker-0", raftPort, directory, Duration.ofMillis(500)),
                server,
                scheduler,
                Metrics.globalRegistry)) {
      groups.reconcile(topology(1, List.of("broker-0"), List.of(), raftPort));
      await(() -> groups.group(1).orElseThrow().businessReady());
      final var actor = Actor.newActor().name("authority-fence-test").build();
      scheduler.submitActor(actor).get(5, TimeUnit.SECONDS);
      final var executor = new JRaftBrokerPartitionChangeExecutor(groups, actor);
      final var installed =
          executor.installAuthority("demo", 1, 0, 1, "successor", 0).get(15, TimeUnit.SECONDS);
      assertEquals(1, installed.executionGeneration());
      assertEquals("successor", installed.ownerChangeId());
      final var members = installed.members();
      final var configuration =
          new BrokerPartitionOperation.Configuration(
              members,
              new RaftConfigurationReference(
                  installed.configurationIndex(), installed.configurationDigest()),
              new RecoveryFence(0, 0, ""));
      for (final var type : List.of(TopologyOperationType.JOIN, TopologyOperationType.ADD)) {
        final var stale =
            new BrokerPartitionOperation(
                new BrokerPartitionOperation.Partition("demo", 1),
                new BrokerPartitionOperation.Operation("old-" + type, type, "broker-0", 0),
                configuration);
        final var error =
            assertThrows(
                ExecutionException.class, () -> executor.apply(stale).get(5, TimeUnit.SECONDS));
        assertTrue(error.getCause().getMessage().contains("PARTITION_OPERATION_SUPERSEDED"));
      }
      final var current =
          new BrokerPartitionOperation(
              new BrokerPartitionOperation.Partition("demo", 1),
              new BrokerPartitionOperation.Operation(
                  "current-transfer", TopologyOperationType.TRANSFER_LEADER, "broker-0", 0),
              configuration,
              new BrokerPartitionOperation.Authority(1, "successor"));
      executor.apply(current).get(10, TimeUnit.SECONDS);
      assertEquals(
          installed.configurationDigest(),
          executor.readConfiguration("demo", 1).get(10, TimeUnit.SECONDS).configurationDigest());
      assertEquals(
          BrokerPartitionOperationStatus.APPLIED,
          executor.latestOperation(1).orElseThrow().status());
    } finally {
      scheduler.stop().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void leadershipTransferRequestsObservationFromNewLeader(@TempDir final Path directory)
      throws Exception {
    final int firstPort = freePort();
    final int secondPort = freePort();
    final var scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    try (var firstServer = new JRaftServer(new Endpoint("127.0.0.1", firstPort));
        var secondServer = new JRaftServer(new Endpoint("127.0.0.1", secondPort));
        var first =
            new BrokerPartitionManager(
                managerConfig(
                    "broker-0", firstPort, directory.resolve("first"), Duration.ofMillis(500)),
                firstServer,
                scheduler,
                Metrics.globalRegistry);
        var second =
            new BrokerPartitionManager(
                managerConfig(
                    "broker-1", secondPort, directory.resolve("second"), Duration.ofMillis(500)),
                secondServer,
                scheduler,
                Metrics.globalRegistry)) {
      final var topology =
          topology(1, List.of("broker-0", "broker-1"), List.of(), firstPort, secondPort);
      first.reconcile(topology);
      second.reconcile(topology);
      await(
          () ->
              first.group(1).orElseThrow().businessReady()
                  && second.group(1).orElseThrow().businessReady()
                  && (first.partition(1).orElseThrow().role() == RaftRole.LEADER
                      || second.partition(1).orElseThrow().role() == RaftRole.LEADER));
      final var source =
          first.partition(1).orElseThrow().role() == RaftRole.LEADER ? first : second;
      final var target = source == first ? second : first;
      final var actor = Actor.newActor().name("leadership-transfer-observation-test").build();
      scheduler.submitActor(actor).get(5, TimeUnit.SECONDS);
      final var executor = new JRaftBrokerPartitionChangeExecutor(source, actor);
      final var installed =
          executor.installAuthority("demo", 1, 0, 1, "transfer-plan", 0).get(15, TimeUnit.SECONDS);
      final var request =
          new BrokerPartitionOperation(
              new BrokerPartitionOperation.Partition("demo", 1),
              new BrokerPartitionOperation.Operation(
                  "transfer-to-other-voter",
                  TopologyOperationType.TRANSFER_LEADER,
                  target.localBrokerId(),
                  0),
              new BrokerPartitionOperation.Configuration(
                  installed.members(),
                  new RaftConfigurationReference(
                      installed.configurationIndex(), installed.configurationDigest()),
                  new RecoveryFence(0, 0, "")),
              new BrokerPartitionOperation.Authority(1, "transfer-plan"));
      final var error =
          assertThrows(
              ExecutionException.class, () -> executor.apply(request).get(15, TimeUnit.SECONDS));
      await(
          () ->
              target.partition(1).orElseThrow().role() == RaftRole.LEADER
                  && target.group(1).orElseThrow().businessReady());
      final var targetExecutor = new JRaftBrokerPartitionChangeExecutor(target, actor);
      final var observed = targetExecutor.readConfiguration("demo", 1).get(10, TimeUnit.SECONDS);
      assertEquals(target.localBrokerId(), observed.leaderId());
      assertEquals(installed.executionGeneration(), observed.executionGeneration());
      assertEquals(installed.ownerChangeId(), observed.ownerChangeId());
      assertEquals(installed.configurationDigest(), observed.configurationDigest());
      assertEquals("NOT_LEADER", error.getCause().getMessage());
    } finally {
      scheduler.stop().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void sameConfigurationRecommittedAfterRestartAcceptsOlderAppliedIndex(
      @TempDir final Path directory) throws Exception {
    final int raftPort = freePort();
    final var scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    try (var server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
        var groups =
            new BrokerPartitionManager(
                managerConfig("broker-0", raftPort, directory, Duration.ofMillis(500)),
                server,
                scheduler,
                Metrics.globalRegistry)) {
      final var topology = topology(1, List.of("broker-0"), List.of(), raftPort);
      groups.reconcile(topology);
      await(() -> groups.group(1).orElseThrow().businessReady());
      await(() -> groups.partition(1).orElseThrow().electionState().configurationIndex() > 0);
      final var before = groups.partition(1).orElseThrow().electionState();
      final String digest = groups.partition(1).orElseThrow().appliedConfigurationDigest();
      groups.reconcile(topology(2, List.of(), List.of(), raftPort));
      groups.reconcile(topology(3, List.of("broker-0"), List.of(), raftPort));
      await(() -> groups.group(1).orElseThrow().businessReady());
      await(() -> groups.partition(1).orElseThrow().electionState().term() > before.term());
      await(
          () ->
              groups.partition(1).orElseThrow().electionState().configurationIndex()
                  > before.configurationIndex());
      final var after = groups.partition(1).orElseThrow().electionState();
      assertTrue(after.configurationIndex() > before.configurationIndex());
      assertEquals(digest, groups.partition(1).orElseThrow().appliedConfigurationDigest());
      final var actor = Actor.newActor().name("recommitted-config-test").build();
      scheduler.submitActor(actor).get(5, TimeUnit.SECONDS);
      final var executor = new JRaftBrokerPartitionChangeExecutor(groups, actor);
      final var members =
          List.of(
              new BrokerPartitionMember("broker-0", true, after.priority()),
              new BrokerPartitionMember("broker-1", false, 0));
      final var futureSource =
          new BrokerPartitionOperation(
              new BrokerPartitionOperation.Partition("demo", 1),
              new BrokerPartitionOperation.Operation(
                  "future-source", TopologyOperationType.JOIN, "broker-1", 0),
              new BrokerPartitionOperation.Configuration(
                  members,
                  new RaftConfigurationReference(after.configurationIndex() + 1, digest),
                  new RecoveryFence(0, 0, "")));
      final var error =
          assertThrows(
              ExecutionException.class,
              () -> executor.apply(futureSource).get(5, TimeUnit.SECONDS));
      assertTrue(error.getCause().getMessage().contains("STALE_CONFIGURATION"));
      final var request =
          new BrokerPartitionOperation(
              futureSource.partition(),
              new BrokerPartitionOperation.Operation(
                  "recommitted-source", TopologyOperationType.JOIN, "broker-1", 0),
              new BrokerPartitionOperation.Configuration(
                  members,
                  new RaftConfigurationReference(before.configurationIndex(), digest),
                  new RecoveryFence(0, 0, "")));
      executor.apply(request).get(15, TimeUnit.SECONDS);
      assertEquals(
          BrokerPartitionOperationStatus.APPLIED,
          executor.latestOperation(1).orElseThrow().status());
    } finally {
      scheduler.stop().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void businessReadinessRemainsFalseUntilProcessorConsumesCommittedCommand(
      @TempDir final Path directory) throws Exception {
    final int raftPort = freePort();
    final var scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    final var release = new CountDownLatch(1);
    try (var server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
        var groups =
            new BrokerPartitionManager(
                managerConfig("broker-0", raftPort, directory, Duration.ofMillis(500)),
                server,
                scheduler,
                Metrics.globalRegistry)) {
      groups.reconcile(topology(1, List.of("broker-0"), List.of(), raftPort));
      final var group = groups.group(1).orElseThrow();
      await(group::businessReady);
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
                    .recordType(RecordType.COMMAND)
                    .valueType(ValueType.JOB)
                    .intent(JobIntent.COMPLETE),
                new JobRecord()));
        await(
            () ->
                group.raftPartition().committedPosition() > before
                    && group.lastAppliedIndex() == group.lastIndex());
        assertFalse(
            group.businessReady(),
            "Raft FSM caught up while the business processor still holds the command");
      } finally {
        release.countDown();
      }
      await(group::businessReady);
    } finally {
      release.countDown();
      scheduler.stop().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void identicalInProgressRequestsShareMembershipExecution(@TempDir final Path directory)
      throws Exception {
    final int raftPort = freePort();
    final var scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    try (var server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
        var groups =
            new BrokerPartitionManager(
                managerConfig("broker-0", raftPort, directory, Duration.ofMillis(500)),
                server,
                scheduler,
                Metrics.globalRegistry)) {
      groups.reconcile(topology(1, List.of("broker-0"), List.of(), raftPort));
      final var actor = Actor.newActor().name("duplicate-operation-test").build();
      scheduler.submitActor(actor).get(5, TimeUnit.SECONDS);
      final var executor = new JRaftBrokerPartitionChangeExecutor(groups, actor);
      final var request =
          new BrokerPartitionOperation(
              new BrokerPartitionOperation.Partition("demo", 1),
              new BrokerPartitionOperation.Operation(
                  "join-duplicate", TopologyOperationType.JOIN, "broker-1", 0),
              new BrokerPartitionOperation.Configuration(
                  List.of(
                      new BrokerPartitionMember("broker-0", true, 10),
                      new BrokerPartitionMember("broker-1", false, 0)),
                  new RaftConfigurationReference(
                      0, groups.partition(1).orElseThrow().appliedConfigurationDigest()),
                  new RecoveryFence(0, 0, "")));
      final var results =
          actor
              .call(() -> List.of(executor.apply(request), executor.apply(request)))
              .get(5, TimeUnit.SECONDS);
      for (final var result : results) {
        result.get(15, TimeUnit.SECONDS);
      }
      assertEquals(
          BrokerPartitionOperationStatus.APPLIED,
          executor.latestOperation(1).orElseThrow().status());
    } finally {
      scheduler.stop().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void addSurvivesTopologyRefreshBeforeFirstAcknowledgement(@TempDir final Path directory)
      throws Exception {
    final int raftPort = freePort();
    final var scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    try (var server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
        var groups =
            new BrokerPartitionManager(
                managerConfig("broker-0", raftPort, directory, Duration.ofMillis(500)),
                server,
                scheduler,
                Metrics.globalRegistry)) {
      final var creating = topology(1, List.of(), List.of(), raftPort);
      groups.reconcile(creating);
      final var request =
          new BrokerPartitionOperation(
              new BrokerPartitionOperation.Partition("demo", 1),
              new BrokerPartitionOperation.Operation(
                  "first-add", TopologyOperationType.ADD, "broker-0", 100),
              new BrokerPartitionOperation.Configuration(
                  List.of(new BrokerPartitionMember("broker-0", true, 100)),
                  new RaftConfigurationReference(0, ""),
                  new RecoveryFence(0, 0, "")));
      new BrokerPartitionMembershipManager(groups).addPartition(request).get(5, TimeUnit.SECONDS);
      final var created = groups.group(1).orElseThrow();
      groups.reconcile(creating);
      assertEquals(created, groups.group(1).orElse(null));
      groups.reconcile(topology(2, List.of("broker-0"), List.of(), raftPort));
      assertEquals(created, groups.group(1).orElse(null));
      groups.reconcile(topology(3, List.of("broker-1"), List.of(), raftPort));
      assertTrue(
          groups.group(1).isEmpty(), "confirmed assignment must release provisional protection");
    } finally {
      scheduler.stop().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void staleSourceAndSupersededAppliedOperationCannotReconfigureGroup(@TempDir final Path directory)
      throws Exception {
    final int raftPort = freePort();
    final var scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    try (var server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
        var groups =
            new BrokerPartitionManager(
                managerConfig("broker-0", raftPort, directory, Duration.ofMillis(500)),
                server,
                scheduler,
                Metrics.globalRegistry)) {
      groups.reconcile(topology(1, List.of("broker-0"), List.of(), raftPort));
      final var actor = Actor.newActor().name("stale-operation-test").build();
      scheduler.submitActor(actor).get(5, TimeUnit.SECONDS);
      final var executor = new JRaftBrokerPartitionChangeExecutor(groups, actor);
      final var request =
          new BrokerPartitionOperation(
              new BrokerPartitionOperation.Partition("demo", 1),
              new BrokerPartitionOperation.Operation(
                  "stale-join", TopologyOperationType.JOIN, "broker-1", 0),
              new BrokerPartitionOperation.Configuration(
                  List.of(
                      new BrokerPartitionMember("broker-0", true, 10),
                      new BrokerPartitionMember("broker-1", false, 0)),
                  new RaftConfigurationReference(0, "stale-source-digest"),
                  new RecoveryFence(0, 0, "")));
      final var stale =
          assertThrows(
              ExecutionException.class, () -> executor.apply(request).get(5, TimeUnit.SECONDS));
      assertTrue(stale.getCause().getMessage().contains("STALE_CONFIGURATION"));
      final var store =
          new BrokerPartitionOperationManifestStore(groups.partition(1).orElseThrow().storage());
      actor
          .call(
              () -> {
                store.put(
                    new BrokerPartitionOperationManifest(
                        new BrokerPartitionOperationManifest.Identity(1, request.opId()),
                        new BrokerPartitionOperationManifest.Result(
                            BrokerPartitionOperationStatus.APPLIED, 1, ""),
                        BrokerPartitionOperationManifest.Evidence.UNKNOWN));
                return null;
              })
          .get(5, TimeUnit.SECONDS);
      final var superseded =
          assertThrows(
              ExecutionException.class, () -> executor.apply(request).get(5, TimeUnit.SECONDS));
      assertTrue(superseded.getCause().getMessage().contains("OPERATION_SUPERSEDED"));
      assertEquals(
          List.of("broker-0"),
          groups.partition(1).orElseThrow().members().stream()
              .map(member -> member.memberId())
              .toList());
    } finally {
      scheduler.stop().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void executorResetRetryAndStaleEpochPreserveAppliedMembership(@TempDir final Path tempDir)
      throws Exception {
    final int raftPort = freePort();
    final ActorScheduler scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    try (JRaftServer server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
        BrokerPartitionManager groups =
            new BrokerPartitionManager(
                managerConfig("broker-0", raftPort, tempDir, Duration.ofMillis(500)),
                server,
                scheduler,
                Metrics.globalRegistry)) {
      groups.reconcile(topology(1, List.of("broker-0"), List.of(), raftPort));
      final Actor actor = Actor.newActor().name("reset-retry-test").build();
      scheduler.submitActor(actor).get(10, TimeUnit.SECONDS);
      final var executor = new JRaftBrokerPartitionChangeExecutor(groups, actor);
      final var reset =
          new BrokerPartitionOperation(
              new BrokerPartitionOperation.Partition("demo", 1),
              new BrokerPartitionOperation.Operation(
                  "reset-five", TopologyOperationType.RESET_PEERS, "broker-0", 5),
              new BrokerPartitionOperation.Configuration(
                  List.of(new BrokerPartitionMember("broker-0", true, 10)),
                  new RaftConfigurationReference(
                      0,
                      groups.group(1).orElseThrow().raftPartition().appliedConfigurationDigest()),
                  new RecoveryFence(0, 5, "fence-five")));
      executor.apply(reset).get(15, TimeUnit.SECONDS);
      executor.apply(reset).get(5, TimeUnit.SECONDS);
      assertEquals(
          BrokerPartitionOperationStatus.APPLIED,
          executor.latestOperation(1).orElseThrow().status());
      assertEquals(5, groups.localRecoveryEpoch(1));
      final var stale =
          new BrokerPartitionOperation(
              new BrokerPartitionOperation.Partition("demo", 1),
              new BrokerPartitionOperation.Operation(
                  "reset-four", TopologyOperationType.RESET_PEERS, "broker-0", 4),
              new BrokerPartitionOperation.Configuration(
                  reset.members(),
                  new RaftConfigurationReference(0, ""),
                  new RecoveryFence(0, 4, "fence-four")));
      final var rejected =
          assertThrows(
              ExecutionException.class, () -> executor.apply(stale).get(5, TimeUnit.SECONDS));
      assertTrue(rejected.getCause().getMessage().contains("STALE_OPERATION"));
      assertEquals(5, groups.localRecoveryEpoch(1));
      assertEquals(1, groups.partition(1).orElseThrow().members().size());
    } finally {
      scheduler.stop().get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void delayedResetCannotOverwriteAnotherSourceConfiguration(@TempDir final Path tempDir)
      throws Exception {
    final int raftPort = freePort();
    final ActorScheduler scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    try (JRaftServer server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
        BrokerPartitionManager groups =
            new BrokerPartitionManager(
                managerConfig("broker-0", raftPort, tempDir, Duration.ofSeconds(5)),
                server,
                scheduler,
                Metrics.globalRegistry)) {
      groups.reconcile(topology(1, List.of("broker-0"), List.of(), raftPort));
      final Actor actor = Actor.newActor().name("stale-reset").build();
      scheduler.submitActor(actor).get(10, TimeUnit.SECONDS);
      final var executor = new JRaftBrokerPartitionChangeExecutor(groups, actor);
      final var group = groups.group(1).orElseThrow().raftPartition();
      final var before = group.members();
      final var request =
          new BrokerPartitionOperation(
              new BrokerPartitionOperation.Partition("demo", 1),
              new BrokerPartitionOperation.Operation(
                  "stale-reset", TopologyOperationType.RESET_PEERS, "broker-0", 5),
              new BrokerPartitionOperation.Configuration(
                  List.of(new BrokerPartitionMember("broker-0", true, 10)),
                  new RaftConfigurationReference(0, "earlier-configuration"),
                  new RecoveryFence(0, 5, "proof-for-earlier-configuration")));
      final var failure =
          assertThrows(
              ExecutionException.class, () -> executor.apply(request).get(5, TimeUnit.SECONDS));
      assertTrue(failure.getCause().getMessage().contains("STALE_CONFIGURATION"));
      assertEquals(0, groups.localRecoveryEpoch(1));
      assertEquals(before, group.members());
      assertTrue(executor.latestOperation(1).isEmpty());
    } finally {
      scheduler.stop().get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void usesExplicitRemoteRaftPortFromTopology(@TempDir final Path tempDir) throws Exception {
    final int localRaftPort = freePort();
    final int remoteRaftPort = freePort();
    final ActorScheduler scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    try (JRaftServer server = new JRaftServer(new Endpoint("127.0.0.1", localRaftPort));
        BrokerPartitionManager groups =
            new BrokerPartitionManager(
                managerConfig("broker-0", localRaftPort, tempDir, Duration.ofSeconds(5)),
                server,
                scheduler,
                Metrics.globalRegistry)) {
      final ClusterTopology topology =
          new ClusterTopology(
              "demo",
              1,
              1,
              2,
              List.of(
                  new Partition(
                      1,
                      new Partition.Leadership("broker-0", 1),
                      new Partition.Membership(
                          List.of("broker-0", "broker-1"), List.of(), Map.of(), 0),
                      PartitionStatus.ACTIVE)),
              Map.of(
                  "broker-0",
                  new BrokerEndpoint("broker-0", "127.0.0.1", 26501, localRaftPort),
                  "broker-1",
                  new BrokerEndpoint("broker-1", "127.0.0.1", 31001, remoteRaftPort)),
              Instant.EPOCH);

      final var descriptor = groups.localPartitionDescriptors(topology).getFirst();
      assertEquals(
          remoteRaftPort,
          descriptor.peers().stream()
              .filter(peer -> peer.brokerId().equals("broker-1"))
              .findFirst()
              .orElseThrow()
              .port());
    } finally {
      scheduler.stop().get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void incompleteTopologyDoesNotShrinkVotersOrReplaceRunningGroup(@TempDir final Path tempDir)
      throws Exception {
    final int raftPort = freePort();
    final ActorScheduler scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    try (JRaftServer server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
        BrokerPartitionManager groups =
            new BrokerPartitionManager(
                managerConfig("broker-0", raftPort, tempDir, Duration.ofSeconds(5)),
                server,
                scheduler,
                Metrics.globalRegistry)) {
      final ClusterTopology complete =
          topology(1, List.of("broker-0", "broker-1"), List.of(), raftPort);
      groups.reconcile(complete);
      final var group = groups.group(1).orElseThrow();
      final var membership = group.raftPartition().members();
      final ClusterTopology incomplete =
          new ClusterTopology(
              "demo",
              2,
              1,
              2,
              complete.partitions(),
              Map.of("broker-0", complete.brokers().get("broker-0")),
              Instant.EPOCH);

      final IllegalStateException error =
          assertThrows(
              IllegalStateException.class, () -> groups.localPartitionDescriptors(incomplete));
      assertTrue(error.getMessage().contains("broker-1"));
      assertTrue(error.getMessage().contains("partition 1"));
      groups.reconcile(incomplete);
      assertEquals(complete, groups.topology());
      assertEquals(group, groups.group(1).orElseThrow());
      assertEquals(membership, group.raftPartition().members());

      groups.reconcile(topology(3, List.of("broker-0", "broker-1"), List.of(), raftPort));
      assertEquals(3, groups.topology().version());
    } finally {
      scheduler.stop().get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void appliesConfiguredElectionTimeoutToPartition(@TempDir final Path tempDir) throws Exception {
    final int raftPort = freePort();
    final ActorScheduler scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    try (JRaftServer server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
        BrokerPartitionManager groups =
            new BrokerPartitionManager(
                managerConfig("broker-0", raftPort, tempDir, Duration.ofSeconds(7)),
                server,
                scheduler,
                Metrics.globalRegistry)) {
      groups.reconcile(topology(1, List.of("broker-0"), List.of(), raftPort));

      final JRaftPartition partition =
          assertInstanceOf(JRaftPartition.class, groups.partition(1).orElseThrow());
      assertEquals(7_000, partition.node().getOptions().getElectionTimeoutMs());
    } finally {
      scheduler.stop().get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void staleTopologyDoesNotRemoveJoiningLearner(@TempDir final Path tempDir) throws Exception {
    final int raftPort = freePort();
    final ActorScheduler scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    try (JRaftServer server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
        BrokerPartitionManager groups =
            new BrokerPartitionManager(
                managerConfig("broker-1", raftPort, tempDir, Duration.ofSeconds(5)),
                server,
                scheduler,
                Metrics.globalRegistry)) {
      final ClusterTopology assigned =
          topology(1, List.of("broker-0"), List.of("broker-1"), 27502, raftPort);
      final var descriptor = groups.localPartitionDescriptors(assigned).getFirst();
      groups.bootstrapPartition(assigned, descriptor);

      groups.reconcile(topology(2, List.of("broker-0"), List.of(), 27502, raftPort));

      assertEquals(1, groups.groupCount());
      assertTrue(groups.isAwaitingAssignment(1));
      assertTrue(groups.isLocalLearner(1));
    } finally {
      scheduler.stop().get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void failedAddWithoutPartitionStorageCompletesWithOriginalFailure(@TempDir final Path tempDir)
      throws Exception {
    final int raftPort = freePort();
    final ActorScheduler scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    try (JRaftServer server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
        BrokerPartitionManager groups =
            new BrokerPartitionManager(
                managerConfig("broker-0", raftPort, tempDir, Duration.ofSeconds(5)),
                server,
                scheduler,
                Metrics.globalRegistry)) {
      final Actor actor = Actor.newActor().name("partition-change-test").build();
      scheduler.submitActor(actor).get(10, TimeUnit.SECONDS);
      final JRaftBrokerPartitionChangeExecutor executor =
          new JRaftBrokerPartitionChangeExecutor(groups, actor);
      final BrokerPartitionOperation request =
          new BrokerPartitionOperation(
              new BrokerPartitionOperation.Partition("demo", 2),
              new BrokerPartitionOperation.Operation(
                  "2:ADD:broker-1:0", TopologyOperationType.ADD, "broker-1", 0),
              new BrokerPartitionOperation.Configuration(
                  List.of(new BrokerPartitionMember("broker-1", true)),
                  new RaftConfigurationReference(0, ""),
                  new RecoveryFence(0, 0, "")));

      final ExecutionException error =
          assertThrows(
              ExecutionException.class, () -> executor.apply(request).get(1, TimeUnit.SECONDS));

      assertInstanceOf(IllegalStateException.class, error.getCause());
    } finally {
      scheduler.stop().get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void removedPartitionDoesNotReuseClosedManifestStore(@TempDir final Path tempDir)
      throws Exception {
    final int raftPort = freePort();
    final ActorScheduler scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    try (JRaftServer server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
        BrokerPartitionManager groups =
            new BrokerPartitionManager(
                managerConfig("broker-0", raftPort, tempDir, Duration.ofSeconds(5)),
                server,
                scheduler,
                Metrics.globalRegistry)) {
      groups.reconcile(topology(1, List.of("broker-0"), List.of(), raftPort));
      final Actor actor = Actor.newActor().name("partition-change-test").build();
      scheduler.submitActor(actor).get(10, TimeUnit.SECONDS);
      final JRaftBrokerPartitionChangeExecutor executor =
          new JRaftBrokerPartitionChangeExecutor(groups, actor);
      assertTrue(executor.latestOperation(1).isEmpty());

      groups.reconcile(topology(2, List.of("broker-1"), List.of(), raftPort));

      assertTrue(executor.latestOperation(1).isEmpty());
    } finally {
      scheduler.stop().get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void resetPeersRejectsNonSurvivorBroker(@TempDir final Path tempDir) throws Exception {
    final int raftPort = freePort();
    final ActorScheduler scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    try (JRaftServer server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
        BrokerPartitionManager groups =
            new BrokerPartitionManager(
                managerConfig("broker-0", raftPort, tempDir, Duration.ofSeconds(5)),
                server,
                scheduler,
                Metrics.globalRegistry)) {
      groups.reconcile(topology(1, List.of("broker-0"), List.of(), raftPort));
      // The reset targets broker-1, but this node is broker-0 — Q4 requires the local node to be
      // the
      // named survivor, so the reset must be refused rather than resetting a group it does not own.
      final BrokerPartitionOperation request =
          new BrokerPartitionOperation(
              new BrokerPartitionOperation.Partition("demo", 1),
              new BrokerPartitionOperation.Operation(
                  "1:RESET_PEERS:broker-1:5", TopologyOperationType.RESET_PEERS, "broker-1", 5),
              new BrokerPartitionOperation.Configuration(
                  List.of(new BrokerPartitionMember("broker-1", true)),
                  new RaftConfigurationReference(0L, ""),
                  new RecoveryFence(0L, 5L, "1:RESET_PEERS:broker-1:5")));

      final ExecutionException error =
          assertThrows(
              ExecutionException.class,
              () ->
                  new BrokerPartitionMembershipManager(groups)
                      .resetPeers(request)
                      .get(1, TimeUnit.SECONDS));

      assertInstanceOf(IllegalStateException.class, error.getCause());
      assertTrue(error.getCause().getMessage().contains("survivor"));
    } finally {
      scheduler.stop().get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void resetPeersRejectsMissingFencingProof(@TempDir final Path tempDir) throws Exception {
    final int raftPort = freePort();
    final ActorScheduler scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    try (JRaftServer server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
        BrokerPartitionManager groups =
            new BrokerPartitionManager(
                managerConfig("broker-0", raftPort, tempDir, Duration.ofSeconds(5)),
                server,
                scheduler,
                Metrics.globalRegistry)) {
      groups.reconcile(topology(1, List.of("broker-0"), List.of(), raftPort));
      final BrokerPartitionOperation request =
          new BrokerPartitionOperation(
              new BrokerPartitionOperation.Partition("demo", 1),
              new BrokerPartitionOperation.Operation(
                  "1:RESET_PEERS:broker-0:5", TopologyOperationType.RESET_PEERS, "broker-0", 5),
              new BrokerPartitionOperation.Configuration(
                  List.of(new BrokerPartitionMember("broker-0", true)),
                  new RaftConfigurationReference(0L, ""),
                  new RecoveryFence(0L, 5L, "")));

      final ExecutionException error =
          assertThrows(
              ExecutionException.class,
              () ->
                  new BrokerPartitionMembershipManager(groups)
                      .resetPeers(request)
                      .get(1, TimeUnit.SECONDS));

      assertInstanceOf(IllegalStateException.class, error.getCause());
      assertTrue(error.getCause().getMessage().contains("fencing proof"));
    } finally {
      scheduler.stop().get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void resetPeersIsIdempotentAtOrBelowAppliedEpoch(@TempDir final Path tempDir) throws Exception {
    final int raftPort = freePort();
    final ActorScheduler scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    try (JRaftServer server = new JRaftServer(new Endpoint("127.0.0.1", raftPort));
        BrokerPartitionManager groups =
            new BrokerPartitionManager(
                managerConfig("broker-0", raftPort, tempDir, Duration.ofSeconds(5)),
                server,
                scheduler,
                Metrics.globalRegistry)) {
      // Single-node group so the survivor really can reset and reach the epoch store.
      groups.reconcile(topology(1, List.of("broker-0"), List.of(), raftPort));

      final BrokerPartitionOperation reset =
          new BrokerPartitionOperation(
              new BrokerPartitionOperation.Partition("demo", 1),
              new BrokerPartitionOperation.Operation(
                  "1:RESET_PEERS:broker-0:5", TopologyOperationType.RESET_PEERS, "broker-0", 5),
              new BrokerPartitionOperation.Configuration(
                  List.of(new BrokerPartitionMember("broker-0", true)),
                  new RaftConfigurationReference(
                      0,
                      groups.group(1).orElseThrow().raftPartition().appliedConfigurationDigest()),
                  new RecoveryFence(0, 5, "1:RESET_PEERS:broker-0:5")));
      final BrokerPartitionMembershipManager membership =
          new BrokerPartitionMembershipManager(groups);
      membership.resetPeers(reset).get(5, TimeUnit.SECONDS);

      // A replay at the same (or lower) epoch is a no-op that completes without touching
      // membership.
      membership.resetPeers(reset).get(5, TimeUnit.SECONDS);
    } finally {
      scheduler.stop().get(10, TimeUnit.SECONDS);
    }
  }

  private static int freePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static void await(final BooleanSupplier condition) throws Exception {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(20);
    assertTrue(condition.getAsBoolean(), "condition did not converge within 10s");
  }

  private static BrokerPartitionManagerConfig managerConfig(
      final String brokerId,
      final int raftPort,
      final Path dataDirectory,
      final Duration electionTimeout) {
    final int commandPort = raftPort == 26501 ? 26502 : 26501;
    return new BrokerPartitionManagerConfig(
        0,
        brokerId,
        new BrokerConfig.Node(
            new BrokerConfig.Network("127.0.0.1", commandPort, raftPort), dataDirectory),
        new BrokerConfig.Engine(
            new BrokerConfig.Snapshot(
                BrokerConfig.DEFAULT_SNAPSHOT_INTERVAL,
                BrokerConfig.DEFAULT_SNAPSHOT_LOG_SIZE_THRESHOLD_BYTES),
            new BrokerConfig.Consensus(electionTimeout),
            BrokerConfig.defaultProcessing()),
        true);
  }

  private static ClusterTopology topology(
      final long version,
      final List<String> voters,
      final List<String> learners,
      final int broker0RaftPort) {
    return topology(version, voters, learners, broker0RaftPort, 27502);
  }

  private static ClusterTopology topology(
      final long version,
      final List<String> voters,
      final List<String> learners,
      final int broker0RaftPort,
      final int broker1RaftPort) {
    return new ClusterTopology(
        "demo",
        version,
        1,
        2,
        List.of(
            new Partition(
                1,
                new Partition.Leadership(voters.isEmpty() ? "" : voters.getFirst(), 1),
                new Partition.Membership(voters, learners, Map.of(), 0),
                PartitionStatus.ACTIVE)),
        Map.of(
            "broker-0", new BrokerEndpoint("broker-0", "127.0.0.1", 26501, broker0RaftPort),
            "broker-1", new BrokerEndpoint("broker-1", "127.0.0.1", 26502, broker1RaftPort)),
        Instant.EPOCH);
  }
}
