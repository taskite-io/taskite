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

import static org.junit.jupiter.api.Assertions.*;

import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.util.Endpoint;
import io.micrometer.core.instrument.Metrics;
import io.taskite.broker.BrokerConfig;
import io.taskite.broker.state.BrokerPartitionOperationManifestStore;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.JRaftPartition;
import io.taskite.cluster.raft.jraft.membership.JRaftPeerMapper;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
import io.taskite.protocol.record.TopologyOperationType;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.scheduler.Actor;
import io.taskite.scheduler.ActorScheduler;
import io.taskite.topology.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Reopens real Raft storage after promotion's removeLearners stage has committed. */
final class BrokerPromotionResumeIntegrationTest {
  private static final Set<Integer> ALLOCATED_PORTS = new HashSet<>();

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void recoveredLearnerOutsideTargetUsesOnlyKnownBrokerEndpoints(
      final boolean learnerAddressKnown, @TempDir final Path directory) throws Exception {
    final int p0 = port();
    final int p1 = port();
    final var broker0 = new BrokerEndpoint("b0", "127.0.0.1", port(), p0);
    final var broker1 = new BrokerEndpoint("b1", "127.0.0.1", port(), p1);
    final var initial =
        new ClusterTopology(
            "resume",
            1,
            1,
            1,
            List.of(
                new Partition(
                    1,
                    new Partition.Leadership("b0", 1),
                    new Partition.Membership(
                        List.of("b0"), List.of("b1"), Map.of("b0", 10, "b1", 0), 0),
                    PartitionStatus.REBALANCING)),
            Map.of("b0", broker0, "b1", broker1),
            Instant.EPOCH);
    final var target =
        new ClusterTopology(
            "resume",
            2,
            1,
            1,
            List.of(
                new Partition(
                    1,
                    new Partition.Leadership("b0", 1),
                    new Partition.Membership(List.of("b0"), List.of(), Map.of("b0", 10), 0),
                    PartitionStatus.REBALANCING)),
            learnerAddressKnown ? initial.brokers() : Map.of("b0", broker0),
            Instant.EPOCH);
    final var scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    try (var server1 = new JRaftServer(new Endpoint("127.0.0.1", p1));
        var groups1 = manager("b1", p1, directory, server1, scheduler)) {
      String digest;
      String configuration;
      long previousTerm;
      try (var server0 = new JRaftServer(new Endpoint("127.0.0.1", p0));
          var groups0 = manager("b0", p0, directory, server0, scheduler)) {
        groups0.reconcile(initial);
        groups1.reconcile(initial);
        final var group0 = groups0.group(1).orElseThrow();
        final var group1 = groups1.group(1).orElseThrow();
        group0.startupFuture().get(10, TimeUnit.SECONDS);
        group1.startupFuture().get(10, TimeUnit.SECONDS);
        await(() -> group0.role() == RaftRole.LEADER && group1.lastIndex() == group0.lastIndex());
        digest = group0.raftPartition().appliedConfigurationDigest();
        configuration =
            ((NodeImpl) ((JRaftPartition) group0.raftPartition()).node())
                .getCurrentConf()
                .toString();
        previousTerm = group0.term();
      }
      try (var server0 = new JRaftServer(new Endpoint("127.0.0.1", p0));
          var groups0 = manager("b0", p0, directory, server0, scheduler)) {
        // Retain the original configuration on disk while PD's target omits its learner.
        groups0.reconcile(target);
        final var recovered = groups0.group(1).orElseThrow();
        recovered.startupFuture().get(10, TimeUnit.SECONDS);
        await(() -> recovered.role() == RaftRole.LEADER && recovered.term() > previousTerm);
        assertEquals(
            configuration,
            ((NodeImpl) ((JRaftPartition) recovered.raftPartition()).node())
                .getCurrentConf()
                .toString());
        if (learnerAddressKnown) {
          assertEquals(digest, recovered.raftPartition().appliedConfigurationDigest());
        }
        assertEquals(2, recovered.raftPartition().members().size());
        final String learnerIdentity =
            JRaftPeerMapper.logical("resume")
                .peerId(new RaftPeer("b1", "127.0.0.1", p1, false, 0))
                .toString();
        assertEquals(
            learnerAddressKnown ? "b1" : learnerIdentity,
            recovered.raftPartition().members().stream()
                .filter(member -> member.role() == RaftRole.LEARNER)
                .findFirst()
                .orElseThrow()
                .memberId());
        if (!learnerAddressKnown) {
          groups0.reconcile(
              new ClusterTopology(
                  "resume", 3, 1, 1, target.partitions(), initial.brokers(), Instant.EPOCH));
        }
        final var learner = groups1.group(1).orElseThrow();
        await(
            () ->
                learner.term() == recovered.term() && learner.lastIndex() == recovered.lastIndex());
        assertEquals(digest, recovered.raftPartition().appliedConfigurationDigest());
        assertEquals(
            "b1",
            recovered.raftPartition().members().stream()
                .filter(member -> member.role() == RaftRole.LEARNER)
                .findFirst()
                .orElseThrow()
                .memberId());
      }
    } finally {
      scheduler.stop().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void pinnedPromotionResumesAfterCommittedIntermediateConfiguration(@TempDir final Path directory)
      throws Exception {
    final int p0 = port();
    final int p1 = port();
    final var topology =
        new ClusterTopology(
            "resume",
            1,
            1,
            2,
            List.of(
                new Partition(
                    1,
                    new Partition.Leadership("b0", 1),
                    new Partition.Membership(
                        List.of("b0"), List.of("b1"), Map.of("b0", 10, "b1", 0), 0),
                    PartitionStatus.REBALANCING)),
            Map.of(
                "b0",
                new BrokerEndpoint("b0", "127.0.0.1", port(), p0),
                "b1",
                new BrokerEndpoint("b1", "127.0.0.1", port(), p1)),
            Instant.EPOCH);
    final var retained = List.of(new RaftPeer("b0", "127.0.0.1", p0, true, 10));
    final String intermediate =
        RaftConfigurationDigest.ofBroker(
            topology.clusterId(), List.of(new BrokerPartitionMember("b0", true, 10)));
    final var scheduler = ActorScheduler.newActorScheduler().build();
    scheduler.start();
    BrokerPartitionOperation request;
    try {
      try (var server = new JRaftServer(new Endpoint("127.0.0.1", p0));
          var groups = manager("b0", p0, directory, server, scheduler)) {
        groups.reconcile(topology);
        final var group = groups.group(1).orElseThrow();
        group.startupFuture().get(5, TimeUnit.SECONDS);
        request =
            new BrokerPartitionOperation(
                new BrokerPartitionOperation.Partition("resume", 1),
                new BrokerPartitionOperation.Operation(
                    "promotion-after-remove", TopologyOperationType.PROMOTE_LEARNER, "b1", 10),
                new BrokerPartitionOperation.Configuration(
                    List.of(
                        new BrokerPartitionMember("b0", true, 10),
                        new BrokerPartitionMember("b1", true, 10)),
                    new RaftConfigurationReference(
                        0, group.raftPartition().appliedConfigurationDigest()),
                    new RecoveryFence(0, 0, "")));
        final var store =
            new BrokerPartitionOperationManifestStore(group.raftPartition().storage());
        store.authorizeConfigurations(
            request,
            List.of(
                request.expectedConfigurationDigest(),
                intermediate,
                request.targetConfigurationDigest()));
        store.put(
            new BrokerPartitionOperationManifest(
                new BrokerPartitionOperationManifest.Identity(1, request.opId()),
                new BrokerPartitionOperationManifest.Result(
                    BrokerPartitionOperationStatus.IN_PROGRESS, 0, ""),
                BrokerPartitionOperationManifest.Evidence.UNKNOWN));
        // Stop at a real durable membership boundary, before changePeers installs the voter.
        group.updateRaftMembership(retained).get(15, TimeUnit.SECONDS);
        await(() -> intermediate.equals(group.raftPartition().appliedConfigurationDigest()));
      }
      try (var server0 = new JRaftServer(new Endpoint("127.0.0.1", p0));
          var server1 = new JRaftServer(new Endpoint("127.0.0.1", p1));
          var groups0 = manager("b0", p0, directory, server0, scheduler);
          var groups1 = manager("b1", p1, directory, server1, scheduler)) {
        groups0.reconcile(topology);
        groups1.reconcile(topology);
        final var group = groups0.group(1).orElseThrow();
        group.startupFuture().get(5, TimeUnit.SECONDS);
        assertEquals(intermediate, group.raftPartition().appliedConfigurationDigest());
        assertNotEquals(request.expectedConfigurationDigest(), intermediate);
        final var actor = Actor.newActor().name("resumed-promotion").build();
        scheduler.submitActor(actor).get(5, TimeUnit.SECONDS);
        final var executor = new JRaftBrokerPartitionChangeExecutor(groups0, actor);
        executor.apply(request).get(20, TimeUnit.SECONDS);
        groups1.group(1).orElseThrow().startupFuture().get(5, TimeUnit.SECONDS);
        assertEquals(
            BrokerPartitionOperationStatus.APPLIED,
            executor.latestOperation(1).orElseThrow().status());
        assertEquals(
            request.targetConfigurationDigest(),
            group.raftPartition().appliedConfigurationDigest());
        executor.apply(request).get(5, TimeUnit.SECONDS);
        actor.closeAsync().get(5, TimeUnit.SECONDS);
      }
    } finally {
      scheduler.stop().get(5, TimeUnit.SECONDS);
    }
  }

  private static BrokerPartitionManager manager(
      final String id,
      final int port,
      final Path directory,
      final JRaftServer server,
      final ActorScheduler scheduler)
      throws Exception {
    return new BrokerPartitionManager(
        new BrokerPartitionManagerConfig(
            0,
            id,
            new BrokerConfig.Node(new BrokerConfig.Network("127.0.0.1", port(), port), directory),
            new BrokerConfig.Engine(
                new BrokerConfig.Snapshot(Duration.ofMinutes(15), 128L * 1024 * 1024),
                new BrokerConfig.Consensus(Duration.ofMillis(500)),
                BrokerConfig.defaultProcessing()),
            true),
        server,
        scheduler,
        Metrics.globalRegistry);
  }

  private static synchronized int port() throws Exception {
    while (true) {
      // Raft binds IPv4; wildcard IPv6 allocation can select an occupied IPv4 port.
      try (var socket = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"))) {
        if (ALLOCATED_PORTS.add(socket.getLocalPort())) {
          return socket.getLocalPort();
        }
      }
    }
  }

  private static void await(final BooleanSupplier condition) throws Exception {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(20);
    assertTrue(condition.getAsBoolean(), "configuration did not apply within 10s");
  }
}
