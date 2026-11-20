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
package io.taskite.gateway;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.grpc.Status;
import io.taskite.broker.client.BrokerClient;
import io.taskite.broker.client.request.BrokerCompleteJobRequest;
import io.taskite.broker.client.request.BrokerCreateJobRequest;
import io.taskite.broker.client.request.BrokerRequest;
import io.taskite.broker.client.response.BrokerRequestRejectedBeforeAppendException;
import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.gateway.broker.GatewayBrokerCommandDispatcher;
import io.taskite.gateway.routing.GatewayBrokerRouter;
import io.taskite.gateway.routing.GatewayTopologyCache;
import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.ClusterTopology;
import io.taskite.topology.Partition;
import io.taskite.topology.PartitionStatus;
import io.taskite.util.BufferUtil;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

final class GatewayBrokerCommandDispatcherTest {
  @Test
  void automaticRoutingRotatesPartitionsAndUsesExpandedTopology() {
    final var cache = new GatewayTopologyCache();
    cache.update(withAlternativeReadyPartition(topology("broker-a", 26501, 30611, 1)));
    final var first = new GatewayBrokerRouter(cache);
    final var request = new BrokerCreateJobRequest("", 0, new JobRecord().setTenantId("tenant-a"));
    for (int index = 0; index < 20; index++) {
      assertEquals(1 + index % 2, first.resolve("cluster-a", request).partitionId());
    }
    cache.update(withAlternativeReadyPartition(topology("broker-b", 26502, 30622, 2)));
    final var route = first.resolve("cluster-a", request);
    assertEquals(1, route.partitionId());
    assertEquals(26502, route.endpoint().port());
    final var expanded = withAlternativeReadyPartition(topology("broker-c", 26503, 30633, 3));
    final var partition = expanded.partitions().getFirst();
    cache.update(
        new ClusterTopology(
            expanded.clusterId(),
            3,
            3,
            1,
            List.of(
                partition,
                expanded.partitions().get(1),
                new Partition(
                    3,
                    partition.leadership(),
                    partition.membership(),
                    PartitionStatus.ACTIVE,
                    true)),
            expanded.brokers(),
            Instant.EPOCH));
    final var restartedCache = new GatewayTopologyCache();
    restartedCache.update(cache.current("cluster-a"));
    final var restartedRouter = new GatewayBrokerRouter(restartedCache);
    for (int index = 0; index < 6; index++) {
      assertEquals(1 + index % 3, first.resolve("cluster-a", request).partitionId());
      assertEquals(1 + index % 3, restartedRouter.resolve("cluster-a", request).partitionId());
    }
  }

  @Test
  void automaticRoutingSkipsUnavailablePartitions() {
    final var cache = new GatewayTopologyCache();
    final var topology = withAlternativeReadyPartition(topology("broker-a", 26501, 30611, 1));
    cache.update(
        new ClusterTopology(
            topology.clusterId(),
            topology.version(),
            2,
            1,
            topology.partitions().stream()
                .map(
                    p ->
                        new Partition(
                            p.partitionId(),
                            p.leadership(),
                            p.membership(),
                            p.status(),
                            p.partitionId() == 2))
                .toList(),
            topology.brokers(),
            Instant.EPOCH));
    final var request = new BrokerCreateJobRequest("", 0, new JobRecord().setTenantId("tenant-a"));
    final var router = new GatewayBrokerRouter(cache);
    for (int index = 0; index < 6; index++) {
      assertEquals(2, router.resolve("cluster-a", request).partitionId());
    }
  }

  @Test
  void preAppendRejectionWaitsForFreshLeaderAndKeepsPartition() {
    final var cache = new GatewayTopologyCache();
    cache.update(withAlternativeReadyPartition(topology("broker-a", 26501, 30611, 1)));
    final var client = new CountingClient();
    client.failure = new BrokerRequestRejectedBeforeAppendException();
    final var control = new ManualConcurrencyControl();
    final var request = new BrokerCreateJobRequest("", 0, new JobRecord().setJobType("charge"));
    final var result =
        new GatewayBrokerCommandDispatcher(client, new GatewayBrokerRouter(cache))
            .dispatchWhenReady("cluster-a", request, () -> false, control, Duration.ofMillis(200));
    assertFalse(result.isDone());
    assertEquals(1, client.calls);
    final int selectedPartition = client.partitions.getFirst();
    assertTrue(List.of(1, 2).contains(selectedPartition));
    client.failure = null;
    cache.update(withAlternativeReadyPartition(topology("broker-b", 26502, 30622, 2)));
    control.advance(50);
    assertNotNull(result.join());
    assertEquals(List.of(26501, 26502), client.ports);
    assertEquals(List.of(selectedPartition, selectedPartition), client.partitions);
    assertEquals(2, request.getRoutingEpoch());
    control.advance(500);
    assertEquals(2, client.calls);
    assertEquals(0, control.pendingTimers());
  }

  @Test
  void repeatedPreAppendRejectionStopsAtBudgetOrCancellation() {
    for (final boolean cancellation : List.of(false, true)) {
      final var cache = new GatewayTopologyCache();
      cache.update(withReadiness(true));
      final var client = new CountingClient();
      client.failure = new BrokerRequestRejectedBeforeAppendException();
      final var control = new ManualConcurrencyControl();
      final var cancelled = new AtomicBoolean();
      final var result =
          new GatewayBrokerCommandDispatcher(client, new GatewayBrokerRouter(cache))
              .dispatchWhenReady(
                  "cluster-a",
                  new BrokerCreateJobRequest("", 0, new JobRecord().setJobType("charge")),
                  cancelled::get,
                  control,
                  Duration.ofMillis(125));
      assertFalse(result.isDone());
      cancelled.set(cancellation);
      control.advance(500);
      assertThrows(RuntimeException.class, result::join);
      final int attempts = client.calls;
      assertEquals(cancellation ? 1 : 3, attempts);
      control.advance(500);
      assertEquals(attempts, client.calls);
      assertEquals(0, control.pendingTimers());
    }
  }

  @Test
  void exhaustedRoutingBudgetNeverSendsEvenToReadyPartition() {
    for (final Duration timeout : List.of(Duration.ZERO, Duration.ofMillis(-1))) {
      final var cache = new GatewayTopologyCache();
      cache.update(withReadiness(true));
      final var client = new CountingClient();
      final var control = new ManualConcurrencyControl();
      final var result =
          new GatewayBrokerCommandDispatcher(client, new GatewayBrokerRouter(cache))
              .dispatchWhenReady(
                  "cluster-a",
                  new BrokerCreateJobRequest("", 0, new JobRecord().setJobType("charge")),
                  () -> false,
                  control,
                  timeout);
      assertThrows(RuntimeException.class, result::join);
      assertEquals(0, client.calls);
    }
  }

  @Test
  void refreshesEndpointAndRoutingEpochForEveryRetryAttempt() {
    final GatewayTopologyCache cache = new GatewayTopologyCache();
    cache.update(topology("broker-a", 26501, 30611, 1));
    final RefreshingBrokerClient brokerClient = new RefreshingBrokerClient(cache);
    final GatewayBrokerCommandDispatcher dispatcher =
        new GatewayBrokerCommandDispatcher(brokerClient, new GatewayBrokerRouter(cache));
    final BrokerRequest<?> request =
        new BrokerCreateJobRequest("", 0, new JobRecord().setJobType("charge"));
    request.setTenantId("<default>");

    dispatcher.dispatch("cluster-a", request).join();

    assertEquals(List.of(26501, 26502), brokerClient.ports);
    assertEquals(List.of(1L, 2L), brokerClient.routingEpochs);
    assertEquals(List.of(1, 1), brokerClient.partitionIds);
  }

  @Test
  void waitsBeforeSendThenDispatchesExactlyOnceWhenServiceBecomesReady() {
    final var cache = new GatewayTopologyCache();
    cache.update(withReadiness(false));
    final var client = new CountingClient();
    final var control = new ManualConcurrencyControl();
    final var dispatcher =
        new GatewayBrokerCommandDispatcher(client, new GatewayBrokerRouter(cache));
    final var request = new BrokerCreateJobRequest("", 0, new JobRecord().setJobType("charge"));
    final var result =
        dispatcher.dispatchWhenReady(
            "cluster-a", request, () -> false, control, Duration.ofMillis(200));
    assertEquals(0, client.calls);
    assertFalse(result.isDone());
    cache.update(withReadiness(true));
    control.advance(50);
    assertNotNull(result.join());
    assertEquals(1, client.calls);
    control.advance(500);
    assertEquals(1, client.calls);
    assertEquals(0, control.pendingTimers());
  }

  @Test
  void readinessWaitExpiresWithoutSending() {
    final var cache = new GatewayTopologyCache();
    cache.update(withReadiness(false));
    final var client = new CountingClient();
    final var control = new ManualConcurrencyControl();
    final var result =
        new GatewayBrokerCommandDispatcher(client, new GatewayBrokerRouter(cache))
            .dispatchWhenReady(
                "cluster-a",
                new BrokerCreateJobRequest("", 0, new JobRecord().setJobType("charge")),
                () -> false,
                control,
                Duration.ofMillis(25));
    control.advance(25);
    assertThrows(Exception.class, result::join);
    assertEquals(0, client.calls);
    assertEquals(0, control.pendingTimers());
  }

  @Test
  void unknownTransportResultNeverReentersRoutingWait() {
    final var cache = new GatewayTopologyCache();
    cache.update(withReadiness(true));
    final var client = new CountingClient();
    client.failure = Status.UNAVAILABLE.asRuntimeException();
    final var control = new ManualConcurrencyControl();
    final var result =
        new GatewayBrokerCommandDispatcher(client, new GatewayBrokerRouter(cache))
            .dispatchWhenReady(
                "cluster-a",
                new BrokerCreateJobRequest("", 0, new JobRecord().setJobType("charge")),
                () -> false,
                control,
                Duration.ofMillis(200));
    assertThrows(Exception.class, result::join);
    control.advance(500);
    assertEquals(1, client.calls);
    assertEquals(0, control.pendingTimers());
  }

  @Test
  void cancellationWhileWaitingDoesNotSend() {
    final var cache = new GatewayTopologyCache();
    cache.update(withReadiness(false));
    final var client = new CountingClient();
    final var control = new ManualConcurrencyControl();
    final var cancelled = new AtomicBoolean();
    final var result =
        new GatewayBrokerCommandDispatcher(client, new GatewayBrokerRouter(cache))
            .dispatchWhenReady(
                "cluster-a",
                new BrokerCreateJobRequest("", 0, new JobRecord().setJobType("charge")),
                cancelled::get,
                control,
                Duration.ofMillis(200));
    cancelled.set(true);
    control.advance(50);
    assertThrows(Exception.class, result::join);
    assertEquals(0, client.calls);
    assertEquals(0, control.pendingTimers());
  }

  @Test
  void fixedPartitionWaitDoesNotMoveExistingJobToAnotherReadyPartition() {
    final var cache = new GatewayTopologyCache();
    final var t = withReadiness(false);
    final var p = t.partitions().getFirst();
    cache.update(
        new ClusterTopology(
            "cluster-a",
            1,
            2,
            1,
            List.of(
                p, new Partition(2, p.leadership(), p.membership(), PartitionStatus.ACTIVE, true)),
            t.brokers(),
            Instant.EPOCH));
    final var client = new CountingClient();
    final var control = new ManualConcurrencyControl();
    final var request =
        new BrokerCompleteJobRequest(Protocol.encodePartitionId(1, 1), BufferUtil.wrapString("{}"));
    request.setPartitionId(1);
    final var result =
        new GatewayBrokerCommandDispatcher(client, new GatewayBrokerRouter(cache))
            .dispatchWhenReady("cluster-a", request, () -> false, control, Duration.ofMillis(200));
    control.advance(50);
    assertEquals(0, client.calls);
    cache.update(withReadiness(true));
    control.advance(50);
    assertNotNull(result.join());
    assertEquals(1, client.partitionId);
    assertEquals(1, client.calls);
  }

  private static ClusterTopology withReadiness(boolean ready) {
    final var t = topology("broker-a", 26501, 30611, 1);
    final var p = t.partitions().getFirst();
    return new ClusterTopology(
        t.clusterId(),
        t.version(),
        1,
        1,
        List.of(
            new Partition(1, p.leadership(), p.membership(), PartitionStatus.REBALANCING, ready)),
        t.brokers(),
        Instant.EPOCH);
  }

  private static ClusterTopology withAlternativeReadyPartition(final ClusterTopology topology) {
    final var partition = topology.partitions().getFirst();
    return new ClusterTopology(
        topology.clusterId(),
        topology.version(),
        2,
        1,
        List.of(
            partition,
            new Partition(
                2, partition.leadership(), partition.membership(), PartitionStatus.ACTIVE, true)),
        topology.brokers(),
        Instant.EPOCH);
  }

  private static final class CountingClient implements BrokerClient {
    private int calls;
    private int partitionId;
    private Throwable failure;
    private final List<Integer> ports = new ArrayList<>();
    private final List<Integer> partitions = new ArrayList<>();

    @Override
    public <T> ActorFuture<BrokerResponse<T>> sendRequest(
        BrokerEndpoint endpoint, BrokerRequest<T> request) {
      calls++;
      partitionId = request.getPartitionId();
      ports.add(endpoint.port());
      partitions.add(partitionId);
      return failure == null
          ? CompletableActorFuture.completed(
              new BrokerResponse<>(null, request.getPartitionId(), 1))
          : CompletableActorFuture.completedExceptionally(failure);
    }
  }

  private static ClusterTopology topology(
      final String brokerId, final int port, final int raftPort, final long recoveryEpoch) {
    final BrokerEndpoint endpoint = new BrokerEndpoint(brokerId, "127.0.0.1", port, raftPort);
    return new ClusterTopology(
        "cluster-a",
        recoveryEpoch,
        1,
        1,
        List.of(
            new Partition(
                1,
                new Partition.Leadership(brokerId, 1),
                new Partition.Membership(List.of(), List.of(), Map.of(), recoveryEpoch),
                PartitionStatus.ACTIVE)),
        Map.of(brokerId, endpoint),
        Instant.EPOCH);
  }

  private static final class RefreshingBrokerClient implements BrokerClient {
    private final GatewayTopologyCache cache;
    private final List<Integer> ports = new ArrayList<>();
    private final List<Long> routingEpochs = new ArrayList<>();
    private final List<Integer> partitionIds = new ArrayList<>();

    private RefreshingBrokerClient(final GatewayTopologyCache cache) {
      this.cache = cache;
    }

    @Override
    public <T> ActorFuture<BrokerResponse<T>> sendRequest(
        final BrokerEndpoint endpoint, final BrokerRequest<T> request) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> ActorFuture<BrokerResponse<T>> sendRequest(
        final Supplier<BrokerEndpoint> endpointSupplier, final BrokerRequest<T> request) {
      ports.add(endpointSupplier.get().port());
      routingEpochs.add(request.getRoutingEpoch());
      partitionIds.add(request.getPartitionId());
      cache.update(topology("broker-b", 26502, 30622, 2));
      ports.add(endpointSupplier.get().port());
      routingEpochs.add(request.getRoutingEpoch());
      partitionIds.add(request.getPartitionId());
      return CompletableActorFuture.completed(
          new BrokerResponse<>(null, request.getPartitionId(), 1));
    }
  }
}
