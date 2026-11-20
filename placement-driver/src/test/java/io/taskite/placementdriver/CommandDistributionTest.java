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
package io.taskite.placementdriver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.taskite.broker.client.BrokerClient;
import io.taskite.broker.client.request.BrokerCommandDistributionRequest;
import io.taskite.broker.client.request.BrokerRequest;
import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.placementdriver.broker.distribution.BrokerDistributionRoute;
import io.taskite.placementdriver.broker.distribution.BrokerDistributionRouteResolver;
import io.taskite.placementdriver.broker.distribution.BrokerJobSubscriptionTransport;
import io.taskite.placementdriver.broker.distribution.GrpcBrokerJobSubscriptionTransport;
import io.taskite.placementdriver.processing.broker.distribution.BrokerDistributionScheduler;
import io.taskite.placementdriver.processing.broker.distribution.CommandDistributionAcknowledgeProcessor;
import io.taskite.placementdriver.processing.broker.distribution.CommandDistributionDispatchProcessor;
import io.taskite.placementdriver.processing.broker.distribution.CommandDistributionRetryProcessor;
import io.taskite.placementdriver.state.appliers.CommandDistributionAcknowledgedApplier;
import io.taskite.placementdriver.state.appliers.CommandDistributionCompletedApplier;
import io.taskite.placementdriver.state.appliers.PlacementDriverEventAppliers;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService.CommandWriter;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.StateWriter;
import io.taskite.streamplatform.api.writer.TypedRejectionWriter;
import io.taskite.streamplatform.api.writer.Writers;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.ClusterTopology;
import io.taskite.topology.Partition;
import io.taskite.topology.PartitionStatus;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies the {@code pd -> Broker} leg of the {@code client -> gateway -> pd -> Broker} deploy
 * flow. Once a deployment (or deletion) command has been distributed inside the PD and marked
 * pending per partition, the durable Scheduler and Processor resolve its leader and push the
 * resource. Controlled RPC responses verify payload fidelity, leader/pending gating and retries.
 */
final class CommandDistributionTest {

  private static final String CLUSTER_ID = "demo";
  private static final long DISTRIBUTION_KEY = 101;
  private final Map<DistributionFixture, WorkflowScheduler> workflows = new HashMap<>();

  @Test
  void singleCommandBudgetDrainsPendingTargetsAndStopsAfterAppliedAcks(
      @TempDir final Path tempDir) {
    try (final var storage = new PartitionStorage(tempDir)) {
      final var state = state(storage);
      seedDeployment(state, deployment("order", "{}", "request-order"), 1, 2, 3);
      final var broker = new RecordingBrokerClient();
      final var dispatcher =
          leaderActor(
              state,
              broker,
              ignored -> route(healthyTopology(3)),
              event ->
                  new CommandDistributionAcknowledgedApplier(state)
                      .applyState(event.key(), (CommandDistributionRecord<?>) event.value()));
      final var scheduling = workflows.get(dispatcher);
      scheduling.capacity = 1;
      for (int round = 1; round <= 3; round++) {
        dispatch(dispatcher);
        assertEquals(round, broker.sent.size());
      }
      dispatch(dispatcher);
      dispatch(dispatcher);
      assertTrue(scheduling.tasks.isEmpty());
      assertTrue(state.getCommandDistributionState().isPlanComplete(DISTRIBUTION_KEY));
    }
  }

  @Test
  void scanningDoesNotSendRpcBeforeTheDispatchCommandCommits(@TempDir final Path tempDir) {
    try (final var storage = new PartitionStorage(tempDir)) {
      final var state = state(storage);
      seedDeployment(state, deployment("order", "{}", "request-order"), 1);
      final var broker = new RecordingBrokerClient();
      final var dispatcher =
          leaderActor(state, broker, ignored -> route(healthyTopology(1)), ignored -> {});
      final var scheduling = workflows.get(dispatcher);
      final var task = scheduling.tasks.removeFirst();
      final var commands = new ArrayList<LoggedEvent>();
      task.action.accept(new RecordingCommandWriter(commands::add));
      assertEquals(CommandDistributionIntent.DISPATCH, commands.getFirst().metadata().getIntent());
      assertTrue(broker.sent.isEmpty());
      final var postCommit = new ArrayList<Runnable>();
      final var writers = mock(Writers.class);
      when(writers.postCommit()).thenReturn(postCommit::add);
      dispatcher.processDispatch(
          (CommandDistributionRecord<?>) commands.getFirst().value(), writers);
      assertTrue(broker.sent.isEmpty());
      postCommit.forEach(Runnable::run);
      assertEquals(1, broker.sent.size());
    }
  }

  @Test
  void acknowledgedTargetsCompletePlanWithoutDispatcherCompletionCommands(
      @TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("complete-targets"))) {
      final MutableProcessingState state = state(storage);
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 1);
      state.getCommandDistributionState().putTarget(DISTRIBUTION_KEY, "backup", 1);
      state.getCommandDistributionState().markPending(DISTRIBUTION_KEY, "backup", 1);
      state.getCommandDistributionState().acknowledge(DISTRIBUTION_KEY, CLUSTER_ID, 1);
      final RecordingBrokerClient broker = new RecordingBrokerClient();
      final List<LoggedEvent> commands = new ArrayList<>();
      final var actor =
          dispatcher(
              state,
              new GrpcBrokerJobSubscriptionTransport(broker),
              () -> true,
              ignored -> route(healthyTopology(1)),
              new RecordingCommandWriter(commands::add));
      dispatch(actor);
      assertEquals(1, broker.sent.size());
      assertEquals(
          CommandDistributionIntent.ACKNOWLEDGE, commands.getFirst().metadata().getIntent());
      assertFalse(state.getCommandDistributionState().isPlanComplete(DISTRIBUTION_KEY));
      new CommandDistributionCompletedApplier()
          .applyState(DISTRIBUTION_KEY, (CommandDistributionRecord<?>) commands.getFirst().value());
      assertFalse(state.getCommandDistributionState().isPlanComplete(DISTRIBUTION_KEY));
      new CommandDistributionAcknowledgedApplier(state)
          .applyState(DISTRIBUTION_KEY, (CommandDistributionRecord<?>) commands.getFirst().value());
      assertTrue(state.getCommandDistributionState().isPlanComplete(DISTRIBUTION_KEY));
      dispatch(actor);
      dispatch(actor);
      assertEquals(1, commands.size());
      assertEquals(1, broker.sent.size());
      assertTrue(state.getCommandDistributionState().isPlanComplete(DISTRIBUTION_KEY));
    }
  }

  @Test
  void recoveredAcknowledgedPlanDoesNotRedistributeOrSubmitCompletion(@TempDir final Path tempDir) {
    final Path path = tempDir.resolve("recover-completion");
    try (PartitionStorage storage = new PartitionStorage(path)) {
      final MutableProcessingState state = state(storage);
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 1);
      state.getCommandDistributionState().acknowledge(DISTRIBUTION_KEY, CLUSTER_ID, 1);
      assertTrue(state.getCommandDistributionState().isPlanComplete(DISTRIBUTION_KEY));
    }
    try (PartitionStorage storage = new PartitionStorage(path)) {
      final MutableProcessingState state = state(storage);
      final RecordingBrokerClient broker = new RecordingBrokerClient();
      final AtomicInteger attempts = new AtomicInteger();
      final var actor =
          dispatcher(
              state,
              new GrpcBrokerJobSubscriptionTransport(broker),
              () -> true,
              ignored -> route(healthyTopology(1)),
              new RecordingCommandWriter(command -> attempts.incrementAndGet()));
      dispatch(actor);
      dispatch(actor);
      assertTrue(state.getCommandDistributionState().isPlanComplete(DISTRIBUTION_KEY));
      dispatch(actor);
      dispatch(actor);
      assertEquals(0, attempts.get());
      assertTrue(broker.sent.isEmpty());
      assertTrue(state.getCommandDistributionState().isPlanComplete(DISTRIBUTION_KEY));
    }
  }

  @Test
  void newGenerationDispatchesWhileOldRequestIsInFlightAndIgnoresStaleCompletion(
      @TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("generation-in-flight"))) {
      final MutableProcessingState state = state(storage);
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 1);
      final var oldResponse = new CompletableActorFuture<BrokerResponse<?>>();
      final var newResponse = new CompletableActorFuture<BrokerResponse<?>>();
      final RecordingBrokerClient broker = new RecordingBrokerClient();
      broker.responseFor = ignored -> broker.sent.size() == 1 ? oldResponse : newResponse;
      final List<LoggedEvent> acks = new ArrayList<>();
      final var actor = leaderActor(state, broker, ignored -> route(healthyTopology(1)), acks::add);
      dispatch(actor);
      final var next =
          state
              .getCommandDistributionState()
              .distribution(DISTRIBUTION_KEY)
              .orElseThrow()
              .setGeneration(1);
      state.getCommandDistributionState().putDistribution(next);
      state.getCommandDistributionState().resetTarget(DISTRIBUTION_KEY, CLUSTER_ID, 1);
      dispatch(actor);
      assertEquals(2, broker.sent.size());
      oldResponse.complete(new BrokerResponse<>(null));
      workflows.get(actor).runReady();
      assertTrue(acks.isEmpty(), "superseded network completion must not enqueue an ACK");
      assertTrue(state.getCommandDistributionState().isPending(DISTRIBUTION_KEY, CLUSTER_ID, 1));
      newResponse.complete(new BrokerResponse<>(null));
      workflows.get(actor).runReady();
      assertEquals(1, ((CommandDistributionRecord<?>) acks.getLast().value()).getGeneration());
      new CommandDistributionAcknowledgedApplier(state)
          .applyState(DISTRIBUTION_KEY, (CommandDistributionRecord<?>) acks.getLast().value());
      assertFalse(state.getCommandDistributionState().isPending(DISTRIBUTION_KEY, CLUSTER_ID, 1));
    }
  }

  @Test
  void distributesDeploymentToEveryPartitionLeader(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final MutableProcessingState state = state(storage);
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 1, 2, 3);

      final RecordingBrokerClient broker = new RecordingBrokerClient();
      final DistributionFixture actor =
          leaderActor(state, broker, ignored -> route(healthyTopology(3)), events -> {});

      dispatch(actor);

      final Map<Integer, Sent> byPartition = broker.sentByPartition();
      assertEquals(3, byPartition.size());
      for (int partitionId = 1; partitionId <= 3; partitionId++) {
        final Sent sent = byPartition.get(partitionId);
        assertNotNull(sent, "expected a request for partition " + partitionId);
        assertEquals("broker-" + partitionId, sent.endpoint().brokerId());
        assertEquals(26500 + partitionId, sent.endpoint().port());

        final CommandDistributionRecord<?> pushed = sent.record();
        assertEquals(DISTRIBUTION_KEY, pushed.getDistributionKey());
        assertEquals(CLUSTER_ID, pushed.getClusterId());
        assertEquals(partitionId, pushed.getPartitionId());
        assertEquals(ValueType.JOB_SUBSCRIPTION, pushed.getValueType());
        assertEquals(JobSubscriptionIntent.CREATE.value(), pushed.getIntent());

        final JobSubscriptionRecord resource = (JobSubscriptionRecord) pushed.getValue();
        assertEquals("order", resource.getName());
        assertEquals("{\"amount\":42}", resource.getVariables());
        assertEquals("process", resource.getJobType());
      }
    }
  }

  @Test
  void routesEachDistributionOnlyThroughItsClusterTopology(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("cluster-routing"))) {
      final MutableProcessingState state = state(storage);
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 1);
      seedDistribution(
          state,
          202,
          ValueType.JOB_SUBSCRIPTION,
          JobSubscriptionIntent.CREATE,
          deployment("order", "{\"amount\":42}", "request-order").setClusterId("other"),
          1);

      final RecordingBrokerClient broker = new RecordingBrokerClient();
      final DistributionFixture actor =
          leaderActor(
              state,
              broker,
              clusterId ->
                  new BrokerDistributionRoute(
                      1,
                      Map.of(
                          1,
                          new BrokerEndpoint(
                              clusterId + "-broker",
                              "127.0.0.1",
                              CLUSTER_ID.equals(clusterId) ? 26501 : 27501,
                              CLUSTER_ID.equals(clusterId) ? 26601 : 27601))),
              events -> {});

      dispatch(actor);

      assertEquals(2, broker.sent.size());
      assertEquals("demo-broker", broker.sent.get(0).endpoint().brokerId());
      assertEquals("demo", broker.sent.get(0).request().getClusterId());
      assertEquals("other-broker", broker.sent.get(1).endpoint().brokerId());
      assertEquals("other", broker.sent.get(1).request().getClusterId());
    }
  }

  @Test
  void distributesOnePlanToSourceAndMirrorTarget(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("mirror-routing"))) {
      final MutableProcessingState state = state(storage);
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 1);
      state.getCommandDistributionState().putTarget(DISTRIBUTION_KEY, "backup", 2);
      state.getCommandDistributionState().markPending(DISTRIBUTION_KEY, "backup", 1);
      state.getCommandDistributionState().markPending(DISTRIBUTION_KEY, "backup", 2);

      final RecordingBrokerClient broker = new RecordingBrokerClient();
      final DistributionFixture actor =
          leaderActor(
              state,
              broker,
              clusterId ->
                  new BrokerDistributionRoute(
                      "backup".equals(clusterId) ? 2 : 1,
                      "backup".equals(clusterId)
                          ? Map.of(
                              1,
                              new BrokerEndpoint("backup-1", "127.0.0.1", 27501, 27601),
                              2,
                              new BrokerEndpoint("backup-2", "127.0.0.1", 27502, 27602))
                          : Map.of(1, new BrokerEndpoint("demo-1", "127.0.0.1", 26501, 26601))),
              events -> {});

      dispatch(actor);

      assertEquals(3, broker.sent.size());
      final List<CommandDistributionRecord<?>> backupRecords =
          broker.sent.stream()
              .map(Sent::record)
              .filter(record -> "backup".equals(record.getClusterId()))
              .toList();
      assertEquals(2, backupRecords.size());
      for (final CommandDistributionRecord<?> record : backupRecords) {
        assertEquals(DISTRIBUTION_KEY, record.getDistributionKey());
        assertEquals("backup", ((JobSubscriptionRecord) record.getValue()).getClusterId());
      }
      assertEquals(
          1,
          broker.sent.stream()
              .map(Sent::record)
              .filter(record -> CLUSTER_ID.equals(record.getClusterId()))
              .count());
    }
  }

  @Test
  void unavailableMirrorTargetDoesNotBlockSource(@TempDir final Path tempDir) {
    try (PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("mirror-route-unavailable"))) {
      final MutableProcessingState state = state(storage);
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 1);
      state.getCommandDistributionState().putTarget(DISTRIBUTION_KEY, "backup", 1);
      state.getCommandDistributionState().markPending(DISTRIBUTION_KEY, "backup", 1);

      final RecordingBrokerClient broker = new RecordingBrokerClient();
      final DistributionFixture actor =
          leaderActor(
              state,
              broker,
              clusterId -> {
                if ("backup".equals(clusterId)) {
                  throw new IllegalStateException("backup topology unavailable");
                }
                return new BrokerDistributionRoute(
                    1, Map.of(1, new BrokerEndpoint("demo-1", "127.0.0.1", 26501, 26601)));
              },
              event ->
                  new CommandDistributionAcknowledgedApplier(state)
                      .applyState(event.key(), (CommandDistributionRecord<?>) event.value()));

      dispatch(actor);

      assertEquals(1, broker.sent.size());
      assertFalse(state.getCommandDistributionState().isPending(DISTRIBUTION_KEY, CLUSTER_ID, 1));
      assertTrue(state.getCommandDistributionState().isPending(DISTRIBUTION_KEY, "backup", 1));
    }
  }

  @Test
  void retriesOnlyFailedMirrorTarget(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("mirror-retry"))) {
      final MutableProcessingState state = state(storage);
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 1);
      state.getCommandDistributionState().putTarget(DISTRIBUTION_KEY, "backup", 1);
      state.getCommandDistributionState().markPending(DISTRIBUTION_KEY, "backup", 1);

      final AtomicInteger backupAttempts = new AtomicInteger();
      final RecordingBrokerClient broker = new RecordingBrokerClient();
      broker.responseForEndpoint =
          (endpoint, partitionId) ->
              endpoint.brokerId().startsWith("backup") && backupAttempts.incrementAndGet() == 1
                  ? CompletableActorFuture.completedExceptionally(
                      new RuntimeException("backup unavailable"))
                  : CompletableActorFuture.completed(new BrokerResponse<>(null));
      final DistributionFixture actor =
          leaderActor(
              state,
              broker,
              clusterId ->
                  new BrokerDistributionRoute(
                      1,
                      Map.of(
                          1,
                          new BrokerEndpoint(
                              clusterId + "-1",
                              "127.0.0.1",
                              "backup".equals(clusterId) ? 27501 : 26501,
                              "backup".equals(clusterId) ? 27601 : 26601))),
              event ->
                  new CommandDistributionAcknowledgedApplier(state)
                      .applyState(event.key(), (CommandDistributionRecord<?>) event.value()));

      dispatch(actor);
      assertFalse(state.getCommandDistributionState().isPending(DISTRIBUTION_KEY, CLUSTER_ID, 1));
      assertTrue(state.getCommandDistributionState().isPending(DISTRIBUTION_KEY, "backup", 1));

      dispatch(actor);

      assertFalse(state.getCommandDistributionState().isPending(DISTRIBUTION_KEY, "backup", 1));
      assertEquals(
          1,
          broker.sent.stream()
              .filter(sent -> sent.endpoint().brokerId().startsWith("demo"))
              .count());
      assertEquals(
          2,
          broker.sent.stream()
              .filter(sent -> sent.endpoint().brokerId().startsWith("backup"))
              .count());
    }
  }

  @Test
  void acknowledgesEachPartitionAfterBrokerAccepts(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final MutableProcessingState state = state(storage);
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 1, 2);

      final List<LoggedEvent> acks = new ArrayList<>();
      final DistributionFixture actor =
          leaderActor(
              state, new RecordingBrokerClient(), ignored -> route(healthyTopology(2)), acks::add);

      dispatch(actor);

      assertEquals(2, acks.size());
      final Map<Integer, LoggedEvent> byPartition = new HashMap<>();
      for (final LoggedEvent ack : acks) {
        assertEquals(CommandDistributionIntent.ACKNOWLEDGE, ack.metadata().getIntent());
        assertEquals(ValueType.COMMAND_DISTRIBUTION, ack.metadata().getValueType());
        assertEquals(DISTRIBUTION_KEY, ack.key());
        final CommandDistributionRecord<?> record = (CommandDistributionRecord<?>) ack.value();
        byPartition.put(record.getPartitionId(), ack);
      }
      assertTrue(byPartition.containsKey(1));
      assertTrue(byPartition.containsKey(2));
    }
  }

  @Test
  void doesNotDistributeWhenNotLeader(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final MutableProcessingState state = state(storage);
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 1, 2, 3);

      final RecordingBrokerClient broker = new RecordingBrokerClient();
      final List<LoggedEvent> acks = new ArrayList<>();
      final DistributionFixture actor =
          dispatcher(
              state,
              new GrpcBrokerJobSubscriptionTransport(broker),
              () -> false,
              ignored -> route(healthyTopology(3)),
              acknowledgeInto(acks));

      dispatch(actor);

      assertTrue(broker.sent.isEmpty());
      assertTrue(acks.isEmpty());
    }
  }

  @Test
  void distributesOnlyToPendingPartitions(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final MutableProcessingState state = state(storage);
      // topology has three partitions, but only partition 2 is still pending.
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 2);

      final RecordingBrokerClient broker = new RecordingBrokerClient();
      final DistributionFixture actor =
          leaderActor(state, broker, ignored -> route(healthyTopology(3)), events -> {});

      dispatch(actor);

      assertEquals(1, broker.sent.size());
      assertEquals(2, broker.sent.get(0).record().getPartitionId());
      assertEquals("broker-2", broker.sent.get(0).endpoint().brokerId());
    }
  }

  @Test
  void defersDistributionUntilPartitionLeaderIsKnown(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final MutableProcessingState state = state(storage);
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 1);

      // Leader is elected but its endpoint is not resolvable yet.
      final ClusterTopology withoutEndpoint = topologyOf(1, Map.of(1, "broker-1"), Map.of());
      final ClusterTopology withEndpoint =
          topologyOf(
              1,
              Map.of(1, "broker-1"),
              Map.of("broker-1", new BrokerEndpoint("broker-1", "127.0.0.1", 26501, 26601)));
      final AtomicReference<ClusterTopology> topology = new AtomicReference<>(withoutEndpoint);

      final RecordingBrokerClient broker = new RecordingBrokerClient();
      final DistributionFixture actor =
          leaderActor(state, broker, ignored -> route(topology.get()), events -> {});

      dispatch(actor);
      assertTrue(broker.sent.isEmpty(), "must not push while the leader endpoint is unknown");

      // Endpoint becomes known: the deferred distribution is retried (in-flight was released).
      topology.set(withEndpoint);
      dispatch(actor);

      assertEquals(1, broker.sent.size());
      assertEquals("broker-1", broker.sent.get(0).endpoint().brokerId());
    }
  }

  @Test
  void retriesDistributionAfterBrokerFailure(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final MutableProcessingState state = state(storage);
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 1);

      final List<LoggedEvent> acks = new ArrayList<>();
      final RecordingBrokerClient broker = new RecordingBrokerClient();
      final AtomicInteger attempts = new AtomicInteger();
      broker.responseFor =
          partitionId ->
              attempts.incrementAndGet() == 1
                  ? CompletableActorFuture.completedExceptionally(
                      new RuntimeException("broker down"))
                  : CompletableActorFuture.completed(new BrokerResponse<>(null));

      final DistributionFixture actor =
          leaderActor(state, broker, ignored -> route(healthyTopology(1)), acks::add);

      dispatch(actor);
      assertEquals(1, broker.sent.size());
      assertTrue(acks.isEmpty(), "a failed push must not be acknowledged");

      // Still pending (never acked) and in-flight released, so the next scan retries.
      dispatch(actor);
      assertEquals(2, broker.sent.size());
      assertEquals(1, acks.size());
    }
  }

  @Test
  void retriesOnlyFailedPartitionAfterPartialFailure(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("partial-failure"))) {
      final MutableProcessingState state = state(storage);
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 1, 2);

      final RecordingBrokerClient broker = new RecordingBrokerClient();
      final AtomicInteger partitionTwoAttempts = new AtomicInteger();
      broker.responseFor =
          partitionId ->
              partitionId == 2 && partitionTwoAttempts.incrementAndGet() == 1
                  ? CompletableActorFuture.completedExceptionally(
                      new RuntimeException("partition 2 unavailable"))
                  : CompletableActorFuture.completed(new BrokerResponse<>(null));
      final CommandWriter submitter =
          new RecordingCommandWriter(
              event -> {
                new CommandDistributionAcknowledgedApplier(state)
                    .applyState(event.key(), (CommandDistributionRecord<?>) event.value());
              });
      final DistributionFixture actor =
          dispatcher(
              state,
              new GrpcBrokerJobSubscriptionTransport(broker),
              () -> true,
              ignored -> route(healthyTopology(2)),
              submitter);

      dispatch(actor);

      assertFalse(state.getCommandDistributionState().isPending(DISTRIBUTION_KEY, CLUSTER_ID, 1));
      assertTrue(state.getCommandDistributionState().isPending(DISTRIBUTION_KEY, CLUSTER_ID, 2));

      dispatch(actor);

      assertEquals(
          1, broker.sent.stream().filter(sent -> sent.record().getPartitionId() == 1).count());
      assertEquals(
          2, broker.sent.stream().filter(sent -> sent.record().getPartitionId() == 2).count());
      assertFalse(state.getCommandDistributionState().isPending(DISTRIBUTION_KEY, CLUSTER_ID, 2));
    }
  }

  @Test
  void retriesAfterAcknowledgementSubmissionFailure(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("ack-failure"))) {
      final MutableProcessingState state = state(storage);
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 1);

      final RecordingBrokerClient broker = new RecordingBrokerClient();
      final AtomicInteger acknowledgementAttempts = new AtomicInteger();
      final CommandWriter submitter =
          new CommandWriter() {
            @Override
            public void append(
                final long key,
                final ValueType type,
                final Intent intent,
                final UnifiedRecordValue value) {
              new CommandDistributionAcknowledgedApplier(state)
                  .applyState(key, (CommandDistributionRecord<?>) value);
            }

            @Override
            public boolean tryAppend(
                final long key,
                final ValueType type,
                final Intent intent,
                final UnifiedRecordValue value) {
              if (acknowledgementAttempts.incrementAndGet() == 1) {
                return false;
              }
              append(key, type, intent, value);
              return true;
            }
          };
      final DistributionFixture actor =
          dispatcher(
              state,
              new GrpcBrokerJobSubscriptionTransport(broker),
              () -> true,
              ignored -> route(healthyTopology(1)),
              submitter);

      workflows.get(actor).admission =
          event ->
              event.metadata().getIntent() != CommandDistributionIntent.ACKNOWLEDGE
                  || acknowledgementAttempts.incrementAndGet() != 1;
      dispatch(actor);
      assertTrue(state.getCommandDistributionState().isPending(DISTRIBUTION_KEY, CLUSTER_ID, 1));

      workflows.get(actor).clock.addAndGet(30_000);
      dispatch(actor);

      assertEquals(2, broker.sent.size());
      assertEquals(2, acknowledgementAttempts.get());
      assertFalse(state.getCommandDistributionState().isPending(DISTRIBUTION_KEY, CLUSTER_ID, 1));
    }
  }

  @Test
  void retriesUsingNewLeaderEndpoint(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("leader-change"))) {
      final MutableProcessingState state = state(storage);
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 1);

      final BrokerEndpoint oldLeader = new BrokerEndpoint("broker-old", "127.0.0.1", 26501, 26601);
      final BrokerEndpoint newLeader = new BrokerEndpoint("broker-new", "127.0.0.1", 26502, 26602);
      final AtomicReference<BrokerDistributionRoute> route =
          new AtomicReference<>(new BrokerDistributionRoute(1, Map.of(1, oldLeader)));
      final RecordingBrokerClient broker = new RecordingBrokerClient();
      final AtomicInteger attempts = new AtomicInteger();
      broker.responseFor =
          partitionId ->
              attempts.incrementAndGet() == 1
                  ? CompletableActorFuture.completedExceptionally(
                      new RuntimeException("old leader unavailable"))
                  : CompletableActorFuture.completed(new BrokerResponse<>(null));
      final DistributionFixture actor =
          leaderActor(state, broker, ignored -> route.get(), event -> {});

      dispatch(actor);
      route.set(new BrokerDistributionRoute(1, Map.of(1, newLeader)));
      dispatch(actor);

      assertEquals(
          List.of(oldLeader, newLeader), broker.sent.stream().map(Sent::endpoint).toList());
    }
  }

  @Test
  void recoveryPreservesInFlightDeadlineBeforeRetry(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("suspend"))) {
      final MutableProcessingState state = state(storage);
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 1);

      final RecordingBrokerClient broker = new RecordingBrokerClient();
      broker.responseFor = partitionId -> new CompletableActorFuture<>();
      final DistributionFixture actor =
          leaderActor(state, broker, ignored -> route(healthyTopology(1)), event -> {});

      dispatch(actor);
      actor.suspendDistributionDispatch();
      actor.onRecovered(state, workflows.get(actor));
      dispatch(actor);
      assertEquals(1, broker.sent.size());

      workflows.get(actor).clock.addAndGet(30_000);
      dispatch(actor);
      assertEquals(2, broker.sent.size());
      assertTrue(state.getCommandDistributionState().isPending(DISTRIBUTION_KEY, CLUSTER_ID, 1));
    }
  }

  @Test
  void skipsDistributionWhenTopologyUnavailable(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final MutableProcessingState state = state(storage);
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 1, 2);

      final RecordingBrokerClient broker = new RecordingBrokerClient();
      final DistributionFixture actor =
          leaderActor(
              state,
              broker,
              clusterId -> {
                throw new IllegalStateException("topology not available");
              },
              events -> {});

      // Must swallow the topology error and leave the distribution pending for a later scan.
      dispatch(actor);

      assertTrue(broker.sent.isEmpty());
    }
  }

  @Test
  void doesNotResendWhileDistributionInFlight(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final MutableProcessingState state = state(storage);
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 1);

      final RecordingBrokerClient broker = new RecordingBrokerClient();
      // Response never completes: the request stays in-flight.
      broker.responseFor = partitionId -> new CompletableActorFuture<>();

      final DistributionFixture actor =
          leaderActor(state, broker, ignored -> route(healthyTopology(1)), events -> {});

      dispatch(actor);
      dispatch(actor);

      assertEquals(1, broker.sent.size(), "an in-flight distribution must not be sent twice");
    }
  }

  @Test
  void distributesResourceDeletionToPartitionLeaders(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final MutableProcessingState state = state(storage);
      final JobSubscriptionDeletionRecord deletion =
          new JobSubscriptionDeletionRecord()
              .setClusterId(CLUSTER_ID)
              .setTenantId("tenant")
              .setSubscriptionKey(77);
      seedDistribution(
          state,
          DISTRIBUTION_KEY,
          ValueType.JOB_SUBSCRIPTION_DELETION,
          JobSubscriptionIntent.DELETE,
          deletion,
          1);

      final RecordingBrokerClient broker = new RecordingBrokerClient();
      final DistributionFixture actor =
          leaderActor(state, broker, ignored -> route(healthyTopology(1)), events -> {});

      dispatch(actor);

      assertEquals(1, broker.sent.size());
      final CommandDistributionRecord<?> pushed = broker.sent.get(0).record();
      assertEquals(ValueType.JOB_SUBSCRIPTION_DELETION, pushed.getValueType());
      assertEquals(JobSubscriptionIntent.DELETE.value(), pushed.getIntent());
      final JobSubscriptionDeletionRecord pushedDeletion =
          (JobSubscriptionDeletionRecord) pushed.getValue();
      assertEquals(77, pushedDeletion.getSubscriptionKey());
    }
  }

  @Test
  void acknowledgedPartitionsAreNotRedistributed(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final MutableProcessingState state = state(storage);
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 1, 2);

      final RecordingBrokerClient broker = new RecordingBrokerClient();
      // Simulate the raft-applied ACK clearing the pending entry, closing the loop.
      final CommandWriter submitter =
          new RecordingCommandWriter(
              event -> {
                new CommandDistributionAcknowledgedApplier(state)
                    .applyState(event.key(), (CommandDistributionRecord<?>) event.value());
              });
      final DistributionFixture actor =
          dispatcher(
              state,
              new GrpcBrokerJobSubscriptionTransport(broker),
              () -> true,
              ignored -> route(healthyTopology(2)),
              submitter);

      dispatch(actor);
      assertEquals(2, broker.sent.size());
      assertFalse(state.getCommandDistributionState().isPending(DISTRIBUTION_KEY, CLUSTER_ID, 1));
      assertFalse(state.getCommandDistributionState().isPending(DISTRIBUTION_KEY, CLUSTER_ID, 2));

      // Everything acknowledged: a subsequent scan is a no-op.
      dispatch(actor);
      assertEquals(2, broker.sent.size());
    }
  }

  @Test
  void stopsDistributingAfterClose(@TempDir final Path tempDir) throws Exception {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final MutableProcessingState state = state(storage);
      seedDeployment(state, deployment("order", "{\"amount\":42}", "request-order"), 1, 2, 3);

      final RecordingBrokerClient broker = new RecordingBrokerClient();
      final DistributionFixture actor =
          leaderActor(state, broker, ignored -> route(healthyTopology(3)), events -> {});

      actor.close();
      dispatch(actor);

      assertTrue(broker.sent.isEmpty(), "a closed actor must not distribute");
      assertTrue(broker.closed, "closing the actor must close its broker client");
    }
  }

  @Test
  void closeCancelsScanTimer(@TempDir final Path tempDir) throws Exception {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final MutableProcessingState state = state(storage);
      final RecordingBrokerClient broker = new RecordingBrokerClient();
      final DistributionFixture actor =
          dispatcher(
              state,
              new GrpcBrokerJobSubscriptionTransport(broker),
              () -> true,
              ignored -> route(healthyTopology(1)),
              acknowledgeInto(new ArrayList<>()));

      assertEquals(1, workflows.get(actor).tasks.size(), "recovery must schedule one check");

      actor.close();

      assertTrue(workflows.get(actor).history.getFirst().canceled, "close() must cancel the check");
    }
  }

  // --- helpers -----------------------------------------------------------------------------------

  private static MutableProcessingState state(final PartitionStorage storage) {
    return new MutableProcessingState(new StateStoreProcessingStore(storage));
  }

  private DistributionFixture leaderActor(
      final MutableProcessingState state,
      final BrokerClient broker,
      final BrokerDistributionRouteResolver routeResolver,
      final Consumer<LoggedEvent> onAck) {
    return dispatcher(
        state,
        new GrpcBrokerJobSubscriptionTransport(broker),
        () -> true,
        routeResolver,
        acknowledgeInto(onAck));
  }

  private DistributionFixture dispatcher(
      final MutableProcessingState state,
      final BrokerJobSubscriptionTransport transport,
      final BooleanSupplier leader,
      final BrokerDistributionRouteResolver routes,
      final CommandWriter onAck) {
    final var scheduling = new WorkflowScheduler(new AtomicLong(100_000));
    final var dispatcher =
        new DistributionFixture(state, transport, leader, routes, scheduling, onAck);
    scheduling.processor =
        event -> {
          if (event.metadata().getIntent() == CommandDistributionIntent.DISPATCH) {
            dispatcher.processDispatch(
                (CommandDistributionRecord<?>) event.value(), scheduling.writers);
          } else {
            dispatcher.processResult(
                event.metadata().getIntent(), (CommandDistributionRecord<?>) event.value());
          }
        };
    workflows.put(dispatcher, scheduling);
    state.getCommandDistributionState().rebuildPendingIndex();
    dispatcher.onRecovered(state, scheduling);
    return dispatcher;
  }

  private void dispatch(final DistributionFixture dispatcher) {
    final var scheduling = workflows.get(dispatcher);
    scheduling.clock.addAndGet(1000);
    dispatcher.requestDistributionDispatch();
    scheduling.clock.addAndGet(100);
    scheduling.runReady();
  }

  private static CommandWriter acknowledgeInto(final List<LoggedEvent> sink) {
    return acknowledgeInto(sink::add);
  }

  private static CommandWriter acknowledgeInto(final Consumer<LoggedEvent> onAck) {
    return new RecordingCommandWriter(onAck);
  }

  private static JobSubscriptionRecord deployment(
      final String resourceName, final String content, final String checksum) {
    return new JobSubscriptionRecord()
        .setClusterId(CLUSTER_ID)
        .setTenantId("tenant")
        .setName(resourceName)
        .setJobType("process")
        .setVariables(content);
  }

  private static void seedDeployment(
      final MutableProcessingState state,
      final JobSubscriptionRecord resource,
      final int... pendingPartitions) {
    seedDistribution(
        state,
        DISTRIBUTION_KEY,
        ValueType.JOB_SUBSCRIPTION,
        JobSubscriptionIntent.CREATE,
        resource,
        pendingPartitions);
  }

  private static void seedDistribution(
      final MutableProcessingState state,
      final long distributionKey,
      final ValueType valueType,
      final Intent intent,
      final UnifiedRecordValue value,
      final int... pendingPartitions) {
    final CommandDistributionRecord<UnifiedRecordValue> record =
        new CommandDistributionRecord<UnifiedRecordValue>()
            .setDistributionKey(distributionKey)
            .setClusterId(clusterIdOf(value))
            .setValueType(valueType)
            .setIntent(intent)
            .setValue(value);
    state.getCommandDistributionState().putDistribution(record);
    state
        .getCommandDistributionState()
        .putTarget(
            distributionKey, clusterIdOf(value), Arrays.stream(pendingPartitions).max().orElse(0));
    for (final int partitionId : pendingPartitions) {
      state
          .getCommandDistributionState()
          .markPending(distributionKey, clusterIdOf(value), partitionId);
    }
  }

  private static String clusterIdOf(final UnifiedRecordValue value) {
    if (value instanceof JobSubscriptionRecord deployment) {
      return deployment.getClusterId();
    }
    if (value instanceof JobSubscriptionDeletionRecord deletion) {
      return deletion.getClusterId();
    }
    throw new IllegalArgumentException("unsupported distribution value " + value.getClass());
  }

  private static ClusterTopology healthyTopology(final int partitionCount) {
    final Map<Integer, String> leaders = new HashMap<>();
    final Map<String, BrokerEndpoint> brokers = new HashMap<>();
    for (int partitionId = 1; partitionId <= partitionCount; partitionId++) {
      final String brokerId = "broker-" + partitionId;
      leaders.put(partitionId, brokerId);
      brokers.put(
          brokerId,
          new BrokerEndpoint(brokerId, "127.0.0.1", 26500 + partitionId, 26600 + partitionId));
    }
    return topologyOf(partitionCount, leaders, brokers);
  }

  private static ClusterTopology topologyOf(
      final int partitionCount,
      final Map<Integer, String> leaderByPartition,
      final Map<String, BrokerEndpoint> brokers) {
    final List<Partition> partitions = new ArrayList<>();
    for (int partitionId = 1; partitionId <= partitionCount; partitionId++) {
      final String leaderId = leaderByPartition.getOrDefault(partitionId, "");
      final List<String> replicas = leaderId.isEmpty() ? List.of() : List.of(leaderId);
      partitions.add(
          new Partition(
              partitionId,
              new Partition.Leadership(leaderId, 1),
              new Partition.Membership(replicas, List.of(), Map.of(), 0),
              PartitionStatus.ACTIVE));
    }
    return new ClusterTopology(
        CLUSTER_ID, 1, partitionCount, 1, partitions, brokers, Instant.EPOCH);
  }

  private static BrokerDistributionRoute route(final ClusterTopology topology) {
    final Map<Integer, BrokerEndpoint> leaders = new HashMap<>();
    for (int partitionId = 1; partitionId <= topology.partitionSize(); partitionId++) {
      final int currentPartitionId = partitionId;
      topology
          .leaderEndpoint(currentPartitionId)
          .ifPresent(endpoint -> leaders.put(currentPartitionId, endpoint));
    }
    return new BrokerDistributionRoute(topology.partitionSize(), Map.copyOf(leaders));
  }

  // --- fakes -------------------------------------------------------------------------------------

  /**
   * Test driver around the production Processor/Scheduler, with no replacement scheduling logic.
   */
  private static final class DistributionFixture implements AutoCloseable {
    private final MutableProcessingState state;
    private final BrokerJobSubscriptionTransport transport;
    private final BooleanSupplier leader;
    private final BrokerDistributionRouteResolver routes;
    private final WorkflowScheduler scheduling;
    private final BrokerDistributionScheduler checker;
    private final CommandDistributionDispatchProcessor processor;
    private final CommandDistributionRetryProcessor retries;
    private final CommandDistributionAcknowledgeProcessor acknowledgements;
    private long position = 100;
    private boolean closed;

    DistributionFixture(
        final MutableProcessingState state,
        final BrokerJobSubscriptionTransport transport,
        final BooleanSupplier leader,
        final BrokerDistributionRouteResolver routes,
        final WorkflowScheduler scheduling,
        final CommandWriter acceptedAcks) {
      this.state = state;
      this.transport = transport;
      this.leader = leader;
      this.routes = routes;
      this.scheduling = scheduling;
      final var stateWriter = mock(StateWriter.class);
      when(scheduling.writers.state()).thenReturn(stateWriter);
      when(scheduling.writers.rejection()).thenReturn(mock(TypedRejectionWriter.class));
      doAnswer(
              call -> {
                final long key = call.getArgument(0);
                final Intent intent = call.getArgument(1);
                final CommandDistributionRecord<?> value = call.getArgument(2);
                if (intent == CommandDistributionIntent.ACKNOWLEDGED) {
                  acceptedAcks.append(
                      key,
                      ValueType.COMMAND_DISTRIBUTION,
                      CommandDistributionIntent.ACKNOWLEDGE,
                      value);
                } else {
                  PlacementDriverEventAppliers.create(state).applyState(key, intent, value, 1);
                }
                return null;
              })
          .when(stateWriter)
          .appendFollowUpEvent(anyLong(), any(Intent.class), any(UnifiedRecordValue.class));
      final var context = context(scheduling.writers);
      processor =
          new CommandDistributionDispatchProcessor(
              context, leader, () -> transport, routes, Duration.ofSeconds(30));
      acknowledgements = new CommandDistributionAcknowledgeProcessor(context);
      retries = new CommandDistributionRetryProcessor(context, processor::active);
      checker =
          new BrokerDistributionScheduler(
              state.getCommandDistributionState(), leader, scheduling.clock::get);
    }

    private TypedRecordProcessorContext context(final Writers writers) {
      final var context = mock(TypedRecordProcessorContext.class);
      when(context.getProcessingState(ProcessingState.class)).thenReturn(state);
      when(context.getWriters()).thenReturn(writers);
      return context;
    }

    private TypedRecord<CommandDistributionRecord<?>> record(
        final Intent intent, final CommandDistributionRecord<?> value) {
      @SuppressWarnings("unchecked")
      final TypedRecord<CommandDistributionRecord<?>> record = mock(TypedRecord.class);
      when(record.getValue()).thenAnswer(ignored -> value);
      when(record.getKey()).thenReturn(value.getDistributionKey());
      when(record.getIntent()).thenReturn(intent);
      when(record.getValueType()).thenReturn(ValueType.COMMAND_DISTRIBUTION);
      when(record.getTimestamp()).thenReturn(scheduling.clock.get());
      when(record.getPosition()).thenReturn(++position);
      return record;
    }

    void processDispatch(final CommandDistributionRecord<?> value, final Writers writers) {
      if (writers == scheduling.writers) {
        processor.processRecord(record(CommandDistributionIntent.DISPATCH, value));
      } else {
        final var stateWriter = scheduling.writers.state();
        when(writers.state()).thenReturn(stateWriter);
        final var isolated =
            new CommandDistributionDispatchProcessor(
                context(writers), leader, () -> transport, routes, Duration.ofSeconds(30));
        isolated.onRecovered(scheduling);
        isolated.processRecord(record(CommandDistributionIntent.DISPATCH, value));
      }
    }

    void processResult(final Intent intent, final CommandDistributionRecord<?> value) {
      final var record = record(intent, value);
      if (intent == CommandDistributionIntent.RETRY) {
        retries.processRecord(record);
      } else if (intent == CommandDistributionIntent.ACKNOWLEDGE) {
        acknowledgements.processRecord(record);
      }
      checker.onProcessed(record);
    }

    void requestDistributionDispatch() {
      if (!closed) {
        checker.onProcessed(
            record(CommandDistributionIntent.QUERY, new CommandDistributionRecord<>()));
      }
    }

    void onRecovered(final MutableProcessingState ignored, final WorkflowScheduler service) {
      closed = false;
      processor.onRecovered(service);
      checker.onRecovered(service);
    }

    void suspendDistributionDispatch() {
      close();
    }

    @Override
    public void close() {
      closed = true;
      processor.onClose();
      checker.onClose();
    }
  }

  private record Sent(BrokerEndpoint endpoint, BrokerCommandDistributionRequest request) {
    CommandDistributionRecord<?> record() {
      return (CommandDistributionRecord<?>) request.getRequestWriter();
    }
  }

  private static final class RecordingBrokerClient implements BrokerClient {
    private final List<Sent> sent = new ArrayList<>();
    private IntFunction<ActorFuture<?>> responseFor =
        partitionId -> CompletableActorFuture.completed(new BrokerResponse<>(null));
    private BiFunction<BrokerEndpoint, Integer, ActorFuture<?>> responseForEndpoint;
    private boolean closed;

    Map<Integer, Sent> sentByPartition() {
      final Map<Integer, Sent> byPartition = new HashMap<>();
      for (final Sent value : sent) {
        byPartition.put(value.record().getPartitionId(), value);
      }
      return byPartition;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> ActorFuture<BrokerResponse<T>> sendRequest(
        final BrokerEndpoint endpoint, final BrokerRequest<T> request) {
      sent.add(new Sent(endpoint, (BrokerCommandDistributionRequest) request));
      return (ActorFuture)
          (responseForEndpoint == null
              ? responseFor.apply(request.getPartitionId())
              : responseForEndpoint.apply(endpoint, request.getPartitionId()));
    }

    @Override
    public void close() {
      closed = true;
    }
  }
}
