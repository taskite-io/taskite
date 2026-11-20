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
package io.taskite.placementdriver.job;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

import io.grpc.Status;
import io.taskite.broker.client.response.BrokerError;
import io.taskite.broker.client.response.BrokerErrorException;
import io.taskite.broker.client.response.BrokerRejection;
import io.taskite.broker.client.response.BrokerRejectionException;
import io.taskite.placementdriver.WorkflowScheduler;
import io.taskite.placementdriver.broker.distribution.BrokerDistributionRoute;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.processing.job.JobYieldAcknowledgeProcessor;
import io.taskite.placementdriver.processing.job.JobYieldCommandSender;
import io.taskite.placementdriver.processing.job.JobYieldScheduler;
import io.taskite.placementdriver.processing.job.JobYieldUpdateProgressProcessor;
import io.taskite.placementdriver.state.appliers.BrokerClusterCreatedApplier;
import io.taskite.placementdriver.state.appliers.PlacementDriverEventAppliers;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.impl.record.value.placementdriver.job.JobYieldPlan;
import io.taskite.protocol.impl.record.value.placementdriver.job.JobYieldSyncRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.ErrorCode;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.protocol.record.value.broker.JobYieldReason;
import io.taskite.protocol.record.value.broker.JobYieldStatus;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.StateWriter;
import io.taskite.streamplatform.api.writer.TypedRejectionWriter;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.PartitionStatus;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Exercises durable scheduling and business results with real RocksDB state. */
class JobYieldSchedulingTest {
  private static final AtomicLong POSITIONS = new AtomicLong(100);
  private static final BrokerEndpoint ENDPOINT =
      new BrokerEndpoint("broker", "127.0.0.1", 26500, 26600);
  @TempDir Path directory;

  @Test
  void alternatingPlanAndSynchronizationKeysPreserveDeadlinesPaginationAndDeletion()
      throws Exception {
    try (var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var yields = new MutableProcessingState(store).getJobYieldState();
      final var early = yieldPlan(91, "a:b", 2, 100);
      final var late = yieldPlan(3, "other", 1, 200);
      yields.create(early);
      yields.create(late);
      // A duplicate with another deadline must not change the original plan or indexes.
      yields.create(yieldPlan(91, "a:b", 2, 900));
      final var sync = yields.synchronization();
      final var firstSync = new JobYieldSyncRecord("a:b", 2, 17, 9, 110, true);
      final var secondSync = new JobYieldSyncRecord("other", 1, 21, 10, 210, true);
      sync.put(firstSync);
      sync.put(secondSync);
      store.getCurrentTransaction().commit();
      assertEquals(List.of(late, early), yields.pendingPlans(0, 10));
      assertEquals(List.of(early), yields.pendingPlans(3, 1));
      assertEquals(List.of(early), yields.plansAfter("a:b", 0, 10));
      assertEquals(
          List.of(91L, 3L),
          yields.dueBefore(200, 10).stream().map(target -> target.yieldId()).toList());
      assertEquals(100, yields.nextDueAt());
      assertEquals(1, yields.reservedTargets(100, 10));
      assertEquals(List.of(firstSync, secondSync), sync.dueBefore(210, 10));
      assertEquals(1, sync.reservedTargets(110, 10));
      assertEquals(firstSync, sync.get("a:b", 2).orElseThrow());
      assertEquals(secondSync, sync.get("other", 1).orElseThrow());
      store.getCurrentTransaction();
      yields.updateProgress(
          new JobYieldRecord()
              .setClusterId("a:b")
              .setYieldId(91)
              .setPartitionId(2)
              .setTimestampEpochMillis(300),
          true);
      sync.delete("a:b", 2);
      assertEquals(List.of(late), yields.pendingPlans(0, 10));
      assertEquals(200, yields.nextDueAt());
      assertEquals(List.of(secondSync), sync.dueBefore(300, 10));
      store.getCurrentTransaction().rollback();
      assertEquals(List.of(late, early), yields.pendingPlans(0, 10));
      assertEquals(List.of(firstSync, secondSync), sync.dueBefore(210, 10));
      yields.deleteForWorker("a:b", "tenant", "worker");
      sync.put(new JobYieldSyncRecord("a:b", 2, 18, 0, 400, false));
      assertTrue(yields.plan("a:b", 91).isEmpty());
      assertEquals(List.of(late), yields.pendingPlans(0, 10));
      assertEquals(List.of(secondSync), sync.dueBefore(300, 10));
      assertEquals(1, sync.reservedTargets(0, 10));
      assertEquals(0, sync.reservedTargets(210, 10));
      assertEquals(late, yields.plan("other", 3).orElseThrow());
    }
  }

  private static JobYieldPlan yieldPlan(
      final long id, final String cluster, final int partition, final long dueAt) {
    return new JobYieldPlan(
        id,
        cluster,
        "tenant",
        "worker",
        42,
        JobYieldReason.WORKER_LEASE_EXPIRED,
        1,
        0,
        null,
        List.of(
            new JobYieldPlan.Partition(
                partition, JobYieldStatus.PENDING, 0, 1, 10, dueAt, 0, "", "")));
  }

  @Test
  void retriesFeatureDisabledAndPreservesCumulativeResultsThroughCompletion() throws Exception {
    final var results = script(directory.resolve("retry"));
    assertEquals(4, results.size());
    assertTrue(results.getFirst().endsWith(":1"));
    assertTrue(results.get(1).endsWith(":2"));
    assertTrue(results.get(2).endsWith(":3"));
    assertEquals("COMPLETED:5", results.getLast());
  }

  private List<String> script(final Path path) throws Exception {
    try (final var fixture = new Fixture(path)) {
      fixture.seed(10, 1, 0);
      fixture.yieldResponse =
          request -> {
            final int count = fixture.requests.size();
            if (count == 1) {
              return CompletableActorFuture.completedExceptionally(
                  new BrokerRejectionException(
                      new BrokerRejection(
                          JobYieldIntent.FENCE_SESSION,
                          10,
                          RejectionType.FEATURE_DISABLED,
                          "disabled")));
            }
            return CompletableActorFuture.completed(
                request
                    .copy()
                    .setStatus(count == 2 ? JobYieldStatus.PENDING : JobYieldStatus.COMPLETED)
                    .setReleasedJobCount(count == 2 ? 5 : 3));
          };
      fixture.tick(200);
      fixture.tick(200);
      assertEquals("FEATURE_DISABLED", fixture.target(10).pauseReason());
      fixture.tick(1500);
      fixture.tick(200);
      assertEquals(5, fixture.target(10).releasedJobCount());
      fixture.tick(2500);
      fixture.tick(200);
      assertEquals(JobYieldStatus.COMPLETED, fixture.target(10).status());
      assertEquals(5, fixture.target(10).releasedJobCount(), "cumulative count must not regress");
      assertEquals(-1, fixture.state.getJobYieldState().nextDueAt());
      assertTrue(fixture.state.getJobYieldState().pendingPlans(0, 100).isEmpty());
      final var results =
          new ArrayList<>(
              fixture.requests.stream()
                  .map(
                      request ->
                          request.getYieldId()
                              + ":"
                              + request.getClusterId()
                              + ":"
                              + request.getTenantId()
                              + ":"
                              + request.getWorkerId()
                              + ":"
                              + request.getWorkerLeaseToken()
                              + ":"
                              + request.getReason()
                              + ":"
                              + request.getPartitionId()
                              + ":"
                              + request.getAttempts())
                  .toList());
      results.add(fixture.target(10).status() + ":" + fixture.target(10).releasedJobCount());
      return results;
    }
  }

  @Test
  void followsBrokerCursorPaginationWithoutPendingYieldPlans() throws Exception {
    assertEquals(2, syncScript(directory.resolve("sync")).size());
  }

  private List<String> syncScript(final Path path) throws Exception {
    try (final var fixture = new Fixture(path)) {
      fixture.topologies(1);
      for (int id = 1; id <= 105; id++) fixture.seed(id, 0, 0);
      fixture.syncResponse =
          request ->
              CompletableActorFuture.completed(
                  request
                      .copy()
                      .setSyncCursor(fixture.syncs.size() == 1 ? 7 : 105)
                      .setSyncComplete(fixture.syncs.size() != 1));
      fixture.tick(200);
      fixture.tick(200);
      assertEquals(2, fixture.syncs.size());
      assertEquals(0, fixture.syncs.getFirst().getPreviousSyncCursor());
      assertEquals(7, fixture.syncs.getLast().getPreviousSyncCursor());
      assertEquals(100, fixture.syncs.getFirst().getSessions().size());
      assertEquals(98, fixture.syncs.getLast().getSessions().size());
      assertTrue(fixture.requests.isEmpty());
      return fixture.syncs.stream()
          .map(
              request ->
                  request.getPreviousSyncCursor()
                      + ":"
                      + request.getSyncCursor()
                      + ":"
                      + request.isSyncComplete()
                      + ":"
                      + request.getSessions().stream()
                          .map(
                              session -> session.getYieldId() + "/" + session.getWorkerLeaseToken())
                          .toList())
          .toList();
    }
  }

  @Test
  void bRecoversLostRequestsAndPersistsSyncCursorAcrossReopen() throws Exception {
    final var path = directory.resolve("recovery");
    try (final var fixture = new Fixture(path)) {
      fixture.topologies(1);
      fixture.seed(10, 1, 0);
      fixture.syncResponse =
          request ->
              fixture.syncs.size() == 1
                  ? CompletableActorFuture.completed(
                      request.copy().setSyncCursor(7).setSyncComplete(false))
                  : new CompletableActorFuture<>();
      fixture.yieldResponse = ignored -> new CompletableActorFuture<>();
      fixture.tick(200);
      fixture.tick(200);
      assertEquals(
          7,
          fixture.state.getJobYieldState().synchronization().get("demo", 1).orElseThrow().cursor());
      assertEquals(1, fixture.requests.size());
    }
    try (final var reopened = new Fixture(path)) {
      assertEquals(
          7,
          reopened
              .state
              .getJobYieldState()
              .synchronization()
              .get("demo", 1)
              .orElseThrow()
              .cursor());
      reopened.tick(200);
      assertTrue(reopened.requests.isEmpty());
      assertTrue(reopened.syncs.isEmpty());
      reopened.tick(31_000);
      assertEquals(7, reopened.syncs.getFirst().getPreviousSyncCursor());
      assertEquals(2, reopened.requests.getFirst().getAttempts());
      assertEquals(JobYieldStatus.COMPLETED, reopened.target(10).status());
    }
  }

  @Test
  void bIgnoresOldAttemptWrongLeaseAndOldLeaderCallbacks() throws Exception {
    try (final var fixture = new Fixture(directory.resolve("fencing"))) {
      fixture.seed(10, 1, 0);
      final var old = new CompletableActorFuture<JobYieldRecord>();
      final var current = new CompletableActorFuture<JobYieldRecord>();
      fixture.yieldResponse = request -> fixture.requests.size() == 1 ? old : current;
      fixture.tick(200);
      final var first = fixture.requests.getFirst().copy();
      fixture.tick(31_000);
      assertEquals(2, fixture.target(10).attempts());
      old.complete(first.setStatus(JobYieldStatus.COMPLETED).setReleasedJobCount(100));
      fixture.scheduler.runReady();
      assertEquals(JobYieldStatus.PENDING, fixture.target(10).status());
      current.complete(fixture.requests.getLast().copy().setWorkerLeaseToken(1));
      fixture.scheduler.runReady();
      assertEquals("INVALID_YIELD_JOBS_RESPONSE", fixture.target(10).lastError());
      assertEquals(0, fixture.target(10).releasedJobCount());
      final var late = new CompletableActorFuture<JobYieldRecord>();
      fixture.yieldResponse = ignored -> late;
      fixture.tick(2500);
      final int events = fixture.events.size();
      fixture.leader.set(false);
      fixture.processor.onClose();
      late.complete(fixture.requests.getLast().copy().setStatus(JobYieldStatus.COMPLETED));
      fixture.scheduler.runReady();
      assertEquals(events, fixture.events.size());
    }
  }

  @Test
  void bRespectsSharedSixteenAndEightSyncQuotasAndAdmissionBudget() throws Exception {
    try (final var fixture = new Fixture(directory.resolve("quotas"))) {
      fixture.topologies(20);
      for (int id = 1; id <= 30; id++) fixture.seed(id, id, 0);
      fixture.yieldResponse = ignored -> new CompletableActorFuture<>();
      fixture.syncResponse = ignored -> new CompletableActorFuture<>();
      fixture.scheduler.capacity = 1;
      fixture.tick(200);
      assertEquals(1, fixture.syncs.size());
      fixture.scheduler.capacity = 100;
      fixture.tick(200);
      assertEquals(8, fixture.syncs.size());
      assertEquals(8, fixture.requests.size());
      fixture.tick(200);
      assertEquals(16, fixture.requests.size() + fixture.syncs.size());
    }
    try (final var fixture = new Fixture(directory.resolve("admission"))) {
      fixture.seed(10, 1, 0);
      fixture.scheduler.admission = ignored -> false;
      fixture.tick(200);
      assertEquals(0, fixture.target(10).attempts());
      assertTrue(fixture.requests.isEmpty());
      fixture.scheduler.admission = ignored -> true;
      fixture.tick(200);
      assertEquals(1, fixture.requests.size());
    }
  }

  @Test
  void bDueIndexDoesNotScanFuturePagesAndDeleteRollsBackAtomically() throws Exception {
    try (final var fixture = new Fixture(directory.resolve("index"))) {
      for (int id = 1; id <= 105; id++) fixture.seed(id, 1, 3_600_000);
      fixture.seed(106, 1, 0);
      fixture.tick(200);
      assertEquals(
          List.of(106L), fixture.requests.stream().map(JobYieldRecord::getYieldId).toList());
      fixture.state.getJobYieldState().deleteForWorker("demo", "a", "worker");
      assertEquals(-1, fixture.state.getJobYieldState().nextDueAt());
      fixture.store.getCurrentTransaction().rollback();
      fixture.store.getCurrentTransaction();
      assertEquals(105, fixture.state.getJobYieldState().dueBefore(3_600_000, 200).size());
      fixture.state.getJobYieldState().deleteForWorker("demo", "a", "worker");
      fixture.commit();
      assertEquals(-1, fixture.state.getJobYieldState().nextDueAt());
    }
  }

  @Test
  void bTopologyDeletionCleansSyncAndRejectsQueuedResponseAfterRecreation() throws Exception {
    try (final var fixture = new Fixture(directory.resolve("delete"))) {
      fixture.topologies(1);
      final var delayed = new CompletableActorFuture<JobYieldRecord>();
      fixture.syncResponse = ignored -> delayed;
      fixture.tick(200);
      final var request = fixture.syncs.getFirst();
      final PartitionTopology topology =
          fixture
              .state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow();
      fixture.state.getBrokerPartitionTopologyState().deletePartitionTopology("demo", 1);
      fixture.commit();
      assertEquals(-1, fixture.state.getJobYieldState().synchronization().nextDueAt());
      fixture.state.getBrokerPartitionTopologyState().putPartitionTopology(topology);
      fixture.commit();
      delayed.complete(request.copy().setSyncCursor(100).setSyncComplete(true));
      fixture.scheduler.runReady();
      assertEquals(
          0,
          fixture.state.getJobYieldState().synchronization().get("demo", 1).orElseThrow().cursor());
    }
  }

  @Test
  void repeatedRestartsResumePendingPlansWithMonotonicAttempts() throws Exception {
    final var path = directory.resolve("restart");
    try (final var a = new Fixture(path)) {
      a.seed(10, 1, 0);
      a.yieldResponse = ignored -> new CompletableActorFuture<>();
      a.tick(200);
      a.tick(200);
      assertEquals(1, a.requests.size());
    }
    try (final var b = new Fixture(path)) {
      b.yieldResponse = ignored -> new CompletableActorFuture<>();
      b.tick(31_000);
      assertEquals(2, b.requests.getFirst().getAttempts());
    }
    try (final var a = new Fixture(path)) {
      a.tick(62_000);
      a.tick(200);
      a.tick(200);
      assertEquals(3, a.requests.getFirst().getAttempts());
      assertEquals(JobYieldStatus.COMPLETED, a.target(10).status());
    }
  }

  @Test
  void bRetriesOnNewLeaderRouteAndLostResultAdmission() throws Exception {
    try (final var fixture = new Fixture(directory.resolve("route"))) {
      fixture.seed(10, 1, 0);
      fixture.route.set(new BrokerDistributionRoute(1, Map.of()));
      fixture.tick(200);
      assertTrue(fixture.requests.isEmpty());
      assertEquals(1, fixture.target(10).attempts());
      fixture.route.set(new BrokerDistributionRoute(1, Map.of(1, ENDPOINT)));
      fixture.scheduler.admission =
          event -> event.metadata().getIntent() != JobYieldIntent.ACKNOWLEDGE;
      fixture.tick(1500);
      assertEquals(1, fixture.requests.size());
      assertEquals(JobYieldStatus.PENDING, fixture.target(10).status());
      fixture.scheduler.admission = ignored -> true;
      fixture.tick(31_000);
      assertEquals(2, fixture.requests.size());
      assertEquals(JobYieldStatus.COMPLETED, fixture.target(10).status());
    }
  }

  @Test
  void bSyncErrorsRetainCursorAndLostSyncResultRetriesFromCommittedDeadline() throws Exception {
    try (final var fixture = new Fixture(directory.resolve("sync-errors"))) {
      fixture.topologies(1);
      fixture.syncResponse =
          ignored ->
              CompletableActorFuture.completedExceptionally(new IllegalStateException("retry"));
      fixture.tick(200);
      final var failed =
          fixture.state.getJobYieldState().synchronization().get("demo", 1).orElseThrow();
      assertEquals(0, failed.cursor());
      assertFalse(failed.inFlight());
      assertEquals(fixture.clock.get() + 1000, failed.dueAt());
      fixture.syncResponse =
          request ->
              CompletableActorFuture.completed(
                  request.copy().setSyncCursor(15).setSyncComplete(true));
      fixture.scheduler.admission =
          event -> event.metadata().getIntent() != JobYieldIntent.UPDATE_PARTITION_SYNC;
      fixture.tick(1200);
      assertTrue(
          fixture
              .state
              .getJobYieldState()
              .synchronization()
              .get("demo", 1)
              .orElseThrow()
              .inFlight());
      fixture.scheduler.admission = ignored -> true;
      fixture.tick(31_000);
      final var recovered =
          fixture.state.getJobYieldState().synchronization().get("demo", 1).orElseThrow();
      assertEquals(15, recovered.cursor());
      assertFalse(recovered.inFlight());
      assertEquals(3, fixture.syncs.size());
    }
  }

  @Test
  void bIdleSynchronizationWakesWhenBrokerRegistrationMaterializesTopology() throws Exception {
    try (final var fixture = new Fixture(directory.resolve("idle-wakeup"))) {
      fixture.tick(200);
      assertTrue(fixture.scheduler.tasks.isEmpty());
      fixture.topologies(1);
      fixture.tick(200);
      assertEquals(1, fixture.syncs.size());
      assertTrue(fixture.requests.isEmpty());
    }
  }

  @Test
  void persistsTypedFailuresWithoutRemoteDescriptions() throws Exception {
    final var failures =
        List.of(
            new Failure(
                new CompletionException(
                    new BrokerRejectionException(
                        new BrokerRejection(
                            JobYieldIntent.FENCE_SESSION,
                            10,
                            RejectionType.FEATURE_DISABLED,
                            "sensitive remote description"))),
                "FEATURE_DISABLED",
                "FEATURE_DISABLED"),
            new Failure(
                new BrokerRejectionException(
                    new BrokerRejection(
                        JobYieldIntent.FENCE_SESSION,
                        10,
                        RejectionType.INVALID_ARGUMENT,
                        "sensitive remote description")),
                "INVALID_ARGUMENT",
                ""),
            new Failure(
                new BrokerErrorException(
                    new BrokerError(ErrorCode.RESOURCE_EXHAUSTED, "sensitive remote description")),
                "RESOURCE_EXHAUSTED",
                ""),
            new Failure(
                Status.UNAUTHENTICATED
                    .withDescription("sensitive remote description")
                    .asRuntimeException(),
                "UNAUTHENTICATED",
                ""));
    for (final var failure : failures) {
      final var path = directory.resolve(failure.code());
      try (final var fixture = new Fixture(path)) {
        fixture.seed(10, 1, 0);
        fixture.yieldResponse =
            ignored -> CompletableActorFuture.completedExceptionally(failure.error());
        fixture.tick(200);
        final var target = fixture.target(10);
        assertEquals(1, target.attempts());
        assertEquals(failure.code(), target.lastError());
        assertEquals(failure.pause(), target.pauseReason());
        assertEquals(JobYieldStatus.PENDING, target.status());
        assertTrue(target.nextAttemptAtEpochMillis() > fixture.clock.get());
      }
      try (final var reopened = new Fixture(path)) {
        assertEquals(failure.code(), reopened.target(10).lastError());
        assertEquals(failure.pause(), reopened.target(10).pauseReason());
      }
    }
  }

  @Test
  void acceptsMatchingResultsAndRejectsInvalidLeaseCountOrStatus() throws Exception {
    for (final var status : List.of(JobYieldStatus.PENDING, JobYieldStatus.COMPLETED)) {
      try (final var fixture = new Fixture(directory.resolve(status.name()))) {
        fixture.seed(10, 1, 0);
        fixture.yieldResponse =
            request ->
                CompletableActorFuture.completed(
                    request.copy().setStatus(status).setReleasedJobCount(8));
        fixture.tick(200);
        assertEquals(status, fixture.target(10).status());
        assertEquals(8, fixture.target(10).releasedJobCount());
        assertEquals("", fixture.target(10).lastError());
        assertEquals(
            status == JobYieldStatus.COMPLETED
                ? JobYieldIntent.ACKNOWLEDGED
                : JobYieldIntent.PROGRESS_UPDATED,
            fixture.events.getLast());
      }
    }
    final List<Function<JobYieldRecord, JobYieldRecord>> invalid =
        List.of(
            request -> request.setWorkerLeaseToken(1),
            request -> request.setReleasedJobCount(-1),
            request -> request.setStatus(JobYieldStatus.RETIRED));
    for (int index = 0; index < invalid.size(); index++) {
      try (final var fixture = new Fixture(directory.resolve("invalid-" + index))) {
        fixture.seed(10, 1, 0);
        final var transform = invalid.get(index);
        fixture.yieldResponse =
            request -> CompletableActorFuture.completed(transform.apply(request.copy()));
        fixture.tick(200);
        assertEquals(JobYieldStatus.PENDING, fixture.target(10).status());
        assertEquals("INVALID_YIELD_JOBS_RESPONSE", fixture.target(10).lastError());
        assertEquals(0, fixture.target(10).releasedJobCount());
        assertEquals(JobYieldIntent.PROGRESS_UPDATED, fixture.events.getLast());
      }
    }
  }

  private record Failure(Throwable error, String code, String pause) {}

  private static final class Fixture implements AutoCloseable {
    private final PartitionStorage storage;
    private final StateStoreProcessingStore store;
    private final MutableProcessingState state;
    private final AtomicLong clock = new AtomicLong(10_000);
    private final AtomicBoolean leader = new AtomicBoolean(true);
    private final WorkflowScheduler scheduler = new WorkflowScheduler(clock);
    private final List<JobYieldRecord> requests = new ArrayList<>();
    private final List<JobYieldRecord> syncs = new ArrayList<>();
    private final List<Intent> events = new ArrayList<>();
    private final AtomicReference<BrokerDistributionRoute> route = new AtomicReference<>();
    private Function<JobYieldRecord, ActorFuture<JobYieldRecord>> yieldResponse =
        request ->
            CompletableActorFuture.completed(request.copy().setStatus(JobYieldStatus.COMPLETED));
    private Function<JobYieldRecord, ActorFuture<JobYieldRecord>> syncResponse =
        request -> CompletableActorFuture.completed(request.copy().setSyncComplete(true));
    private final JobYieldCommandSender processor;
    private final JobYieldScheduler bScheduler;
    private final JobYieldUpdateProgressProcessor progress;
    private final JobYieldAcknowledgeProcessor ack;
    private boolean committed;

    private Fixture(final Path path) {
      storage = new PartitionStorage(path);
      store = new StateStoreProcessingStore(storage);
      state = new MutableProcessingState(store);
      final var context = mock(TypedRecordProcessorContext.class);
      when(context.getProcessingState(ProcessingState.class)).thenReturn(state);
      when(context.getWriters()).thenReturn(scheduler.writers);
      final var appliers = PlacementDriverEventAppliers.create(state);
      final var stateWriter = mock(StateWriter.class);
      when(scheduler.writers.state()).thenReturn(stateWriter);
      doAnswer(
              invocation -> {
                committed = false;
                final Intent intent = invocation.getArgument(1);
                events.add(intent);
                appliers.applyState(
                    invocation.getArgument(0), intent, invocation.getArgument(2), 1);
                return null;
              })
          .when(stateWriter)
          .appendFollowUpEvent(anyLong(), any(Intent.class), any(UnifiedRecordValue.class));
      when(scheduler.writers.rejection()).thenReturn(mock(TypedRejectionWriter.class));
      route.set(
          new BrokerDistributionRoute(
              500,
              IntStream.rangeClosed(1, 500)
                  .boxed()
                  .collect(Collectors.toMap(id -> id, ignored -> ENDPOINT))));
      final JobYieldTransport transport =
          new JobYieldTransport() {
            @Override
            public ActorFuture<JobYieldRecord> yieldJobs(
                final BrokerEndpoint endpoint, final JobYieldRecord request) {
              assertTrue(committed, "prepare must commit before external RPC");
              assertEquals(ENDPOINT, endpoint);
              assertEquals(request.getAttempts(), target(request.getYieldId()).attempts());
              requests.add(request.copy());
              return yieldResponse.apply(request);
            }

            @Override
            public ActorFuture<JobYieldRecord> syncWorkerSessions(
                final BrokerEndpoint endpoint, final JobYieldRecord request) {
              assertTrue(committed);
              syncs.add(request.copy());
              return syncResponse.apply(request);
            }

            @Override
            public void close() {}
          };
      processor =
          new JobYieldCommandSender(
              context,
              leader::get,
              ignored -> route.get(),
              () -> transport,
              Duration.ofSeconds(30));
      bScheduler = new JobYieldScheduler(state.getJobYieldState(), leader::get, clock::get);
      progress = new JobYieldUpdateProgressProcessor(context);
      ack = new JobYieldAcknowledgeProcessor(context);
      scheduler.processor =
          event -> {
            process(event.metadata().getIntent(), (JobYieldRecord) event.value());
            commit();
          };
      commit();
      processor.onRecovered(scheduler);
      bScheduler.onRecovered(scheduler);
    }

    private void process(final Intent intent, final JobYieldRecord value) {
      @SuppressWarnings("unchecked")
      final TypedRecord<JobYieldRecord> record = mock(TypedRecord.class);
      when(record.getValue()).thenReturn(value);
      when(record.getIntent()).thenReturn(intent);
      when(record.getValueType()).thenReturn(ValueType.JOB_YIELD);
      when(record.getKey()).thenReturn(value.getYieldId());
      when(record.getTimestamp()).thenReturn(clock.get());
      when(record.getPosition()).thenReturn(POSITIONS.incrementAndGet());
      if (intent == JobYieldIntent.DISPATCH) {
        processor.new JobYieldDispatchProcessor().processRecord(record);
      } else if (intent == JobYieldIntent.SYNC_PARTITION) {
        processor.new JobYieldSyncPartitionProcessor().processRecord(record);
      } else if (intent == JobYieldIntent.UPDATE_PARTITION_SYNC) {
        processor.new JobYieldUpdatePartitionSyncProcessor().processRecord(record);
      } else if (intent == JobYieldIntent.UPDATE_PROGRESS) {
        progress.processRecord(record);
      } else if (intent == JobYieldIntent.ACKNOWLEDGE) {
        ack.processRecord(record);
      }
      bScheduler.onProcessed(record);
    }

    private void seed(final long id, final int partition, final long due) {
      state
          .getJobYieldState()
          .create(
              new JobYieldPlan(
                  id,
                  "demo",
                  "a",
                  "worker",
                  42,
                  JobYieldReason.WORKER_LEASE_EXPIRED,
                  1000,
                  0,
                  null,
                  partition == 0
                      ? List.of()
                      : List.of(
                          new JobYieldPlan.Partition(
                              partition, JobYieldStatus.PENDING, 0, 0, 0, due, 0, "", ""))));
      commit();
      wake();
    }

    private void topologies(final int count) {
      new BrokerClusterCreatedApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                  .setClusterId("demo")
                  .setClusterSize(1)
                  .setPartitionSize(count)
                  .setReplicationFactor(1));
      for (int id = 1; id <= count; id++) {
        state
            .getBrokerPartitionTopologyState()
            .putPartitionTopology(
                new PartitionTopology(
                    new PartitionTopology.Identity("demo", id),
                    new PartitionTopology.Placement(1, List.of("broker"), "broker"),
                    new PartitionTopology.Membership(List.of("broker"), List.of(), Map.of(), 0),
                    PartitionStatus.ACTIVE,
                    Map.of()));
      }
      commit();
      @SuppressWarnings("unchecked")
      final TypedRecord<JobYieldRecord> registration = mock(TypedRecord.class);
      when(registration.getValueType()).thenReturn(ValueType.BROKER_REGISTRATION);
      bScheduler.onProcessed(registration);
    }

    private void wake() {
      process(JobYieldIntent.SESSIONS_SYNCED, new JobYieldRecord());
    }

    private JobYieldPlan.Partition target(final long id) {
      return state.getJobYieldState().plan("demo", id).orElseThrow().partitions().getFirst();
    }

    private void tick(final long millis) {
      clock.addAndGet(millis);
      scheduler.runReady();
    }

    private void commit() {
      try {
        store.getCurrentTransaction().commit();
      } catch (final Exception error) {
        throw new AssertionError(error);
      }
      committed = true;
      store.getCurrentTransaction();
    }

    @Override
    public void close() throws Exception {
      processor.onClose();
      bScheduler.onClose();
      commit();
      storage.close();
    }
  }
}
