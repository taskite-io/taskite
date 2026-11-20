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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService.BrokerClusterCreation;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.placementdriver.broker.registry.BrokerRegistryKey;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.processing.broker.topology.BrokerTopologyCoordinatorChecker;
import io.taskite.placementdriver.processing.broker.topology.TopologyCoordinateProcessor;
import io.taskite.placementdriver.state.appliers.PlacementDriverEventAppliers;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.immutable.BrokerTopologyCoordinatorState.Entry;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord.PartitionStateRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerTopologyCoordinationRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.BrokerIntent;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.state.EventAppliers;
import io.taskite.streamplatform.api.writer.StateWriter;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies planning rules, revision fencing and recovery with durable scheduling. */
class BrokerTopologySchedulingTest {
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(
      strings = {
        "recovery",
        "learner",
        "rebalance",
        "draining",
        "quorum",
        "exclusion",
        "bootstrap",
        "singleton-no-leader",
        "singleton-leader"
      })
  void preservesPlanningRules(final String scenario) throws Exception {
    final var results = script(scenario);
    if (List.of("draining", "quorum", "singleton-no-leader").contains(scenario)) {
      assertTrue(results.isEmpty());
    } else {
      assertEquals(1, results.size());
      assertTrue(results.getFirst().contains(":300000:"));
    }
  }

  private List<String> script(final String scenario) throws Exception {
    try (final var f = new Fixture(directory.resolve(scenario))) {
      f.seed(1);
      if (scenario.equals("recovery")) f.offline("b");
      if (scenario.equals("learner")) {
        f.topology(1, List.of("a", "b", "c"), List.of("d"), false);
        f.offline("d");
      }
      if (scenario.equals("rebalance") || scenario.equals("draining")) {
        f.topology(1, List.of("a", "b"), List.of(), false);
      }
      if (scenario.equals("draining")) {
        f.state.getBrokerRegistrationState().markBrokerDraining("demo", "c", Instant.EPOCH);
      }
      if (scenario.equals("quorum")) {
        f.offline("b");
        f.offline("c");
      }
      if (scenario.equals("exclusion")) {
        f.offline("b");
        f.state.getBrokerRegistrationState().excludeFromPlacement("demo", "d");
      }
      if (scenario.equals("bootstrap")) {
        f.topology(1, List.of("a"), List.of(), true);
      }
      if (scenario.startsWith("singleton-")) {
        f.state
            .getBrokerClusterConfigState()
            .putClusterConfig("demo", new BrokerClusterConfig(1, 1, 1));
        for (final var broker : List.of("b", "c", "d")) {
          f.state.getBrokerRegistrationState().removeBroker("demo", broker);
        }
        f.state
            .getBrokerPartitionTopologyState()
            .putPartitionTopology(
                new PartitionTopology(
                    new PartitionTopology.Identity("demo", 1),
                    new PartitionTopology.Placement(1, List.of("a"), "a"),
                    PartitionTopology.Membership.initial(List.of("a"), List.of(), "a"),
                    PartitionStatus.ACTIVE,
                    Map.of(),
                    true));
        if (scenario.equals("singleton-leader")) {
          f.state
              .getBrokerRegistrationState()
              .renewBroker(
                  "demo",
                  "a",
                  Instant.EPOCH,
                  List.of(new BrokerPartitionState(1, MemberRole.LEADER, 1)));
        }
      }
      f.commit();
      f.wake();
      f.tick(200);
      f.tick(200);
      f.tick(200);
      if (List.of("draining", "quorum", "singleton-no-leader").contains(scenario))
        assertTrue(f.submissions.isEmpty());
      else if (scenario.equals("exclusion")) {
        assertTrue(
            f.submissions.stream()
                .flatMap(plan -> plan.getOperations().stream())
                .noneMatch(
                    op ->
                        op.targetBroker().equals("d")
                            && (op.type() == TopologyOperationType.JOIN.ordinal()
                                || op.type() == TopologyOperationType.PROMOTE_LEARNER.ordinal())));
      } else assertFalse(f.submissions.isEmpty());
      final var canonical =
          f.submissions.stream()
              .map(
                  plan ->
                      plan.getPartitionId()
                          + ":"
                          + plan.getPurpose()
                          + ":"
                          + plan.getSourceTopologyVersion()
                          + ":"
                          + plan.getTargetTopologyVersion()
                          + ":"
                          + (plan.getDeadlineEpochMillis() - plan.getStartedAtEpochMillis())
                          + ":"
                          + plan.getOperations().stream()
                              .map(op -> op.type() + "/" + op.targetBroker() + "/" + op.priority())
                              .toList())
              .toList();
      return canonical;
    }
  }

  @Test
  void bPersistsObservationAndRetriesUnappliedSubmitAfterReopen() throws Exception {
    final var path = directory.resolve("recover");
    long due;
    try (final var f = new Fixture(path)) {
      f.seed(1);
      f.offline("b");
      f.commit();
      f.wake();
      f.tick(200);
      f.tick(200);
      f.tick(200);
      assertEquals(1, f.submissions.size());
      due = f.state.getBrokerTopologyCoordinatorState().getTask("demo", 1).orElseThrow().dueAt();
      for (int i = 0; i < 5; i++) {
        f.state.getBrokerTopologyCoordinatorState().markCluster("demo", f.clock.get());
        f.commit();
        f.wake();
        f.tick(200);
      }
      assertEquals(1, f.submissions.size(), "dirty expansion cannot shorten SUBMIT observation");
    }
    try (final var f = new Fixture(path)) {
      f.tick(200);
      f.tick(200);
      assertTrue(f.submissions.isEmpty());
      f.clock.set(due);
      f.scheduler.runReady();
      assertEquals(1, f.submissions.size());
    }
  }

  @Test
  void bRejectsOldRevisionEvenAfterDeleteAndRecreationAndRollsBackIndexes() throws Exception {
    try (final var f = new Fixture(directory.resolve("revision"))) {
      f.seed(1);
      final var old = f.state.getBrokerTopologyCoordinatorState().getTask("demo", 1).orElseThrow();
      f.state.getBrokerTopologyCoordinatorState().removeTask("demo", 1);
      f.state.getBrokerTopologyCoordinatorState().markPartition("demo", 1, f.clock.get());
      f.commit();
      assertTrue(
          f.state.getBrokerTopologyCoordinatorState().getTask("demo", 1).orElseThrow().revision()
              > old.revision());
      final var event =
          new BrokerTopologyCoordinationRecord()
              .setClusterId("demo")
              .setPartitionId(1)
              .setRevision(old.revision())
              .setFinished(true);
      f.appliers().applyState(0, TopologyIntent.COORDINATED, event, 1);
      assertTrue(f.state.getBrokerTopologyCoordinatorState().getTask("demo", 1).isPresent());
      f.state.getBrokerTopologyCoordinatorState().deleteCluster("demo");
      assertTrue(f.state.getBrokerTopologyCoordinatorState().getTask("demo", 1).isEmpty());
      f.store.getCurrentTransaction().rollback();
      f.store.getCurrentTransaction();
      assertTrue(f.state.getBrokerTopologyCoordinatorState().getTask("demo", 1).isPresent());
      assertFalse(f.state.getBrokerTopologyCoordinatorState().dueBefore(20_000, 100).isEmpty());
    }
  }

  @Test
  void bPendingPlanReleaseReawakensIdlePartitionAndClusterDeleteCleansState() throws Exception {
    try (final var f = new Fixture(directory.resolve("pending"))) {
      f.seed(1);
      f.offline("b");
      final var plan =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "pending", 1),
                  new ChangePlan.Versions(0, 1),
                  List.of(TopologyOperation.leave(1, "b"))),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              new ChangePlan.Deadline(1, Long.MAX_VALUE));
      f.state.getBrokerTopologyChangeState().putChangePlan(plan);
      f.state.getBrokerTopologyChangeState().markPending(plan);
      f.commit();
      f.tick(200);
      f.tick(200);
      f.tick(200);
      assertTrue(f.submissions.isEmpty());
      f.tick(200);
      assertTrue(f.scheduler.tasks.isEmpty());
      f.state.getBrokerTopologyChangeState().removePending("demo", 1);
      f.state.getBrokerTopologyCoordinatorState().markPartition("demo", 1, f.clock.get());
      f.commit();
      // Distribution ACK can finish bootstrap and release the pending plan in its Applier.
      final TypedRecord<?> distributionAck = mock(TypedRecord.class);
      when(distributionAck.getValueType()).thenReturn(ValueType.COMMAND_DISTRIBUTION);
      f.bScheduler.onProcessed(distributionAck);
      f.tick(200);
      assertEquals(1, f.submissions.size());
      f.state.getBrokerPartitionTopologyState().deleteCluster("demo");
      f.state.getBrokerClusterConfigState().delete("demo");
      f.state.getBrokerTopologyCoordinatorState().deleteCluster("demo");
      f.commit();
      assertEquals(-1, f.state.getBrokerTopologyCoordinatorState().nextDueAt());
    }
  }

  @Test
  void bRecoveryCursorSurvivesReopenAndWriterBudgetRejectsWithoutLosingWork() throws Exception {
    final var path = directory.resolve("cursor");
    try (final var f = new Fixture(path)) {
      f.seed(205);
      f.state.getBrokerTopologyCoordinatorState().deleteCluster("demo");
      f.commit();
      f.scheduler.capacity = 1;
      f.scheduler.admission =
          event ->
              !(event.value() instanceof BrokerTopologyCoordinationRecord value)
                  || !value.getClusterId().equals("demo");
      f.tick(200);
      f.tick(200);
      assertEquals(
          100,
          f.state
              .getBrokerTopologyCoordinatorState()
              .getRecoveryScan()
              .orElseThrow()
              .cursorPartition());
      assertEquals(
          100,
          f.state
              .getBrokerTopologyCoordinatorState()
              .getTask("demo", 100)
              .orElseThrow()
              .partitionId());
      assertTrue(f.submissions.isEmpty());
    }
    try (final var f = new Fixture(path)) {
      f.scheduler.capacity = 1;
      for (int tick = 0; tick < 220; tick++) f.tick(100);
      assertEquals(-1, f.state.getBrokerTopologyCoordinatorState().nextDueAt());
      assertTrue(f.scheduler.tasks.isEmpty());
      assertTrue(f.submissions.isEmpty());
    }
  }

  @Test
  void brokerProgressReportsKeepClusterCursorAndLeaseOnlyRenewalDoesNotMark() throws Exception {
    try (final var f = new Fixture(directory.resolve("report-scope"))) {
      f.seed(205);
      final var cluster =
          f.state.getBrokerTopologyCoordinatorState().getTask("demo", 0).orElseThrow();
      f.state
          .getBrokerTopologyCoordinatorState()
          .putTask(new Entry("demo", 0, cluster.revision(), "", 100, 0, false));
      final long partitionRevision =
          f.state.getBrokerTopologyCoordinatorState().getTask("demo", 1).orElseThrow().revision();
      final var report =
          new PartitionStateRecord()
              .setPartitionId(1)
              .setRole(MemberRole.LEADER)
              .setLifecycleState(BrokerPartitionLifecycle.ACTIVE)
              .setTerm(1);
      f.renew(List.of(report), 1000);
      f.commit();
      assertEquals(
          100,
          f.state
              .getBrokerTopologyCoordinatorState()
              .getTask("demo", 0)
              .orElseThrow()
              .cursorPartition());
      assertTrue(
          f.state.getBrokerTopologyCoordinatorState().getTask("demo", 1).orElseThrow().revision()
              > partitionRevision);
      final long updated =
          f.state.getBrokerTopologyCoordinatorState().getTask("demo", 1).orElseThrow().revision();
      f.renew(List.of(report), 2000);
      f.commit();
      assertEquals(
          updated,
          f.state.getBrokerTopologyCoordinatorState().getTask("demo", 1).orElseThrow().revision());
    }
  }

  @Test
  void hotPartitionsReenterAfterWaitingPartitionsWithOneCommandPerTick() throws Exception {
    try (final var f = new Fixture(directory.resolve("fairness"))) {
      f.seed(10);
      f.scheduler.capacity = 1;
      final var deferred =
          f.state.getBrokerTopologyCoordinatorState().getTask("demo", 10).orElseThrow();
      f.state
          .getBrokerTopologyCoordinatorState()
          .putTask(new Entry("demo", 10, deferred.revision(), "", 0, 10_500, true));
      f.commit();
      for (int tick = 0; tick < 25; tick++) {
        f.tick(100);
        if (f.clock.get() < 10_500) {
          assertFalse(f.coordinations.contains(10), "observation must not run before its deadline");
        }
        for (int partition = 1; partition <= 8; partition++) {
          f.state.getBrokerTopologyCoordinatorState().markPartition("demo", partition);
        }
        f.commit();
        f.wake();
      }
      assertTrue(f.coordinations.contains(9), "partition 9 must receive coordination");
      assertTrue(f.coordinations.contains(10), "partition 10 must receive coordination");
    }
  }

  @Test
  void fullHeartbeatsUpdateProgressWithoutDirtyAndMissingReportsStillTriggerCoordination()
      throws Exception {
    try (final var f = new Fixture(directory.resolve("heartbeat-boundary"))) {
      f.seed(1);
      final var report =
          new PartitionStateRecord()
              .setPartitionId(1)
              .setRole(MemberRole.LEADER)
              .setLifecycleState(BrokerPartitionLifecycle.ACTIVE)
              .setPriority(100)
              .setTerm(1);
      f.renew(List.of(report), 11_000);
      f.state.getBrokerTopologyCoordinatorState().deleteCluster("demo");
      f.commit();
      report
          .setTerm(2)
          .setMatchIndex(999)
          .setLag(0)
          .setReadyForPromotion(true)
          .setLastOpId("receipt")
          .setLastOpStatus(BrokerTopologyOperationStatus.APPLIED);
      f.renew(List.of(report), 12_000);
      f.commit();
      assertTrue(f.state.getBrokerTopologyCoordinatorState().dueBefore(20_000, 100).isEmpty());
      final var observed =
          f.state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .replicaStates()
              .get("a");
      assertEquals(
          2,
          f.state
              .getBrokerRegistrationState()
              .registration(new BrokerRegistryKey("demo", "a"))
              .orElseThrow()
              .partitions()
              .getFirst()
              .term());
      assertEquals(999, observed.matchIndex());
      assertEquals(0, observed.lag());
      assertTrue(observed.readyForPromotion());
      assertEquals("receipt", observed.lastOpId());
      assertEquals(12_000, observed.lastReportTimeEpochMillis());
      f.renew(List.of(), 13_000);
      assertFalse(
          f.state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .replicaStates()
              .containsKey("a"));
      assertTrue(f.state.getBrokerTopologyCoordinatorState().getTask("demo", 1).isPresent());
      assertTrue(f.state.getBrokerTopologyCoordinatorState().getTask("demo", 0).isEmpty());
    }
  }

  @Test
  void offlineEventReplacesVoterAndAutomaticallyPlansRecovery() throws Exception {
    try (final var f = new Fixture(directory.resolve("offline-event"))) {
      f.seed(1);
      f.state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(3, 1, 3, List.of("a", "b", "c"), 1));
      f.state.getBrokerTopologyCoordinatorState().deleteCluster("demo");
      f.appliers()
          .applyState(
              0,
              BrokerIntent.MARKED_OFFLINE,
              new BrokerRegistrationRecord()
                  .setClusterId("demo")
                  .setBrokerId("b")
                  .setEventTimestampEpochMillis(10_000),
              1);
      f.commit();
      assertEquals(
          List.of("a", "d", "c"),
          f.state.getBrokerClusterConfigState().clusterConfig("demo").voterRoster());
      f.wake();
      for (int tick = 0; tick < 4; tick++) {
        f.tick(200);
      }
      assertEquals(1, f.submissions.size());
      assertTrue(
          f.submissions.getFirst().getOperations().stream()
              .anyMatch(
                  operation ->
                      operation.targetBroker().equals("b")
                          && operation.type() == TopologyOperationType.LEAVE.ordinal()));
    }
  }

  @Test
  void preservedHeartbeatKeepsReceiptsButNewIncarnationInvalidatesThem() throws Exception {
    try (final var f = new Fixture(directory.resolve("incarnation"))) {
      f.seed(1);
      f.renew(
          List.of(
              new PartitionStateRecord()
                  .setPartitionId(1)
                  .setRole(MemberRole.LEADER)
                  .setLastOpId("old-receipt")
                  .setLastOpStatus(BrokerTopologyOperationStatus.APPLIED)),
          11_000);
      f.state.getBrokerTopologyCoordinatorState().deleteCluster("demo");
      f.appliers()
          .applyState(
              0,
              BrokerIntent.RENEWED,
              new BrokerRegistrationRecord()
                  .setClusterId("demo")
                  .setBrokerId("a")
                  .setEventTimestampEpochMillis(12_000)
                  .setPreservePartitionObservations(true),
              1);
      assertEquals(
          "old-receipt",
          f.state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .replicaStates()
              .get("a")
              .lastOpId());
      assertEquals(-1, f.state.getBrokerTopologyCoordinatorState().nextDueAt());
      f.appliers()
          .applyState(
              0,
              BrokerIntent.REGISTERED,
              new BrokerRegistrationRecord()
                  .setClusterId("demo")
                  .setBrokerId("a")
                  .setHost("127.0.0.1")
                  .setPort(26501)
                  .setRaftPort(26502)
                  .setIncarnationId("a-restarted")
                  .setEventTimestampEpochMillis(13_000),
              1);
      assertFalse(
          f.state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .replicaStates()
              .containsKey("a"));
      assertTrue(f.state.getBrokerTopologyCoordinatorState().getTask("demo", 0).isPresent());
      assertTrue(f.state.getBrokerTopologyCoordinatorState().getTask("demo", 1).isPresent());
    }
  }

  @Test
  void reentryUsesPersistedQueueTimeAndKeepsObservationDeadline() throws Exception {
    final var path = directory.resolve("queue-time");
    try (final var f = new Fixture(path)) {
      final var queue = f.state.getBrokerTopologyCoordinatorState();
      queue.markPartition("demo", 1);
      final var first = queue.getTask("demo", 1).orElseThrow();
      f.appliers()
          .applyState(
              0,
              TopologyIntent.COORDINATED,
              new BrokerTopologyCoordinationRecord()
                  .setClusterId("demo")
                  .setPartitionId(1)
                  .setRevision(first.revision())
                  .setCoordinatedAt(9000)
                  .setFinished(true),
              1);
      queue.markPartition("demo", 1, 1);
      assertEquals(9000, queue.getTask("demo", 1).orElseThrow().dueAt());
      final var next = queue.getTask("demo", 1).orElseThrow();
      queue.putTask(new Entry("demo", 1, next.revision(), "", 0, 25_000, true));
      queue.advanceTime(10_000);
      queue.markPartition("demo", 1, 2);
      assertEquals(25_000, queue.getTask("demo", 1).orElseThrow().dueAt());
      f.commit();
    }
    try (final var f = new Fixture(path)) {
      f.state.getBrokerTopologyCoordinatorState().markPartition("demo", 2, 3);
      assertEquals(
          10_000,
          f.state.getBrokerTopologyCoordinatorState().getTask("demo", 2).orElseThrow().dueAt());
    }
  }

  @Test
  void bLeaderFencingAndUncollectedRecoverySeedRemainRetryable() throws Exception {
    try (final var f = new Fixture(directory.resolve("leader"))) {
      f.seed(1);
      f.offline("b");
      f.commit();
      f.scheduler.admission = ignored -> false;
      f.tick(200);
      assertTrue(f.submissions.isEmpty());
      f.scheduler.admission = ignored -> true;
      f.tick(200);
      f.leader.set(false);
      f.tick(200);
      assertTrue(f.submissions.isEmpty());
      f.leader.set(true);
      f.bScheduler.onRecovered(f.scheduler);
      f.tick(200);
      f.tick(200);
      f.tick(200);
      assertEquals(1, f.submissions.size());
    }
  }

  @Test
  void acceptedSubmitReleasesReservationBeforeQueuedPlanCanExecute() throws Exception {
    try (final var f = new Fixture(directory.resolve("accepted"))) {
      f.seed(1);
      f.offline("b");
      f.commit();
      for (int i = 0; i < 3; i++) f.tick(200);
      assertEquals(
          1, f.state.getBrokerTopologyCoordinatorState().reservedPartitions(f.clock.get(), 256));
      final var accepted =
          BrokerPartitionChangeRecordMapper.toPlan(
              f.submissions.getFirst(), BrokerPartitionChangeStatus.QUEUED);
      f.appliers()
          .applyState(0, BrokerPartitionChangeIntent.SUBMITTED, f.submissions.getFirst(), 1);
      f.commit();
      assertEquals(
          0, f.state.getBrokerTopologyCoordinatorState().reservedPartitions(f.clock.get(), 256));
      assertTrue(f.state.getBrokerTopologyChangeState().ownsPendingPlan(accepted));
      f.wake();
      f.tick(200);
      f.tick(200);
      assertEquals(1, f.submissions.size());
      assertTrue(f.scheduler.tasks.isEmpty());
    }
  }

  @Test
  void bCapsOutstandingSubmissionsAt256AndPreservesRemainingDirtyWork() throws Exception {
    try (final var f = new Fixture(directory.resolve("cap"))) {
      f.seed(270);
      f.offline("b");
      f.commit();
      f.wake();
      for (int i = 0; i < 10; i++) f.tick(100);
      assertEquals(256, f.submissions.size());
      assertEquals(
          256, f.state.getBrokerTopologyCoordinatorState().reservedPartitions(f.clock.get(), 256));
      assertTrue(f.state.getBrokerTopologyCoordinatorState().getTask("demo", 270).isPresent());
      f.state.getBrokerTopologyCoordinatorState().removeTask("demo", 1);
      f.commit();
      f.tick(100);
      assertEquals(
          257, f.submissions.size(), "a released slot must be reused by a deferred partition");
    }
  }

  @Test
  void repeatedRestartsRetainSubmissionObservationAndPlanningRules() throws Exception {
    final var path = directory.resolve("restart");
    long due;
    try (final var f = new Fixture(path)) {
      f.seed(1);
      f.offline("b");
      f.commit();
      f.wake();
      f.tick(200);
      f.tick(200);
      f.tick(200);
      assertEquals(1, f.submissions.size());
      due = f.state.getBrokerTopologyCoordinatorState().getTask("demo", 1).orElseThrow().dueAt();
    }
    try (final var f = new Fixture(path)) {
      f.tick(200);
      f.tick(200);
      f.tick(200);
      assertTrue(f.submissions.isEmpty());
      f.clock.set(due);
      f.scheduler.runReady();
      assertEquals(1, f.submissions.size());
      due = f.state.getBrokerTopologyCoordinatorState().getTask("demo", 1).orElseThrow().dueAt();
    }
    try (final var f = new Fixture(path)) {
      f.tick(200);
      f.tick(200);
      f.tick(200);
      assertTrue(f.submissions.isEmpty());
      f.clock.set(due);
      f.scheduler.runReady();
      assertEquals(1, f.submissions.size());
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final PartitionStorage storage;
    private final StateStoreProcessingStore store;
    private final MutableProcessingState state;
    private final AtomicLong clock = new AtomicLong(10_000);
    private final AtomicBoolean leader = new AtomicBoolean(true);
    private final WorkflowScheduler scheduler = new WorkflowScheduler(clock);
    private final List<PartitionChangeRecord> submissions = new ArrayList<>();
    private final List<Integer> coordinations = new ArrayList<>();
    private final BrokerTopologyCoordinatorChecker bScheduler;
    private final TopologyCoordinateProcessor processor;

    private Fixture(final Path path) {
      storage = new PartitionStorage(path);
      store = new StateStoreProcessingStore(storage);
      state = new MutableProcessingState(store);
      clock.set(Math.max(clock.get(), state.getBrokerTopologyCoordinatorState().getQueueTime()));
      final var context = mock(TypedRecordProcessorContext.class);
      when(context.getProcessingState(ProcessingState.class)).thenReturn(state);
      when(context.getWriters()).thenReturn(scheduler.writers);
      final var writer = mock(StateWriter.class);
      when(scheduler.writers.state()).thenReturn(writer);
      doAnswer(
              invocation -> {
                appliers()
                    .applyState(
                        invocation.getArgument(0),
                        invocation.getArgument(1),
                        invocation.getArgument(2),
                        1);
                return null;
              })
          .when(writer)
          .appendFollowUpEvent(anyLong(), any(Intent.class), any(UnifiedRecordValue.class));
      final var planning = new BrokerClusterChangePlanningService();
      processor =
          new TopologyCoordinateProcessor(context, leader::get, planning, Duration.ofMinutes(5));
      bScheduler =
          new BrokerTopologyCoordinatorChecker(
              state.getBrokerTopologyCoordinatorState(), leader::get, clock::get);
      scheduler.processor =
          event -> {
            if (event.metadata().getIntent() == TopologyIntent.COORDINATE) {
              @SuppressWarnings("unchecked")
              final TypedRecord<BrokerTopologyCoordinationRecord> record = mock(TypedRecord.class);
              when(record.getValue()).thenReturn((BrokerTopologyCoordinationRecord) event.value());
              final var coordination = (BrokerTopologyCoordinationRecord) event.value();
              if (coordination.getPartitionId() > 0) {
                coordinations.add(coordination.getPartitionId());
              }
              when(record.getTimestamp()).thenReturn(clock.get());
              when(record.getValueType()).thenReturn(ValueType.TOPOLOGY);
              processor.processRecord(record);
              commit();
              bScheduler.onProcessed(record);
            } else if (event.metadata().getIntent() == BrokerPartitionChangeIntent.SUBMIT) {
              submissions.add((PartitionChangeRecord) event.value());
            }
          };
      commit();
      bScheduler.onRecovered(scheduler);
    }

    private EventAppliers appliers() {
      return PlacementDriverEventAppliers.create(state);
    }

    private void seed(final int count) {
      state.getBrokerClusterState().remember("demo", 1);
      state
          .getBrokerClusterConfigState()
          .putClusterConfig(
              "demo", new BrokerClusterConfig(3, count, 3, List.of("a", "b", "c"), 1));
      for (final String broker : List.of("a", "b", "c", "d")) {
        state
            .getBrokerRegistrationState()
            .registerBroker(
                "demo",
                broker,
                "127.0.0.1",
                26501,
                26502,
                "test",
                30_000,
                Instant.EPOCH,
                List.of(),
                broker);
      }
      final var planning = new BrokerClusterChangePlanningService();
      final var registrations = state.getBrokerRegistrationState().registrations("demo");
      final var config = state.getBrokerClusterConfigState().clusterConfig("demo");
      final var voters = planning.calculateVoterBrokers(config, registrations);
      for (int partition = 1; partition <= count; partition++) {
        state
            .getBrokerPartitionTopologyState()
            .putPartitionTopology(
                planning
                    .calculateClusterCreation(
                        new BrokerClusterCreation("demo", config, partition, registrations, voters))
                    .topology()
                    .withInitializing(false));
        state.getBrokerTopologyCoordinatorState().markPartition("demo", partition, clock.get());
      }
      state.getBrokerTopologyCoordinatorState().markCluster("demo", clock.get());
      commit();
      wake();
    }

    private void topology(
        final int partition,
        final List<String> voters,
        final List<String> learners,
        final boolean initializing) {
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", partition),
                  new PartitionTopology.Placement(3, List.of("a", "b", "c"), "a"),
                  PartitionTopology.Membership.initial(voters, learners, "a"),
                  PartitionStatus.ACTIVE,
                  Map.of(),
                  initializing));
    }

    private void offline(final String broker) {
      state.getBrokerRegistrationState().markBrokerOffline("demo", broker, Instant.EPOCH);
    }

    private void renew(final List<PartitionStateRecord> reports, final long timestamp) {
      appliers()
          .applyState(
              0,
              BrokerIntent.RENEWED,
              new BrokerRegistrationRecord()
                  .setClusterId("demo")
                  .setBrokerId("a")
                  .setIncarnationId("a")
                  .setEventTimestampEpochMillis(timestamp)
                  .setPartitions(reports),
              1);
    }

    private void wake() {
      final TypedRecord<?> record = mock(TypedRecord.class);
      when(record.getValueType()).thenReturn(ValueType.TOPOLOGY);
      bScheduler.onProcessed(record);
    }

    private void tick(final long millis) {
      clock.addAndGet(millis);
      scheduler.runReady();
    }

    private void commit() {
      try {
        store.getCurrentTransaction().commit();
        store.getCurrentTransaction();
      } catch (final Exception error) {
        throw new AssertionError(error);
      }
    }

    @Override
    public void close() throws Exception {
      bScheduler.onClose();
      commit();
      storage.close();
    }
  }
}
