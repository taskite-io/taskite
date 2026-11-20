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
package io.taskite.placementdriver.processing.broker.cluster;

import static io.taskite.protocol.record.TopologyOperationType.JOIN;
import static io.taskite.protocol.record.TopologyOperationType.PROMOTE_LEARNER;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.taskite.placementdriver.PartitionOperationFixture;
import io.taskite.placementdriver.WorkflowScheduler;
import io.taskite.placementdriver.broker.cluster.*;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.placementdriver.broker.request.*;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.broker.topology.ReplicaState;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.processing.topology.PartitionExecuteProcessor;
import io.taskite.placementdriver.processing.topology.TopologyMembershipChangeChecker;
import io.taskite.placementdriver.processing.topology.TopologyMembershipChangeExecutor;
import io.taskite.placementdriver.processing.topology.TopologyMembershipChangeProcessors;
import io.taskite.placementdriver.processing.topology.TopologyPartitionChangeBehavior;
import io.taskite.placementdriver.state.appliers.*;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.broker.BrokerPartitionConfigurationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionExecutionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerClusterChangeStatusRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.record.*;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeStatus;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.scheduler.future.*;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;
import io.taskite.streamplatform.api.state.*;
import io.taskite.streamplatform.api.writer.*;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.topology.*;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RaftNodeState;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

final class BrokerConsecutiveFailuresTest {
  @TempDir Path directory;

  @ParameterizedTest
  @CsvSource({"false,false", "true,false", "true,true"})
  void successorRetainsReachableRootPrioritiesAcrossRepeatedRecovery(
      final boolean standby, final boolean returnedLearner) throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      if (returnedLearner) {
        final var registrations = fixture.state.getBrokerRegistrationState();
        final var previous =
            registrations.registrations("demo").stream()
                .filter(registration -> registration.brokerId().equals("broker-4"))
                .findFirst()
                .orElseThrow();
        registrations.excludeFromPlacement("demo", "broker-4");
        registrations.removeBroker("demo", "broker-4");
        registrations.registerBroker(
            "demo",
            "broker-4",
            previous.host(),
            previous.port(),
            previous.raftPort(),
            previous.version(),
            30_000,
            Instant.now(),
            List.of(),
            "restart-4");
        assertTrue(registrations.isVoterExcluded("demo", "broker-4"));
        assertFalse(registrations.isLearnerExcluded("demo", "broker-4", "restart-4"));
        assertEquals(
            PlanBlockedReason.TARGET_UNAVAILABLE,
            BrokerPlanDependencies.invalidation(fixture.state, fixture.old, 100));
      }
      final var goalMembers =
          configuration(
                  List.of("broker-0", "broker-1", "broker-2"),
                  standby ? List.of("broker-4") : List.of(),
                  0,
                  "",
                  0)
              .members()
              .stream()
              .map(
                  member ->
                      new BrokerPartitionMember(
                          member.brokerId(),
                          member.voter(),
                          member.voter() ? member.brokerId().equals("broker-1") ? 100 : 10 : 0))
              .toList();
      final var transfer = TopologyOperation.transferLeader(1, "broker-1");
      final var root =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "goal", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(
                      new TopologyOperation(
                          transfer.opId(),
                          transfer.type(),
                          transfer.targetBroker(),
                          transfer.priority(),
                          new BrokerOperationConfiguration(
                              fixture.transport.configuration.members(),
                              goalMembers,
                              8,
                              new RecoveryFence(0, 0, ""))))),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.SUPERSEDING),
              new ChangePlan.Deadline(1, Long.MAX_VALUE));
      fixture.state.getBrokerTopologyChangeState().putChangePlan(root);
      // The intermediate recovery ended with broker-0 primary. The persistent goal still wins.
      final var previous =
          fixture.old.withExecution(
              new ChangePlan.Execution(1, 9, "goal", "next", "goal", PlanBlockedReason.NONE, 0));
      for (final var predecessor :
          List.of(
              root.withExecution(
                  new ChangePlan.Execution(1, 9, "", "next", "goal", PlanBlockedReason.NONE, 0)),
              previous)) {
        final var successor =
            BrokerPartitionSuccessorPlanner.plan(
                fixture.state,
                predecessor,
                configuration(
                    List.of("broker-0", "broker-1", "broker-2"),
                    standby ? List.of("broker-4") : List.of(),
                    2,
                    "next",
                    12));
        final var actual = successor.operations().getLast().configuration().targetMembers();
        assertEquals(
            Set.copyOf(root.operations().getLast().configuration().targetMembers()),
            Set.copyOf(actual));
        assertTrue(
            successor.operations().stream()
                .anyMatch(
                    operation ->
                        operation.type() == TopologyOperationType.TRANSFER_LEADER
                            && operation.targetBroker().equals("broker-1")));
      }
    }
  }

  @Test
  void stoppedFirstAddBootstrapsOriginalTargetBeforeRaftReconciliation() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      final var bootstrap = fixture.uninitializedStoppedPlan();
      assertEquals(ChangePlan.Purpose.BOOTSTRAP, bootstrap.purpose());
      fixture.scan();
      assertEquals(1, fixture.transport.dispatches);
      assertEquals(bootstrap.operations().getFirst().opId(), fixture.transport.request.opId());
      assertEquals("broker-0", fixture.transport.request.targetBroker());
      assertEquals(0, fixture.transport.request.authority().generation());
      assertEquals(
          bootstrap.operations().getFirst().configuration().targetMembers(),
          fixture.transport.request.members());
      assertEquals(0, fixture.transport.reads);
      assertEquals(0, fixture.transport.installs);
      assertEquals(BrokerPartitionChangeStatus.SUPERSEDING, fixture.pending().status());
      fixture.transport.configuration = configuration(List.of("broker-0"), List.of(), 0, "", 0);
      fixture.transport.operation.complete(
          new BrokerClusterChangeDispatchResult(fixture.transport.request.opId(), true));
      fixture.scheduling.runReady();
      assertTrue(fixture.projectedLeader().isEmpty());
      assertEquals("broker-0", fixture.transport.readEndpoint.brokerId());
      assertEquals(1, fixture.transport.reads);
      assertEquals(1, fixture.transport.installs);
      final var successor = fixture.pending();
      assertEquals("replacement", successor.changeId());
      assertEquals(ChangePlan.Purpose.BOOTSTRAP, successor.purpose());
      assertTrue(
          fixture
              .state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .initializing());
      assertEquals("old", successor.rootChangeId());
      assertEquals(1, successor.execution().generation());
      assertEquals(
          BrokerPartitionChangeStatus.SUPERSEDED,
          fixture
              .state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "old")
              .orElseThrow()
              .status());
      assertTrue(
          successor.operations().stream()
              .noneMatch(
                  operation ->
                      operation.type() == TopologyOperationType.ADD
                          || operation.type() == TopologyOperationType.RESET_PEERS));
      assertEquals(
          PartitionStatus.CREATING,
          fixture
              .state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .status());
    }
  }

  @Test
  void provisionalBootstrapLeaderCanReconcileWithoutBusinessReadiness() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.uninitializedStoppedPlan();
      fixture.transport.configuration = configuration(List.of("broker-0"), List.of(), 0, "", 0);
      fixture.bootstrapLeader(false);
      fixture.scan();

      assertTrue(fixture.projectedLeader().isEmpty());
      assertEquals(0, fixture.transport.dispatches);
      assertEquals("broker-0", fixture.transport.readEndpoint.brokerId());
      assertEquals(1, fixture.transport.reads);
      assertEquals(1, fixture.transport.installs);
      assertEquals("replacement", fixture.pending().changeId());
    }
  }

  @Test
  void provisionalBootstrapLeaderRecoversLostAddResponse() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.uninitializedStoppedPlan();
      fixture.scan();
      fixture.transport.operation.completeExceptionally(new IllegalStateException("response lost"));
      fixture.scheduling.runReady();
      assertEquals(
          PlanBlockedReason.BROKER_UNAVAILABLE, fixture.pending().execution().blockedReason());
      fixture.transport.configuration = configuration(List.of("broker-0"), List.of(), 0, "", 0);
      fixture.bootstrapLeader(false);
      fixture.scan();

      assertEquals(1, fixture.transport.dispatches);
      assertEquals(1, fixture.transport.reads);
      assertEquals(1, fixture.transport.installs);
      assertEquals("replacement", fixture.pending().changeId());
    }
  }

  @Test
  void provisionalBootstrapLeaderRecoversLostInstallResponseWithoutReinstalling() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.uninitializedStoppedPlan();
      fixture.transport.configuration = configuration(List.of("broker-0"), List.of(), 0, "", 0);
      fixture.bootstrapLeader(false);
      fixture.transport.loseInstallResponse = true;
      fixture.scan();
      assertEquals(
          PlanBlockedReason.AUTHORITY_INSTALL_UNAVAILABLE,
          fixture.pending().execution().blockedReason());
      assertEquals(1, fixture.transport.configuration.executionGeneration());
      fixture.scan();

      assertEquals(0, fixture.transport.dispatches);
      assertEquals(2, fixture.transport.reads);
      assertEquals(1, fixture.transport.installs);
      assertEquals("replacement", fixture.pending().changeId());
      assertEquals(1, fixture.pending().execution().generation());
    }
  }

  @Test
  void provisionalBootstrapLeaderCannotReplaceAnActualConfigurationWithoutQuorum()
      throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.uninitializedStoppedPlan();
      fixture.transport.configuration =
          configuration(List.of("broker-0", "broker-1", "broker-2"), List.of(), 0, "", 0);
      fixture.bootstrapLeader(false);
      fixture.offline("broker-1");
      fixture.offline("broker-2");
      fixture.scan();

      assertEquals(0, fixture.transport.dispatches);
      assertEquals(1, fixture.transport.reads);
      assertEquals(0, fixture.transport.installs);
      assertEquals(PlanBlockedReason.NO_QUORUM, fixture.pending().execution().blockedReason());
      assertEquals("old", fixture.pending().changeId());
    }
  }

  @Test
  void staleBootstrapLeaderReportCannotSkipFrozenAdd() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.uninitializedStoppedPlan();
      fixture.transport.configuration = configuration(List.of("broker-0"), List.of(), 0, "", 0);
      fixture.bootstrapLeader(true);
      fixture.scan();

      assertEquals(1, fixture.transport.dispatches);
      assertEquals(0, fixture.transport.reads);
      assertEquals(0, fixture.transport.installs);
      assertEquals("old", fixture.pending().changeId());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void stoppedFirstAddWaitsForOriginalTargetAndDoesNotRedirect(final boolean excluded)
      throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.uninitializedStoppedPlan();
      if (excluded) {
        fixture.state.getBrokerRegistrationState().excludeFromPlacement("demo", "broker-0");
      } else {
        fixture.offline("broker-0");
      }
      fixture.scan();
      assertEquals(0, fixture.transport.dispatches);
      assertEquals(0, fixture.transport.installs);
      assertEquals(
          PlanBlockedReason.TARGET_UNAVAILABLE, fixture.pending().execution().blockedReason());
    }
  }

  @Test
  void stoppedFirstAddWaitsForReplicationCapacity() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.uninitializedStoppedPlan();
      for (final var id : List.of("broker-1", "broker-2", "broker-3", "broker-4"))
        fixture.offline(id);
      fixture.scan();
      assertEquals(0, fixture.transport.dispatches);
      assertEquals(
          PlanBlockedReason.INSUFFICIENT_CAPACITY, fixture.pending().execution().blockedReason());
    }
  }

  @Test
  void stoppedFirstAddCallbackCannotAdvanceAfterPdLeadershipChanges() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.uninitializedStoppedPlan();
      fixture.scan();
      assertEquals(1, fixture.transport.dispatches);
      fixture.executor.onClose();
      fixture.transport.operation.complete(
          new BrokerClusterChangeDispatchResult(fixture.transport.request.opId(), true));
      assertTrue(fixture.commands.isEmpty());
      assertEquals(BrokerPartitionChangeStatus.SUPERSEDING, fixture.pending().status());
    }
  }

  @Test
  void committedMembershipCannotUseFirstAddRecoveryWithoutQuorum() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.uninitializedStoppedPlan();
      final var empty =
          fixture
              .state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow();
      fixture
          .state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  empty.identity(),
                  empty.placement(),
                  PartitionTopology.Membership.initial(
                      List.of("broker-0", "broker-1", "broker-2"), List.of(), "broker-0"),
                  PartitionStatus.CREATING,
                  Map.of()));
      fixture.offline("broker-1");
      fixture.offline("broker-2");
      fixture.scan();
      assertEquals(0, fixture.transport.dispatches);
      assertEquals(0, fixture.transport.installs);
      assertEquals(PlanBlockedReason.NO_QUORUM, fixture.pending().execution().blockedReason());
    }
  }

  @Test
  void successorCannotBecomeActiveBeforeOutstandingResourceAcknowledgement() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      final var creating =
          fixture
              .state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow();
      fixture
          .state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  creating.identity(),
                  creating.placement(),
                  creating.membership(),
                  PartitionStatus.CREATING,
                  creating.replicaStates()));
      final var distribution =
          new CommandDistributionRecord<JobSubscriptionRecord>()
              .setDistributionKey(51)
              .setClusterId("demo")
              .setPartitionId(1)
              .setValueType(ValueType.JOB_SUBSCRIPTION)
              .setIntent(JobSubscriptionIntent.CREATE)
              .setValue(
                  new JobSubscriptionRecord()
                      .setClusterId("demo")
                      .setTenantId("tenant")
                      .setName("charge")
                      .setJobType("process")
                      .setVariables("<xml/>"));
      fixture.state.getCommandDistributionState().putDistribution(distribution);
      fixture.state.getCommandDistributionState().putTarget(51, "demo", 1);
      fixture.state.getCommandDistributionState().markPending(51, "demo", 1);
      fixture.offline("broker-4");
      fixture.scan();
      fixture.scan();
      ChangePlan current = fixture.admit(fixture.pending());
      while (current.hasMoreOperations()) {
        final int previousIndex = current.nextOperationIndex();
        PartitionOperationFixture.applyOperation(fixture.state, 50, current);
        current = fixture.pending();
        assertEquals(previousIndex + 1, current.nextOperationIndex());
      }
      assertEquals(BrokerPartitionChangeStatus.IN_PROGRESS, current.status());
      assertEquals(
          PartitionStatus.CREATING,
          fixture
              .state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .status());
      new CommandDistributionAcknowledgedApplier(fixture.state).applyState(51, distribution);
      assertTrue(fixture.state.getBrokerTopologyChangeState().pendingChangePlans("demo").isEmpty());
      assertEquals(
          BrokerPartitionChangeStatus.COMPLETED,
          fixture
              .state
              .getBrokerTopologyChangeState()
              .changePlan("demo", current.changeId())
              .orElseThrow()
              .status());
      assertEquals(
          PartitionStatus.ACTIVE,
          fixture
              .state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .status());
    }
  }

  @Test
  void exhaustedOperationsStillStopAnOfflineTargetBeforeResourceDeadline() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      final var exhausted =
          new ChangePlan(
              fixture.old.definition(),
              new ChangePlan.Progress(
                  fixture.old.operations().size(), BrokerPartitionChangeStatus.IN_PROGRESS),
              new ChangePlan.Deadline(1, 2),
              fixture.old.execution());
      fixture.state.getBrokerTopologyChangeState().putChangePlan(exhausted);
      fixture.offline("broker-4");
      fixture.scan();
      assertEquals(BrokerPartitionChangeStatus.SUPERSEDING, fixture.pending().status());
      assertEquals(
          PlanBlockedReason.TARGET_UNAVAILABLE, fixture.pending().execution().blockedReason());
    }
  }

  @Test
  void drainingTargetIsReplacedWhileDrainingExistingVotersStillContributeToQuorum()
      throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture
          .state
          .getBrokerRegistrationState()
          .markBrokerDraining("demo", "broker-4", Instant.now());
      assertEquals(
          PlanBlockedReason.TARGET_UNAVAILABLE,
          BrokerPlanDependencies.invalidation(fixture.state, fixture.old, 100));
      fixture.scan();
      fixture.scan();
      assertEquals(
          BrokerPartitionChangeStatus.SUPERSEDED,
          fixture
              .state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "old")
              .orElseThrow()
              .status());
      assertTrue(
          fixture.pending().operations().stream()
              .anyMatch(
                  operation ->
                      operation.type() == TopologyOperationType.LEAVE
                          && operation.targetBroker().equals("broker-4")));
      fixture
          .state
          .getBrokerRegistrationState()
          .markBrokerDraining("demo", "broker-1", Instant.now());
      fixture.offline("broker-2");
      assertTrue(BrokerPlanDependencies.hasQuorum(fixture.state, fixture.pending()));
    }
  }

  @Test
  void offlineLeaveTargetDoesNotInvalidateThePlan() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      final var leaving =
          BrokerTopologyOperationRequests.freeze(
              fixture.state,
              new ChangePlan(
                  new ChangePlan.Definition(
                      new ChangePlan.Identity("demo", "leave", 1),
                      new ChangePlan.Versions(1, 2),
                      List.of(TopologyOperation.leave(1, "broker-4"))),
                  new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
                  new ChangePlan.Deadline(1, Long.MAX_VALUE)));
      fixture.offline("broker-4");
      assertEquals(
          PlanBlockedReason.NONE, BrokerPlanDependencies.invalidation(fixture.state, leaving, 100));
    }
  }

  @Test
  void queryRetainsOriginalGoalAndReportsCurrentSuccessorAndHistory() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.offline("broker-4");
      fixture.scan();
      fixture.scan();
      final var successor = fixture.pending();
      final var query =
          (BrokerClusterChangeStatusRecord)
              new BrokerClusterChangeQueryBehavior(fixture.state)
                  .createResponseValue(
                      new TopologyQueryRecord().setClusterId("demo").setChangeId("old"));
      assertFalse(query.isTerminal());
      assertEquals(successor.changeId(), query.getPartitionChanges().getFirst().changeId());
      assertEquals(2, query.getPartitionChanges().getFirst().executionGeneration());
      assertEquals(
          List.of("old", successor.changeId()),
          query.getExecutionHistory().stream().map(item -> item.changeId()).toList());
      assertEquals(ChangeStatus.SUPERSEDED, query.getExecutionHistory().getFirst().status());
      assertTrue(
          query.getPlannedChanges().stream()
              .anyMatch(
                  operation ->
                      operation.type() == PROMOTE_LEARNER
                          && operation.targetBroker().equals("broker-4")));
      assertTrue(query.getExpectedTopology().getFirst().currentVoters().contains("broker-4"));
    }
  }

  @Test
  void parentWaitsForOriginalGoalAndCompletesAfterLaterActualRestoration() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.parent();
      fixture.offline("broker-4");
      fixture.scan();
      fixture.scan();
      fixture.complete(fixture.pending());
      assertEquals(
          BrokerClusterChangeStatus.WAITING_READY,
          fixture
              .state
              .getBrokerTopologyChangeState()
              .clusterChangeOperation("demo", "old")
              .orElseThrow()
              .status());
      final var restore =
          BrokerTopologyOperationRequests.freeze(
              fixture.state,
              new ChangePlan(
                  new ChangePlan.Definition(
                      new ChangePlan.Identity("demo", "restore", 1),
                      new ChangePlan.Versions(2, 3),
                      List.of(
                          TopologyOperation.join(1, "broker-4"),
                          TopologyOperation.promoteLearner(1, "broker-4"),
                          TopologyOperation.leave(1, "broker-0"))),
                  new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
                  new ChangePlan.Deadline(1, Long.MAX_VALUE)));
      new PartitionChangeSubmittedApplier(fixture.state)
          .applyState(44, BrokerPartitionChangeRecordMapper.fromPlan(restore));
      fixture.complete(restore);
      assertEquals(
          BrokerClusterChangeStatus.COMPLETED,
          fixture
              .state
              .getBrokerTopologyChangeState()
              .clusterChangeOperation("demo", "old")
              .orElseThrow()
              .status());
    }
  }

  @Test
  void cancellingParentRetainsItsStoppedRecoveryReservationAndDoesNotReviveParent()
      throws Exception {
    for (final boolean afterReplacement : List.of(false, true)) {
      try (final Fixture fixture = new Fixture(directory)) {
        fixture.parent();
        fixture.offline("broker-4");
        fixture.scan();
        if (afterReplacement) {
          fixture.scan();
        }
        final var current = fixture.pending();
        new BrokerClusterChangeCancelledApplier(fixture.state)
            .applyState(45, new ClusterConfigRecord().setClusterId("demo").setChangeId("old"));
        assertEquals(current, fixture.pending());
        assertEquals(
            BrokerClusterChangeStatus.CANCELLED,
            fixture
                .state
                .getBrokerTopologyChangeState()
                .clusterChangeOperation("demo", "old")
                .orElseThrow()
                .status());
        if (!afterReplacement) {
          fixture.scan();
        }
        fixture.complete(fixture.pending());
        assertEquals(
            BrokerClusterChangeStatus.CANCELLED,
            fixture
                .state
                .getBrokerTopologyChangeState()
                .clusterChangeOperation("demo", "old")
                .orElseThrow()
                .status());
        assertTrue(
            fixture.state.getBrokerTopologyChangeState().pendingChangePlans("demo").isEmpty());
      }
    }
  }

  @Test
  void missingBlockedReasonIsRejectedWithoutChangingDurableState() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.command(
          BrokerPartitionChangeIntent.BLOCK,
          BrokerPartitionChangeRecordMapper.fromPlan(fixture.old).setBlockedReason(null));
      assertEquals(RejectionType.INVALID_ARGUMENT, fixture.writers.rejection);
      assertEquals(fixture.old, fixture.pending());
    }
  }

  @Test
  void deadlineIsEvaluatedBeforeUnavailableTopology() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.state.getBrokerTopologyChangeState().putChangePlan(fixture.old.withDeadline(1, 2));
      fixture.state.getBrokerPartitionTopologyState().deletePartitionTopology("demo", 1);
      fixture.scan();
      assertEquals(BrokerPartitionChangeStatus.SUPERSEDING, fixture.pending().status());
      assertEquals(
          PlanBlockedReason.PLAN_DEADLINE_EXCEEDED, fixture.pending().execution().blockedReason());
      assertEquals(0, fixture.transport.reads);
      assertEquals(0, fixture.transport.dispatches);
    }
  }

  @Test
  void offlineTargetStopsPlanWhileAnOperationRpcIsStillPending() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.scan();
      assertEquals(1, fixture.transport.dispatches);
      fixture.offline("broker-4");
      fixture.scan();
      assertEquals(BrokerPartitionChangeStatus.SUPERSEDING, fixture.pending().status());
      final String reserved = fixture.pending().execution().successor();
      fixture.transport.operation.complete(
          new BrokerClusterChangeDispatchResult(fixture.old.nextOperation().opId(), true));
      fixture.scan();
      final var successor = fixture.pending();
      assertEquals(reserved, successor.changeId());
      assertEquals(2, successor.execution().generation());
      assertEquals("old", successor.execution().predecessor());
      assertEquals("old", successor.rootChangeId());
      assertEquals(
          BrokerPartitionChangeStatus.SUPERSEDED,
          fixture
              .state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "old")
              .orElseThrow()
              .status());
      assertFalse(
          successor.operations().stream()
              .anyMatch(
                  operation ->
                      operation.targetBroker().equals("broker-4")
                          && operation.type() != TopologyOperationType.LEAVE));
      final var finalMembers = successor.operations().getLast().configuration().targetMembers();
      assertEquals(
          Set.of("broker-0", "broker-1", "broker-2"),
          finalMembers.stream()
              .filter(member -> member.voter())
              .map(member -> member.brokerId())
              .collect(Collectors.toSet()));
      assertFalse(finalMembers.stream().anyMatch(member -> member.brokerId().equals("broker-4")));
      assertEquals(
          List.of(successor),
          fixture.state.getBrokerTopologyChangeState().pendingChangePlans("demo"));
      assertEquals(
          successor,
          fixture
              .state
              .getBrokerTopologyChangeState()
              .currentDescendant("demo", "old")
              .orElseThrow());
      final var before =
          fixture.state.getBrokerPartitionTopologyState().partitionTopology("demo", 1);
      fixture.command(
          TopologyPartitionChangeBehavior.command(fixture.old.nextOperation().type()),
          BrokerPartitionChangeRecordMapper.fromPlan(fixture.old));
      assertEquals(RejectionType.INVALID_STATE, fixture.writers.rejection);
      PartitionOperationFixture.applyOperation(fixture.state, 99, fixture.old);
      assertEquals(
          before, fixture.state.getBrokerPartitionTopologyState().partitionTopology("demo", 1));
      assertEquals(successor, fixture.pending());
    }
  }

  @Test
  void successorUsesActualCommittedVotersWhenPdHasNotAcknowledgedPromotion() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.offline("broker-4");
      fixture.transport.configuration =
          configuration(
              List.of("broker-0", "broker-1", "broker-2", "broker-4"), List.of(), 1, "old", 9);
      fixture.scan();
      fixture.scan();
      final var successor = fixture.pending();
      assertEquals(4, successor.operations().getFirst().configuration().sourceMembers().size());
      assertTrue(
          successor.operations().stream()
              .anyMatch(
                  operation ->
                      operation.type() == TopologyOperationType.LEAVE
                          && operation.targetBroker().equals("broker-4")));
      assertFalse(
          successor.operations().stream()
              .anyMatch(
                  operation ->
                      operation.type() == TopologyOperationType.LEAVE
                          && operation.targetBroker().equals("broker-0")));
      assertEquals(
          Set.of("broker-0", "broker-1", "broker-2", "broker-4"),
          new HashSet<>(
              fixture
                  .state
                  .getBrokerPartitionTopologyState()
                  .partitionTopology("demo", 1)
                  .orElseThrow()
                  .currentVoters()));
    }
  }

  @Test
  void noQuorumBlocksWithoutInstallingAuthorityOrGeneratingReset() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.offline("broker-4");
      fixture.offline("broker-1");
      fixture.offline("broker-2");
      fixture.scan();
      final String reserved = fixture.pending().execution().successor();
      fixture.scan();
      fixture.scan();
      assertEquals(PlanBlockedReason.NO_QUORUM, fixture.pending().execution().blockedReason());
      assertEquals(BrokerPartitionChangeStatus.SUPERSEDING, fixture.pending().status());
      assertEquals(reserved, fixture.pending().execution().successor());
      assertEquals(0, fixture.transport.installs);
      assertTrue(
          fixture.commands.stream()
              .noneMatch(
                  command ->
                      command.metadata().getIntent() == BrokerPartitionChangeIntent.REPLACE));
    }
  }

  @Test
  void failedLinearReadPreservesQuorumAndLeadershipReasonsWithoutInstallingAuthority()
      throws Exception {
    final var reasons =
        Map.of(
            "NO_QUORUM: timeout", "NO_QUORUM",
            "NOT_LEADER", "LEADER_UNAVAILABLE",
            "transport timeout", "CONFIGURATION_READ_UNAVAILABLE");
    for (final var reason : reasons.entrySet()) {
      try (final Fixture fixture = new Fixture(directory)) {
        fixture.offline("broker-4");
        fixture.scan();
        final String reserved = fixture.pending().execution().successor();
        fixture.transport.delayedRead = new CompletableActorFuture<>();
        fixture.scan();
        fixture.transport.delayedRead.completeExceptionally(
            new RuntimeException("RPC failed", new IllegalStateException(reason.getKey())));
        fixture.scheduling.runReady();
        assertEquals(
            PlanBlockedReason.parse(reason.getValue()),
            fixture.pending().execution().blockedReason());
        assertEquals(BrokerPartitionChangeStatus.SUPERSEDING, fixture.pending().status());
        assertEquals(reserved, fixture.pending().execution().successor());
        assertEquals(0, fixture.transport.installs);
        assertEquals(0, fixture.transport.dispatches);
      }
    }
  }

  @Test
  void availableQuorumWithInsufficientCapacityKeepsTheSameReservedSuccessor() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.offline("broker-4");
      fixture.offline("broker-2");
      fixture.offline("broker-3");
      fixture.scan();
      fixture.scan();
      final String reserved = fixture.pending().execution().successor();
      fixture.scan();
      assertEquals(
          PlanBlockedReason.INSUFFICIENT_CAPACITY, fixture.pending().execution().blockedReason());
      assertEquals(reserved, fixture.pending().execution().successor());
      assertEquals(0, fixture.transport.installs);
    }
  }

  @Test
  void lostInstallationResponseResumesTheCommittedOwnerWithoutIncreasingGeneration()
      throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.offline("broker-4");
      fixture.transport.loseInstallResponse = true;
      fixture.scan();
      fixture.scan();
      final String successor = fixture.pending().execution().successor();
      assertEquals(
          PlanBlockedReason.AUTHORITY_INSTALL_UNAVAILABLE,
          fixture.pending().execution().blockedReason());
      assertEquals(successor, fixture.transport.configuration.ownerChangeId());
      fixture.scan();
      assertEquals(successor, fixture.pending().changeId());
      assertEquals(2, fixture.pending().execution().generation());
      assertEquals(1, fixture.transport.installs);
    }
  }

  @Test
  void legacyPlanPersistsAuthorityBeforeSendingItsFirstOperation() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture
          .state
          .getBrokerTopologyChangeState()
          .putChangePlan(fixture.old.withExecution(ChangePlan.Execution.LEGACY));
      fixture.transport.configuration = withExecution(fixture.transport.configuration, 0, "", 0);
      fixture.scan();
      assertEquals(1, fixture.pending().execution().generation());
      assertEquals(1, fixture.transport.installs);
      assertEquals(0, fixture.transport.dispatches);
      fixture.scan();
      assertEquals(1, fixture.transport.dispatches);
      assertEquals(1, fixture.transport.request.authority().generation());
      assertEquals("old", fixture.transport.request.authority().owner());
    }
  }

  @Test
  void leadershipChangeIgnoresAnOldConfigurationCallback() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.offline("broker-4");
      fixture.scan();
      fixture.transport.delayedRead = new CompletableActorFuture<>();
      fixture.scan();
      fixture.executor.onClose();
      fixture.transport.delayedRead.complete(fixture.transport.configuration);
      assertEquals(0, fixture.transport.installs);
      assertEquals(BrokerPartitionChangeStatus.SUPERSEDING, fixture.pending().status());
    }
  }

  @Test
  void incompleteConfigurationCannotProduceAReplacement() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.offline("broker-4");
      fixture.transport.configuration = withDigest(fixture.transport.configuration, "unverified");
      fixture.scan();
      fixture.scan();
      assertEquals(
          PlanBlockedReason.INCOMPLETE_SOURCE_CONFIGURATION,
          fixture.pending().execution().blockedReason());
      assertEquals(0, fixture.transport.installs);
    }
  }

  @Test
  void successorReadsPromotedLeaderMissingFromProjectedVoters() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.promotedLeader("LEADER", true, false);
      fixture.projectLeader("");
      fixture.stopped();
      fixture.scan();

      assertEquals("replacement", fixture.pending().changeId());
      assertEquals("broker-4", fixture.transport.readEndpoint.brokerId());
      assertEquals(2, fixture.pending().execution().generation());
      final var actual =
          fixture
              .state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow();
      assertTrue(actual.currentVoters().contains("broker-4"));
      assertFalse(actual.learners().contains("broker-4"));
    }
  }

  @Test
  void promotedLeaderStopsStalePlanBeforeDeadlineAndReplacesIt() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.promotedLeader("LEADER", true, false);
      fixture.scan();
      assertEquals(
          PlanBlockedReason.CONFIGURATION_CONFLICT, fixture.pending().execution().blockedReason());
      fixture.scan();
      assertEquals(BrokerPartitionChangeStatus.SUPERSEDING, fixture.pending().status());
      fixture.scan();

      assertNotEquals("old", fixture.pending().changeId());
      assertEquals(1, fixture.transport.installs);
      assertEquals(0, fixture.transport.dispatches);
    }
  }

  @Test
  void promotedLeaderConfigurationReadDoesNotDependOnRoutingProjection() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.promotedLeader("LEADER", true, false);
      fixture.projectLeader("");
      fixture.stopped();
      fixture.scan();

      assertEquals("replacement", fixture.pending().changeId());
      assertEquals(1, fixture.transport.reads);
    }
  }

  @Test
  void staleLearnerOrOfflineReportCannotAuthorizeAReplacement() throws Exception {
    for (final String scenario : List.of("stale", "learner", "offline")) {
      try (final Fixture fixture = new Fixture(directory)) {
        fixture.promotedLeader(
            scenario.equals("learner") ? "LEARNER" : "LEADER",
            !scenario.equals("unready"),
            scenario.equals("stale"));
        if (scenario.equals("offline")) {
          fixture.offline("broker-4");
        }
        fixture.projectLeader("");
        fixture.stopped();
        fixture.scan();

        assertEquals("old", fixture.pending().changeId(), scenario);
        assertEquals(
            PlanBlockedReason.LEADER_UNAVAILABLE,
            fixture.pending().execution().blockedReason(),
            scenario);
        assertEquals(0, fixture.transport.reads, scenario);
        assertEquals(0, fixture.transport.installs, scenario);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void stoppedPlanReadsRaftLeaderBeforeBusinessReadiness(final boolean promoted) throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      if (promoted) {
        fixture.promotedLeader("LEADER", false, false);
      } else {
        fixture.bootstrapLeader(false);
      }
      fixture.projectLeader("");
      fixture.stopped();
      fixture.scan();

      assertEquals(1, fixture.transport.reads);
      assertEquals(promoted ? "broker-4" : "broker-0", fixture.transport.readEndpoint.brokerId());
      assertEquals(1, fixture.transport.installs);
      assertEquals("replacement", fixture.pending().changeId());
      assertEquals(2, fixture.pending().execution().generation());
      assertEquals(
          BrokerPartitionChangeStatus.SUPERSEDED,
          fixture
              .state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "old")
              .orElseThrow()
              .status());
    }
  }

  @Test
  void successorDispatchesJoinThroughRaftLeaderBeforeBusinessReadiness() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.bootstrapLeader(false);
      fixture.projectLeader("");
      fixture.stopped();
      fixture.scan();
      assertEquals("replacement", fixture.pending().changeId());
      assertEquals(BrokerPartitionChangeStatus.PENDING, fixture.admit(fixture.pending()).status());
      fixture.scan();

      assertEquals(1, fixture.transport.dispatches);
      assertEquals(JOIN, fixture.transport.request.type());
      assertEquals("broker-4", fixture.transport.request.targetBroker());
      assertEquals("replacement", fixture.transport.request.authority().owner());
      assertEquals(2, fixture.transport.request.authority().generation());
    }
  }

  @Test
  void unreadyRaftLeaderCannotReplacePlanWithoutCommittedConfigurationQuorum() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.bootstrapLeader(false);
      fixture.projectLeader("");
      fixture.stopped();
      fixture.offline("broker-1");
      fixture.offline("broker-2");
      fixture.scan();

      assertEquals(1, fixture.transport.reads);
      assertEquals(0, fixture.transport.installs);
      assertEquals(PlanBlockedReason.NO_QUORUM, fixture.pending().execution().blockedReason());
      assertEquals("old", fixture.pending().changeId());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void leaveOfNewlyElectedLeaderReplansTransferBeforeRemoval(final boolean staleRouting)
      throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      for (int index = 0; index < 2; index++) {
        PartitionOperationFixture.applyOperation(fixture.state, 46, fixture.pending());
      }
      assertEquals(TopologyOperationType.LEAVE, fixture.pending().nextOperation().type());
      fixture.transport.configuration =
          configuration(
              List.of("broker-0", "broker-1", "broker-2", "broker-4"), List.of(), 1, "old", 9);
      fixture.bootstrapLeader(false);
      fixture.projectLeader(staleRouting ? "broker-4" : "");
      fixture.scan();

      assertEquals(
          PlanBlockedReason.CONFIGURATION_CONFLICT, fixture.pending().execution().blockedReason());
      assertEquals(0, fixture.transport.dispatches);
      fixture.scan();
      assertEquals(BrokerPartitionChangeStatus.SUPERSEDING, fixture.pending().status());
      fixture.scan();

      final var successor = fixture.pending();
      assertNotEquals("old", successor.changeId());
      assertEquals(2, successor.execution().generation());
      assertEquals("broker-0", fixture.transport.readEndpoint.brokerId());
      assertEquals(1, fixture.transport.installs);
      assertEquals(
          BrokerPartitionChangeStatus.SUPERSEDED,
          fixture
              .state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "old")
              .orElseThrow()
              .status());
      int transferIndex = -1;
      int leaveIndex = -1;
      for (int index = 0; index < successor.operations().size(); index++) {
        final var operation = successor.operations().get(index);
        if (operation.type() == TopologyOperationType.TRANSFER_LEADER) {
          transferIndex = index;
          assertNotEquals("broker-0", operation.targetBroker());
        }
        if (operation.type() == TopologyOperationType.LEAVE
            && operation.targetBroker().equals("broker-0")) {
          leaveIndex = index;
        }
      }
      assertTrue(
          transferIndex >= 0 && leaveIndex > transferIndex, successor.operations().toString());
      assertEquals(0, fixture.transport.dispatches);
    }
  }

  @Test
  void reportedLeaderMustMatchTheVerifiedConfigurationLeader() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.promotedLeader("LEADER", true, false);
      fixture.transport.configuration = withLeader(fixture.transport.configuration, "broker-0");
      fixture.projectLeader("");
      fixture.stopped();
      fixture.scan();

      assertEquals(
          PlanBlockedReason.LEADER_UNAVAILABLE, fixture.pending().execution().blockedReason());
      assertEquals(0, fixture.transport.installs);
    }
  }

  @Test
  void promotedLeaderCannotInstallAuthorityWithoutActualConfigurationQuorum() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.promotedLeader("LEADER", true, false);
      fixture.offline("broker-1");
      fixture.offline("broker-2");
      fixture.projectLeader("");
      fixture.stopped();
      fixture.scan();

      assertEquals(PlanBlockedReason.NO_QUORUM, fixture.pending().execution().blockedReason());
      assertEquals(1, fixture.transport.reads);
      assertEquals(0, fixture.transport.installs);
      assertEquals("old", fixture.pending().changeId());
    }
  }

  @Test
  void promotedLeaderWithInvalidConfigurationDigestCannotReplaceThePlan() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.promotedLeader("LEADER", true, false);
      fixture.transport.configuration = withDigest(fixture.transport.configuration, "invalid");
      fixture.projectLeader("");
      fixture.stopped();
      fixture.scan();

      assertEquals(
          PlanBlockedReason.INCOMPLETE_SOURCE_CONFIGURATION,
          fixture.pending().execution().blockedReason());
      assertEquals(0, fixture.transport.installs);
    }
  }

  @Test
  void leadershipChangeFencesAReadDiscoveredThroughPromotedLeaderReport() throws Exception {
    try (final Fixture fixture = new Fixture(directory)) {
      fixture.promotedLeader("LEADER", true, false);
      fixture.projectLeader("");
      fixture.stopped();
      fixture.transport.delayedRead = new CompletableActorFuture<>();
      fixture.scan();
      fixture.executor.onClose();
      fixture.transport.delayedRead.complete(fixture.transport.configuration);

      assertEquals(1, fixture.transport.reads);
      assertEquals(0, fixture.transport.installs);
      assertEquals("old", fixture.pending().changeId());
    }
  }

  private static BrokerPartitionConfigurationRecord configuration(
      final List<String> voters,
      final List<String> learners,
      final long generation,
      final String owner,
      final long authorityIndex) {
    final var members = new ArrayList<BrokerPartitionMember>();
    voters.forEach(
        id -> members.add(new BrokerPartitionMember(id, true, id.equals("broker-0") ? 100 : 10)));
    learners.forEach(id -> members.add(new BrokerPartitionMember(id, false, 0)));
    final String digest = RaftConfigurationDigest.ofBroker("demo", members);
    return new BrokerPartitionConfigurationRecord(
        "demo", 1, "broker-0", 3, 8, digest, members, 0, generation, owner, authorityIndex);
  }

  private static BrokerPartitionConfigurationRecord withExecution(
      final BrokerPartitionConfigurationRecord source,
      final long generation,
      final String owner,
      final long authorityIndex) {
    return new BrokerPartitionConfigurationRecord(
        source.clusterId(),
        source.partitionId(),
        source.leaderId(),
        source.leaderTerm(),
        source.configurationIndex(),
        source.configurationDigest(),
        source.members(),
        source.recoveryEpoch(),
        generation,
        owner,
        authorityIndex);
  }

  private static BrokerPartitionConfigurationRecord withDigest(
      final BrokerPartitionConfigurationRecord source, final String digest) {
    return new BrokerPartitionConfigurationRecord(
        source.clusterId(),
        source.partitionId(),
        source.leaderId(),
        source.leaderTerm(),
        source.configurationIndex(),
        digest,
        source.members(),
        source.recoveryEpoch(),
        source.executionGeneration(),
        source.ownerChangeId(),
        source.authorityIndex());
  }

  private static BrokerPartitionConfigurationRecord withLeader(
      final BrokerPartitionConfigurationRecord source, final String leader) {
    return new BrokerPartitionConfigurationRecord(
        source.clusterId(),
        source.partitionId(),
        leader,
        source.leaderTerm(),
        source.configurationIndex(),
        source.configurationDigest(),
        source.members(),
        source.recoveryEpoch(),
        source.executionGeneration(),
        source.ownerChangeId(),
        source.authorityIndex());
  }

  private static final class Transport implements BrokerClusterChangeTransport {
    private BrokerPartitionConfigurationRecord configuration =
        configuration(
            List.of("broker-0", "broker-1", "broker-2"), List.of("broker-4"), 1, "old", 9);
    private final CompletableActorFuture<BrokerClusterChangeDispatchResult> operation =
        new CompletableActorFuture<>();
    private CompletableActorFuture<BrokerPartitionConfigurationRecord> delayedRead;
    private int reads, installs, dispatches;
    private boolean loseInstallResponse;
    private BrokerApplyTopologyOperationRequest request;
    private BrokerEndpoint readEndpoint;

    @Override
    public ActorFuture<BrokerClusterChangeDispatchResult> dispatchBrokerClusterChange(
        final BrokerEndpoint endpoint, final BrokerApplyTopologyOperationRequest request) {
      dispatches++;
      this.request = request;
      return operation;
    }

    @Override
    public ActorFuture<BrokerPartitionConfigurationRecord> readConfiguration(
        final BrokerEndpoint endpoint, final BrokerReadPartitionConfigurationRequest request) {
      reads++;
      readEndpoint = endpoint;
      return delayedRead == null ? CompletableActorFuture.completed(configuration) : delayedRead;
    }

    @Override
    public ActorFuture<BrokerPartitionConfigurationRecord> installAuthority(
        final BrokerEndpoint endpoint, final BrokerInstallPartitionAuthorityRequest request) {
      installs++;
      assertEquals(configuration.executionGeneration(), request.expectedGeneration());
      configuration =
          withExecution(configuration, request.generation(), request.ownerChangeId(), 12);
      return loseInstallResponse
          ? CompletableActorFuture.completedExceptionally(
              new IllegalStateException("response lost"))
          : CompletableActorFuture.completed(configuration);
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final PartitionStorage storage;
    private final MutableProcessingState state;
    private final ChangePlan old;
    private final Transport transport = new Transport();
    private final Capture writers = new Capture();
    private final List<LoggedEvent> commands = new ArrayList<>();
    private final PartitionExecuteProcessor execution;
    private final TopologyMembershipChangeExecutor executor;
    private final TopologyMembershipChangeChecker scheduler;
    private final StateStoreProcessingStore store;
    private final WorkflowScheduler scheduling =
        new WorkflowScheduler(new AtomicLong(System.currentTimeMillis()));
    private long position;

    Fixture(final Path directory) {
      storage = new PartitionStorage(directory.resolve(UUID.randomUUID().toString()));
      store = new StateStoreProcessingStore(storage);
      state = new MutableProcessingState(store);
      new BrokerClusterCreatedApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                  .setClusterId("demo")
                  .setClusterSize(5)
                  .setPartitionSize(1)
                  .setReplicationFactor(3));
      for (int index = 0; index < 5; index++) {
        state
            .getBrokerRegistrationState()
            .registerBroker(
                "demo",
                "broker-" + index,
                "127.0.0.1",
                27000 + index,
                28000 + index,
                "test",
                30000,
                Instant.now(),
                List.of());
      }
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(
                      3, List.of("broker-0", "broker-1", "broker-2"), "broker-0"),
                  PartitionTopology.Membership.initial(
                      List.of("broker-0", "broker-1", "broker-2"), List.of("broker-4"), "broker-0"),
                  PartitionStatus.ACTIVE,
                  Map.of()));
      old =
          BrokerTopologyOperationRequests.freeze(
                  state,
                  new ChangePlan(
                      new ChangePlan.Definition(
                          new ChangePlan.Identity("demo", "old", 1),
                          new ChangePlan.Versions(1, 2),
                          List.of(
                              TopologyOperation.join(1, "broker-4"),
                              TopologyOperation.promoteLearner(1, "broker-4"),
                              TopologyOperation.leave(1, "broker-0"))),
                      new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
                      new ChangePlan.Deadline(1, Long.MAX_VALUE)))
              .withExecution(
                  new ChangePlan.Execution(1, 9, "", "", "old", PlanBlockedReason.NONE, 0));
      new PartitionChangeSubmittedApplier(state)
          .applyState(2, BrokerPartitionChangeRecordMapper.fromPlan(old));
      new PartitionChangeAdmittedApplier(state)
          .applyState(3, BrokerPartitionChangeRecordMapper.fromPlan(old));
      bootstrapLeader(false);
      final var context = mock(TypedRecordProcessorContext.class);
      when(context.getProcessingState(ProcessingState.class)).thenReturn(state);
      when(context.getWriters()).thenReturn(scheduling.writers);
      final var stateWriter = mock(StateWriter.class);
      when(scheduling.writers.state()).thenReturn(stateWriter);
      doAnswer(
              call -> {
                PlacementDriverEventAppliers.create(state)
                    .applyState(call.getArgument(0), call.getArgument(1), call.getArgument(2), 1);
                return null;
              })
          .when(stateWriter)
          .appendFollowUpEvent(anyLong(), any(Intent.class), any(UnifiedRecordValue.class));
      executor =
          new TopologyMembershipChangeExecutor(
              context,
              () -> true,
              () -> transport,
              null,
              Duration.ofSeconds(15),
              Duration.ofSeconds(15));
      execution = new PartitionExecuteProcessor(context, executor);
      scheduler = new TopologyMembershipChangeChecker(state, () -> true, scheduling.clock::get);
      scheduling.processor =
          event -> {
            if (event.metadata().getIntent() == BrokerPartitionChangeIntent.EXECUTE) {
              execution.processRecord(record((PartitionChangeRecord) event.value()));
            } else if (event.metadata().getIntent()
                == BrokerPartitionChangeIntent.EXECUTION_RESULT) {
              execution.results().processRecord(record((PartitionExecutionRecord) event.value()));
            } else {
              commands.add(event);
              command(
                  (BrokerPartitionChangeIntent) event.metadata().getIntent(),
                  (PartitionChangeRecord) event.value());
            }
            commit();
          };
      commit();
      executor.onRecovered(scheduling);
      scheduler.onRecovered(scheduling);
    }

    <T extends UnifiedRecordValue> TypedRecord<T> record(final T value) {
      @SuppressWarnings("unchecked")
      final TypedRecord<T> record = mock(TypedRecord.class);
      when(record.getValue()).thenReturn(value);
      when(record.getPosition()).thenReturn(++position);
      when(record.getTimestamp()).thenReturn(scheduling.clock.get());
      return record;
    }

    void commit() {
      try {
        store.getCurrentTransaction().commit();
        store.getCurrentTransaction();
      } catch (final Exception error) {
        throw new AssertionError(error);
      }
    }

    ChangePlan uninitializedStoppedPlan() {
      state.getBrokerRegistrationState().renewBroker("demo", "broker-0", Instant.now(), List.of());
      final var empty =
          new PartitionTopology(
              new PartitionTopology.Identity("demo", 1),
              new PartitionTopology.Placement(
                  3, List.of("broker-0", "broker-1", "broker-2"), "broker-0"),
              PartitionTopology.Membership.initial(List.of(), List.of(), ""),
              PartitionStatus.CREATING,
              Map.of(),
              true);
      state.getBrokerPartitionTopologyState().putPartitionTopology(empty);
      final var bootstrap =
          BrokerTopologyOperationRequests.freeze(
              state,
              new ChangePlan(
                  new ChangePlan.Definition(
                      old.definition().identity(),
                      old.definition().versions(),
                      List.of(
                          TopologyOperation.add(1, "broker-0"),
                          TopologyOperation.join(1, "broker-1"),
                          TopologyOperation.promoteLearner(1, "broker-1"),
                          TopologyOperation.join(1, "broker-2"),
                          TopologyOperation.promoteLearner(1, "broker-2")),
                      ChangePlan.Purpose.BOOTSTRAP),
                  new ChangePlan.Progress(0, BrokerPartitionChangeStatus.SUPERSEDING),
                  new ChangePlan.Deadline(1, 2),
                  new ChangePlan.Execution(
                      0, 0, "", "replacement", "old", PlanBlockedReason.NO_QUORUM, 1)));
      state.getBrokerTopologyChangeState().putChangePlan(bootstrap);
      state.getBrokerTopologyChangeState().markPending(bootstrap);
      return bootstrap;
    }

    void promotedLeader(final String role, final boolean ready, final boolean stale) {
      state.getBrokerRegistrationState().renewBroker("demo", "broker-0", Instant.now(), List.of());
      final var source =
          configuration(
              List.of("broker-0", "broker-1", "broker-2", "broker-4"), List.of(), 1, "old", 9);
      transport.configuration =
          new BrokerPartitionConfigurationRecord(
              source.clusterId(),
              source.partitionId(),
              "broker-4",
              4,
              source.configurationIndex(),
              source.configurationDigest(),
              source.members(),
              source.recoveryEpoch(),
              source.executionGeneration(),
              source.ownerChangeId(),
              source.authorityIndex());
      final Instant now = Instant.now();
      state
          .getBrokerRegistrationState()
          .renewBroker(
              "demo",
              "broker-4",
              now,
              List.of(
                  new BrokerPartitionState(
                      1,
                      new BrokerPartitionState.Status(
                          MemberRole.parse(role), 4, BrokerPartitionLifecycle.ACTIVE, 10, 0),
                      new BrokerPartitionState.Replication(8, 0, true),
                      new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
                      new BrokerPartitionElectionState(
                          new RaftConfigurationReference(
                              8, transport.configuration.configurationDigest()),
                          new BrokerPartitionElectionState.Priorities(0, 100),
                          new BrokerPartitionElectionState.Observation(
                              RaftNodeState.STATE_LEADER,
                              "broker-4",
                              ready,
                              now.toEpochMilli() - (stale ? 1 : 0))))));
      state
          .getBrokerTopologyChangeState()
          .putChangePlan(
              new ChangePlan(
                  old.definition(),
                  new ChangePlan.Progress(1, BrokerPartitionChangeStatus.WAITING_READY),
                  old.deadline(),
                  old.execution()));
    }

    void bootstrapLeader(final boolean stale) {
      final Instant now = Instant.now();
      state
          .getBrokerRegistrationState()
          .renewBroker(
              "demo",
              "broker-0",
              now,
              List.of(
                  new BrokerPartitionState(
                      1,
                      new BrokerPartitionState.Status(
                          MemberRole.LEADER, 3, BrokerPartitionLifecycle.CATCHING_UP, 10, 0),
                      new BrokerPartitionState.Replication(8, 0, true),
                      new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
                      new BrokerPartitionElectionState(
                          new RaftConfigurationReference(
                              8, transport.configuration.configurationDigest()),
                          new BrokerPartitionElectionState.Priorities(100, 100),
                          new BrokerPartitionElectionState.Observation(
                              RaftNodeState.STATE_LEADER,
                              "broker-0",
                              false,
                              now.toEpochMilli() - (stale ? 1 : 0))))));
    }

    void stopped() {
      state
          .getBrokerTopologyChangeState()
          .putChangePlan(
              pending()
                  .withStatus(BrokerPartitionChangeStatus.SUPERSEDING)
                  .withExecution(
                      new ChangePlan.Execution(
                          1,
                          9,
                          "",
                          "replacement",
                          "old",
                          PlanBlockedReason.LEADER_UNAVAILABLE,
                          1)));
    }

    void scan() {
      commit();
      scheduler.onProcessed(mock(TypedRecord.class));
      scheduling.clock.addAndGet(1000);
      scheduling.runReady();
    }

    String projectedLeader() {
      return state
          .getBrokerPartitionTopologyState()
          .partitionTopology("demo", 1)
          .orElseThrow()
          .replicaStates()
          .entrySet()
          .stream()
          .filter(entry -> entry.getValue().role() == MemberRole.LEADER)
          .map(Map.Entry::getKey)
          .findFirst()
          .orElse("");
    }

    void projectLeader(final String memberId) {
      final var topology =
          state.getBrokerPartitionTopologyState().partitionTopology("demo", 1).orElseThrow();
      final Map<String, ReplicaState> replicas =
          memberId.isEmpty()
              ? Map.of()
              : Map.of(
                  memberId,
                  new ReplicaState(
                      new BrokerPartitionState.Status(
                          MemberRole.LEADER,
                          3,
                          BrokerPartitionLifecycle.ACTIVE,
                          100,
                          topology.recoveryEpoch()),
                      new BrokerPartitionState.Replication(8, 0, false),
                      new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
                      1,
                      BrokerPartitionElectionState.UNKNOWN));
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  topology.identity(),
                  topology.placement(),
                  topology.membership(),
                  topology.status(),
                  replicas,
                  topology.initializing()));
    }

    void parent() {
      state
          .getBrokerTopologyChangeState()
          .putClusterChangeOperation(
              new ClusterChangeOperation(
                  "demo",
                  "old",
                  1,
                  5,
                  1,
                  3,
                  List.of(),
                  List.of(),
                  List.of(),
                  List.of(),
                  List.of("old"),
                  BrokerClusterChangeStatus.PENDING,
                  1,
                  Long.MAX_VALUE));
    }

    ChangePlan admit(final ChangePlan initial) {
      ChangePlan plan =
          state.getBrokerTopologyChangeState().changePlan("demo", initial.changeId()).orElseThrow();
      if (plan.status() == BrokerPartitionChangeStatus.QUEUED) {
        command(
            BrokerPartitionChangeIntent.ADMIT, BrokerPartitionChangeRecordMapper.fromPlan(plan));
        assertNull(writers.rejection);
        plan =
            state
                .getBrokerTopologyChangeState()
                .changePlan("demo", initial.changeId())
                .orElseThrow();
        assertEquals(BrokerPartitionChangeStatus.PENDING, plan.status());
      }
      return plan;
    }

    void complete(final ChangePlan initial) {
      ChangePlan plan = admit(initial);
      while (plan.hasMoreOperations()) {
        final int previousIndex = plan.nextOperationIndex();
        PartitionOperationFixture.applyOperation(state, 46, plan);
        plan =
            state.getBrokerTopologyChangeState().changePlan("demo", plan.changeId()).orElseThrow();
        assertEquals(previousIndex + 1, plan.nextOperationIndex());
      }
      assertEquals(BrokerPartitionChangeStatus.COMPLETED, plan.status());
    }

    void offline(final String id) {
      state.getBrokerRegistrationState().markBrokerOffline("demo", id, Instant.now());
    }

    ChangePlan pending() {
      return state.getBrokerTopologyChangeState().pendingChangePlans("demo").getFirst();
    }

    void command(final BrokerPartitionChangeIntent intent, final PartitionChangeRecord value) {
      writers.intent = null;
      writers.value = null;
      writers.rejection = null;
      writers.commandIntent = null;
      writers.commandValue = null;
      final var context = new Context(state, writers, System.currentTimeMillis());
      final var operations = TypedRecordProcessors.processors();
      TopologyMembershipChangeProcessors.register(operations, context);
      @SuppressWarnings("unchecked")
      final var processor =
          switch (intent) {
            case ADMIT -> new PartitionAdmitProcessor(context);
            case SUPERSEDE -> new PartitionSupersedeProcessor(context);
            case REPLACE -> new PartitionReplaceProcessor(context);
            case AUTHORIZE -> new PartitionAuthorizeProcessor(context);
            case BLOCK -> new PartitionBlockProcessor(context);
            case UPDATE_STATUS -> new PartitionUpdateStatusProcessor(context);
            default ->
                (TypedRecordProcessor<PartitionChangeRecord>)
                    operations.get(ValueType.PARTITION_CHANGE, intent, 1);
          };
      processor.processRecord(
          new TypedRecord<PartitionChangeRecord>() {
            @Override
            public long getKey() {
              return 1;
            }

            @Override
            public PartitionChangeRecord getValue() {
              return value;
            }

            @Override
            public String getRequestStreamId() {
              return "stream";
            }

            @Override
            public String getRequestId() {
              return "request";
            }

            @Override
            public int getLength() {
              return value.getLength();
            }
          });
      if (writers.intent != null) {
        PlacementDriverEventAppliers.create(state).applyState(3, writers.intent, writers.value, 1);
      }
      if (writers.commandIntent != null) {
        command(
            (BrokerPartitionChangeIntent) writers.commandIntent,
            (PartitionChangeRecord) writers.commandValue);
      }
    }

    @Override
    public void close() throws Exception {
      executor.onClose();
      scheduler.onClose();
      commit();
      storage.close();
    }
  }

  private record Context(MutableProcessingState state, Capture writers, long processingTime)
      implements TypedRecordProcessorContext {
    @Override
    public long getProcessingTime() {
      return processingTime;
    }

    @Override
    public int getPartitionId() {
      return 1;
    }

    @Override
    public KeyGenerator getKeyGenerator() {
      return () -> 1;
    }

    @Override
    public Writers getWriters() {
      return writers;
    }

    @Override
    public Object getProcessingState() {
      return state;
    }

    @Override
    public EventAppliers getEventAppliers() {
      return PlacementDriverEventAppliers.create(state);
    }
  }

  private static final class Capture implements Writers {
    private Intent intent;
    private UnifiedRecordValue value;
    private RejectionType rejection;
    private Intent commandIntent;
    private UnifiedRecordValue commandValue;

    @Override
    public TypedCommandWriter command() {
      return new TypedCommandWriter() {
        @Override
        public void appendFollowUpCommand(long key, Intent intent, UnifiedRecordValue value) {
          commandIntent = intent;
          commandValue = value;
        }

        @Override
        public void appendFollowUpCommand(
            long key, ValueType valueType, Intent intent, UnifiedRecordValue value) {
          appendFollowUpCommand(key, intent, value);
        }
      };
    }

    @Override
    public StateWriter state() {
      return new StateWriter() {
        @Override
        public void appendFollowUpEvent(long key, Intent intent, UnifiedRecordValue value) {
          Capture.this.intent = intent;
          Capture.this.value = value;
        }

        @Override
        public void appendFollowUpEvent(
            long key, Intent intent, UnifiedRecordValue value, int version) {
          appendFollowUpEvent(key, intent, value);
        }

        @Override
        public void appendFollowUpEvent(
            long key, ValueType type, Intent intent, UnifiedRecordValue value) {
          appendFollowUpEvent(key, intent, value);
        }

        @Override
        public void appendFollowUpEvent(
            long key, ValueType type, Intent intent, UnifiedRecordValue value, int version) {
          appendFollowUpEvent(key, intent, value);
        }
      };
    }

    @Override
    public TypedRejectionWriter rejection() {
      return (type, reason) -> rejection = type;
    }

    @Override
    public TypedResponseWriter response() {
      return new TypedResponseWriter() {
        @Override
        public void writeEventOnCommand(
            long key, Intent intent, UnifiedRecordValue value, TypedRecord<?> command) {}

        @Override
        public void writeRejectionOnCommand(
            TypedRecord<?> command, RejectionType type, String reason) {}
      };
    }

    @Override
    public PostCommitActionWriter postCommit() {
      return action -> {};
    }
  }
}
