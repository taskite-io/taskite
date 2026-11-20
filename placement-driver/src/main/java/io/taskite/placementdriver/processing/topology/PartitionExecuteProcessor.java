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
package io.taskite.placementdriver.processing.topology;

import io.taskite.placementdriver.broker.cluster.BrokerMigrationBudget;
import io.taskite.placementdriver.broker.cluster.BrokerOperationEffectVerifier;
import io.taskite.placementdriver.broker.cluster.BrokerOperationSafety;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionSuccessorPlanner;
import io.taskite.placementdriver.broker.cluster.BrokerPlanDependencies;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.PartitionLeadershipResolver;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.immutable.TopologyMembershipChangeState;
import io.taskite.protocol.impl.record.value.broker.BrokerPartitionConfigurationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionExecutionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionExecutionRecord.Phase;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionExecutionRecord.Schedule;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.PartitionStatus;
import java.util.List;
import java.util.UUID;

/**
 * Owns only partition-plan execution. RPC callbacks submit concrete commands; business progress and
 * observations are persisted by independent Intent Appliers.
 */
public final class PartitionExecuteProcessor
    implements TypedRecordProcessor<PartitionChangeRecord> {
  private final ProcessingState state;
  private final Writers writers;
  private final TopologyMembershipChangeExecutor executor;

  public PartitionExecuteProcessor(
      final TypedRecordProcessorContext context, final TopologyMembershipChangeExecutor executor) {
    state = context.getProcessingState(ProcessingState.class);
    writers = context.getWriters();
    this.executor = executor;
  }

  @Override
  public void processRecord(final TypedRecord<PartitionChangeRecord> record) {
    if (!executor.active()) {
      return;
    }
    final var value = record.getValue();
    final var plan =
        state
            .getBrokerTopologyChangeState()
            .changePlan(value.getClusterId(), value.getChangeId())
            .orElse(null);
    if (plan == null
        || !current(plan)
        || !TopologyMembershipChangeState.identity(plan)
            .equals(
                TopologyMembershipChangeState.identity(
                    BrokerPartitionChangeRecordMapper.toPlan(
                        value, BrokerPartitionChangeStatus.valueOf(value.getStatus().name()))))) {
      return;
    }
    final var entry =
        state
            .getTopologyMembershipChangeState()
            .partition(plan.clusterId(), plan.partitionId())
            .orElse(null);
    if (entry == null || !entry.plan().equals(TopologyMembershipChangeState.identity(plan))) {
      return;
    }
    final long now = record.getTimestamp();
    if (entry.dueAt() > now) {
      return;
    }
    if (plan.status() == BrokerPartitionChangeStatus.QUEUED) {
      update(entry.ready(now + 1000));
      if (BrokerMigrationBudget.canAdmit(state, plan, BrokerMigrationBudget.DEFAULT)) {
        command(BrokerPartitionChangeIntent.ADMIT, plan);
      }
      return;
    }
    // Invalidation always precedes RPC exclusion and the end-of-plan check.
    if (plan.status() != BrokerPartitionChangeStatus.SUPERSEDING) {
      final var invalidation = BrokerPlanDependencies.invalidation(state, plan, now);
      if (invalidation != PlanBlockedReason.NONE) {
        update(entry.ready(now + 1000));
        if (!plan.hasMoreOperations() && invalidation == PlanBlockedReason.PLAN_DEADLINE_EXCEEDED) {
          block(plan, PlanBlockedReason.JOB_SUBSCRIPTION_DISTRIBUTION_TIMEOUT, now);
        } else {
          final var old = plan.execution();
          command(
              BrokerPartitionChangeIntent.SUPERSEDE,
              plan.withExecution(
                  new ChangePlan.Execution(
                      old.generation(),
                      old.authorityIndex(),
                      old.predecessor(),
                      "successor:" + plan.partitionId() + ":" + UUID.randomUUID(),
                      plan.rootChangeId(),
                      invalidation,
                      now)));
        }
        return;
      }
    }
    final var topology =
        state
            .getBrokerPartitionTopologyState()
            .partitionTopology(plan.clusterId(), plan.partitionId())
            .orElse(null);
    if (topology == null) {
      update(entry.ready(now + 1000));
      return;
    }
    final var observed = managementLeader(plan, topology);
    if (plan.status() != BrokerPartitionChangeStatus.SUPERSEDING
        && observed != null
        && !topology.currentVoters().contains(observed.brokerId())) {
      update(entry.ready(now + 1000));
      block(plan, PlanBlockedReason.CONFIGURATION_CONFLICT, now);
      return;
    }
    if (entry.phase() == Phase.APPLY
        && plan.status() != BrokerPartitionChangeStatus.SUPERSEDING
        && plan.hasMoreOperations()
        && (plan.execution().generation() != 0
            || plan.nextOperation().type() == TopologyOperationType.ADD
            || plan.nextOperation().type() == TopologyOperationType.RESET_PEERS)
        && applied(plan, topology)) {
      update(entry.ready(now + 1000));
      command(TopologyPartitionChangeBehavior.command(plan.nextOperation().type()), plan);
      return;
    }
    if (entry.phase() != Phase.READY && entry.deadline() > now) {
      update(entry.observe(Math.min(now + 1000, entry.deadline())));
      return;
    }
    if (plan.status() == BrokerPartitionChangeStatus.SUPERSEDING) {
      if (bootstrap(plan, topology, observed)) {
        final String target = plan.nextOperation().targetBroker();
        final var eligible =
            registrations(plan).stream()
                .filter(broker -> broker.status() == BrokerStatus.ONLINE)
                .filter(
                    broker ->
                        !state
                            .getBrokerRegistrationState()
                            .isVoterExcluded(plan.clusterId(), broker.brokerId()))
                .toList();
        if (eligible.stream().noneMatch(broker -> broker.brokerId().equals(target))) {
          update(entry.ready(now + 1000));
          block(plan, PlanBlockedReason.TARGET_UNAVAILABLE, now);
        } else if (eligible.size()
            < state
                .getBrokerClusterConfigState()
                .clusterConfig(plan.clusterId())
                .replicationFactor()) {
          update(entry.ready(now + 1000));
          block(plan, PlanBlockedReason.INSUFFICIENT_CAPACITY, now);
        } else {
          begin(
              plan,
              entry,
              Phase.BOOTSTRAP,
              endpoint(plan, target),
              null,
              record.getPosition(),
              now);
        }
      } else {
        read(plan, entry, observed, record.getPosition(), now);
      }
      return;
    }
    if (!plan.hasMoreOperations()) {
      update(entry.ready(now + 1000));
      return;
    }
    final var operation = plan.nextOperation();
    if (plan.execution().generation() == 0
        && operation.type() != TopologyOperationType.ADD
        && operation.type() != TopologyOperationType.RESET_PEERS) {
      read(plan, entry, observed, record.getPosition(), now);
      return;
    }
    if (applied(plan, topology)) {
      update(entry.ready(now + 1000));
      command(TopologyPartitionChangeBehavior.command(operation.type()), plan);
      return;
    }
    if (operation.type() == TopologyOperationType.LEAVE
        && observed != null
        && operation.targetBroker().equals(observed.brokerId())) {
      update(entry.ready(now + 1000));
      block(plan, PlanBlockedReason.CONFIGURATION_CONFLICT, now);
      return;
    }
    if (operation.type() == TopologyOperationType.JOIN
        && plan.status() == BrokerPartitionChangeStatus.WAITING_READY) {
      update(entry.ready(now + 1000));
      return;
    }
    if (!ready(plan, topology, operation)) {
      update(entry.ready(now + 1000));
      status(plan, BrokerPartitionChangeStatus.WAITING_READY);
      return;
    }
    if ((operation.type() == TopologyOperationType.TRANSFER_LEADER
            || operation.type() == TopologyOperationType.STEP_DOWN_IF_NOT_PRIMARY)
        && plan.status() != BrokerPartitionChangeStatus.WAITING_LEADER_CONVERGE) {
      update(entry.ready(now + 1000));
      status(plan, BrokerPartitionChangeStatus.WAITING_LEADER_CONVERGE);
      return;
    }
    final var endpoint =
        operation.type() == TopologyOperationType.ADD
                || operation.type() == TopologyOperationType.RESET_PEERS
            ? endpoint(plan, operation.targetBroker())
            : observed;
    if (endpoint == null) {
      update(entry.ready(now + 1000));
      return;
    }
    begin(plan, entry, Phase.APPLY, endpoint, null, record.getPosition(), now);
  }

  /** Result commands are registered separately, without a generic command/event interpreter. */
  public TypedRecordProcessor<PartitionExecutionRecord> results() {
    return new PartitionExecutionResultProcessor();
  }

  public final class PartitionExecutionResultProcessor
      implements TypedRecordProcessor<PartitionExecutionRecord> {
    @Override
    public void processRecord(final TypedRecord<PartitionExecutionRecord> record) {
      if (!executor.active()) {
        return;
      }
      final var result = record.getValue();
      final var expected = result.getSchedule();
      if (expected == null) {
        return;
      }
      final var identity = expected.plan();
      final var entry =
          state
              .getTopologyMembershipChangeState()
              .partition(identity.clusterId(), identity.partitionId())
              .orElse(null);
      final var plan =
          state
              .getBrokerTopologyChangeState()
              .changePlan(identity.clusterId(), identity.changeId())
              .orElse(null);
      if (entry == null
          || plan == null
          || !current(plan)
          || !entry.plan().equals(identity)
          || entry.phase() != expected.phase()
          || entry.attempt() != expected.attempt()
          || !TopologyMembershipChangeState.identity(plan).equals(identity)) {
        return;
      }
      final long now = record.getTimestamp();
      // A successful RPC starts prompt report verification. Retain its attempt until the
      // original one-second retry interval so a missing report cannot cause repeated dispatch.
      update(
          result.isSuccess() && entry.phase() == Phase.APPLY
              ? new Schedule(
                  entry.plan(),
                  entry.phase(),
                  entry.attempt(),
                  now,
                  now + 1000,
                  entry.endpoint(),
                  entry.source())
              : entry.ready(now + 1000));
      if (plan.status() != BrokerPartitionChangeStatus.SUPERSEDING
          && BrokerPlanDependencies.invalidation(state, plan, now) != PlanBlockedReason.NONE) {
        return;
      }
      if (!result.isSuccess()) {
        block(plan, result.getFailure(), now);
        return;
      }
      if (entry.phase() == Phase.APPLY) {
        // Success only starts observation; ACK is based on BrokerOperationEffectVerifier.
        return;
      }
      if (entry.phase() == Phase.BOOTSTRAP) {
        begin(plan, entry, Phase.READ, entry.endpoint(), null, record.getPosition(), now);
        return;
      }
      final var configuration = result.getConfiguration();
      try {
        final var members = BrokerPartitionSuccessorPlanner.verifiedMembers(plan, configuration);
        if (!entry.endpoint().brokerId().equals(configuration.leaderId())) {
          block(plan, PlanBlockedReason.LEADER_UNAVAILABLE, now);
          return;
        }
        final String owner =
            plan.status() == BrokerPartitionChangeStatus.SUPERSEDING
                ? plan.execution().successor()
                : plan.changeId();
        if (entry.phase() == Phase.INSTALL) {
          final var source = entry.source();
          if (!owner.equals(configuration.ownerChangeId())
              || configuration.executionGeneration() != source.executionGeneration() + 1
              || configuration.authorityIndex() <= 0
              || configuration.recoveryEpoch() != source.recoveryEpoch()) {
            block(plan, PlanBlockedReason.EXECUTION_AUTHORITY_CONFLICT, now);
            return;
          }
          authorized(plan, configuration, now);
          return;
        }
        final var voters = members.stream().filter(member -> member.voter()).toList();
        final var available = BrokerPlanDependencies.available(state, plan.clusterId());
        if (voters.stream().filter(member -> available.contains(member.brokerId())).count()
            < voters.size() / 2 + 1) {
          block(plan, PlanBlockedReason.NO_QUORUM, now);
          return;
        }
        if (plan.status() == BrokerPartitionChangeStatus.SUPERSEDING
            && registrations(plan).stream()
                    .filter(broker -> broker.status() == BrokerStatus.ONLINE)
                    .count()
                < state
                    .getBrokerClusterConfigState()
                    .clusterConfig(plan.clusterId())
                    .replicationFactor()) {
          block(plan, PlanBlockedReason.INSUFFICIENT_CAPACITY, now);
          return;
        }
        if (owner.equals(configuration.ownerChangeId())
            && configuration.executionGeneration() > plan.execution().generation()
            && configuration.authorityIndex() > 0) {
          authorized(plan, configuration, now);
        } else {
          begin(
              plan,
              entry,
              Phase.INSTALL,
              entry.endpoint(),
              configuration,
              record.getPosition(),
              now);
        }
      } catch (final IllegalStateException invalid) {
        block(plan, PlanBlockedReason.fromFailure(invalid.getMessage()), now);
      }
    }
  }

  private void authorized(
      final ChangePlan plan,
      final BrokerPartitionConfigurationRecord configuration,
      final long now) {
    if (plan.status() == BrokerPartitionChangeStatus.SUPERSEDING) {
      try {
        command(
            BrokerPartitionChangeIntent.REPLACE,
            BrokerPartitionSuccessorPlanner.plan(state, plan, configuration));
      } catch (final IllegalStateException invalid) {
        block(plan, PlanBlockedReason.fromFailure(invalid.getMessage()), now);
      }
    } else {
      final var old = plan.execution();
      command(
          BrokerPartitionChangeIntent.AUTHORIZE,
          plan.withExecution(
              new ChangePlan.Execution(
                  configuration.executionGeneration(),
                  configuration.authorityIndex(),
                  old.predecessor(),
                  old.successor(),
                  plan.rootChangeId(),
                  PlanBlockedReason.NONE,
                  0)));
    }
  }

  private void read(
      final ChangePlan plan,
      final Schedule entry,
      final BrokerEndpoint endpoint,
      final long attempt,
      final long now) {
    if (endpoint == null) {
      update(entry.ready(now + 1000));
      block(
          plan,
          BrokerPlanDependencies.hasQuorum(state, plan)
              ? PlanBlockedReason.LEADER_UNAVAILABLE
              : PlanBlockedReason.NO_QUORUM,
          now);
      return;
    }
    begin(plan, entry, Phase.READ, endpoint, null, attempt, now);
  }

  private void begin(
      final ChangePlan plan,
      final Schedule previous,
      final Phase phase,
      final BrokerEndpoint endpoint,
      final BrokerPartitionConfigurationRecord source,
      final long attempt,
      final long now) {
    executor.beginPartition(plan, previous, phase, endpoint, source, attempt, now);
  }

  private boolean current(final ChangePlan plan) {
    return !plan.isTerminal() && state.getBrokerTopologyChangeState().ownsPendingPlan(plan);
  }

  private void update(final Schedule schedule) {
    writers
        .state()
        .appendFollowUpEvent(
            0,
            BrokerPartitionChangeIntent.EXECUTION_UPDATED,
            new PartitionExecutionRecord().setSchedule(schedule));
  }

  private void command(final BrokerPartitionChangeIntent intent, final ChangePlan plan) {
    writers
        .command()
        .appendFollowUpCommand(
            0,
            ValueType.PARTITION_CHANGE,
            intent,
            BrokerPartitionChangeRecordMapper.fromPlan(plan));
  }

  private void block(final ChangePlan plan, final PlanBlockedReason reason, final long now) {
    if (plan.execution().blockedReason() != reason) {
      final var old = plan.execution();
      command(
          BrokerPartitionChangeIntent.BLOCK,
          plan.withExecution(
              new ChangePlan.Execution(
                  old.generation(),
                  old.authorityIndex(),
                  old.predecessor(),
                  old.successor(),
                  plan.rootChangeId(),
                  reason,
                  now)));
    }
  }

  private void status(final ChangePlan plan, final BrokerPartitionChangeStatus status) {
    if (plan.status() != status) {
      command(BrokerPartitionChangeIntent.UPDATE_STATUS, plan.withStatus(status));
    }
  }

  private List<BrokerRegistration> registrations(final ChangePlan plan) {
    return state.getBrokerRegistrationState().registrations(plan.clusterId());
  }

  private BrokerEndpoint endpoint(final ChangePlan plan, final String brokerId) {
    return registrations(plan).stream()
        .filter(broker -> broker.brokerId().equals(brokerId))
        .map(
            broker ->
                new BrokerEndpoint(
                    broker.brokerId(), broker.host(), broker.port(), broker.raftPort()))
        .findFirst()
        .orElse(null);
  }

  private BrokerEndpoint managementLeader(final ChangePlan plan, final PartitionTopology topology) {
    final var brokers =
        registrations(plan).stream()
            .filter(broker -> broker.status() != BrokerStatus.OFFLINE)
            .toList();
    final var observed =
        PartitionLeadershipResolver.resolveForManagement(
            brokers,
            plan.partitionId(),
            brokers.stream().map(BrokerRegistration::brokerId).toList(),
            topology.recoveryEpoch());
    final String leaderId =
        observed.leaderId().isBlank()
            ? PartitionLeadershipResolver.resolve(
                    brokers, plan.partitionId(), topology.currentVoters(), topology.recoveryEpoch())
                .leaderId()
            : observed.leaderId();
    return endpoint(plan, leaderId);
  }

  private boolean bootstrap(
      final ChangePlan plan, final PartitionTopology topology, final BrokerEndpoint leader) {
    if (leader != null
        || topology.status() != PartitionStatus.CREATING
        || !topology.currentVoters().isEmpty()
        || !topology.learners().isEmpty()
        || plan.nextOperationIndex() != 0
        || !plan.hasMoreOperations()
        || plan.execution().generation() != 0
        || plan.execution().authorityIndex() != 0) {
      return false;
    }
    final var operation = plan.nextOperation();
    final var frozen = operation.configuration();
    if (operation.type() != TopologyOperationType.ADD
        || frozen == null
        || !frozen.sourceMembers().isEmpty()
        || frozen.expectedMembershipEpoch() != 0
        || frozen.expectedRecoveryEpoch() != topology.recoveryEpoch()
        || frozen.targetMembers().size() != 1
        || !frozen.targetMembers().getFirst().voter()
        || !operation.targetBroker().equals(frozen.targetMembers().getFirst().brokerId())) {
      return false;
    }
    return true;
  }

  private boolean ready(
      final ChangePlan plan, final PartitionTopology topology, final TopologyOperation operation) {
    return switch (operation.type()) {
      case LEAVE ->
          BrokerOperationSafety.leavePreservesQuorum(
              topology,
              operation.targetBroker(),
              BrokerPlanDependencies.available(state, plan.clusterId()));
      case PROMOTE_LEARNER ->
          BrokerOperationSafety.replicaReadyForPromotion(topology, operation.targetBroker());
      case RECONFIGURE_PRIORITY -> topology.replicaStates().containsKey(operation.targetBroker());
      default -> true;
    };
  }

  private boolean applied(final ChangePlan plan, final PartitionTopology topology) {
    final var operation = plan.nextOperation();
    return state
            .getBrokerTopologyChangeState()
            .isOperationApplied(plan.clusterId(), operation.opId())
        || (BrokerOperationEffectVerifier.applied(topology, operation, registrations(plan))
            && (operation.type() != TopologyOperationType.PROMOTE_LEARNER
                || BrokerOperationSafety.replicaReadyForPromotion(
                    topology, operation.targetBroker())));
  }
}
