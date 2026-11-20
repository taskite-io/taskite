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
package io.taskite.placementdriver.processing.broker.topology;

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService.BrokerClusterCreation;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService.BrokerTopologyRebalance;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.BrokerTopologyPlanner;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.broker.topology.QuorumRecoveryCoordinator;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.immutable.BrokerTopologyCoordinatorState.Entry;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerTopologyCoordinationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerTopologyCoordinationRecord.Resource;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerTopologyCoordinationRecord.TaskType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BooleanSupplier;

/** Coordinates recovery and rebalance from the current topology and durable dirty revisions. */
public final class TopologyCoordinateProcessor
    implements TypedRecordProcessor<BrokerTopologyCoordinationRecord> {
  private final ProcessingState state;
  private final Writers writers;
  private final BooleanSupplier leader;
  private final BrokerClusterChangePlanningService planning;
  private final long planTimeoutMillis;
  private final QuorumRecoveryCoordinator quorumRecovery = new QuorumRecoveryCoordinator();

  public TopologyCoordinateProcessor(
      final TypedRecordProcessorContext context,
      final BooleanSupplier leader,
      final BrokerClusterChangePlanningService planning,
      final Duration planTimeout) {
    state = context.getProcessingState(ProcessingState.class);
    writers = context.getWriters();
    this.leader = leader;
    this.planning = planning;
    planTimeoutMillis = planTimeout.toMillis();
  }

  @Override
  public void processRecord(final TypedRecord<BrokerTopologyCoordinationRecord> record) {
    if (!leader.getAsBoolean()) {
      return;
    }
    final var command = record.getValue();
    if (command.getRevision() == 0 && command.getTaskType() == TaskType.RECOVERY_SCAN) {
      writers
          .state()
          .appendFollowUpEvent(
              0, TopologyIntent.COORDINATED, command.setCoordinatedAt(record.getTimestamp()));
      return;
    }
    final var entry =
        state
            .getBrokerTopologyCoordinatorState()
            .getTask(command.getClusterId(), command.getPartitionId())
            .orElse(null);
    if (entry == null
        || entry.revision() != command.getRevision()
        || entry.dueAt() > record.getTimestamp()) {
      return;
    }
    final var result =
        new BrokerTopologyCoordinationRecord()
            .setClusterId(entry.clusterId())
            .setPartitionId(entry.partitionId())
            .setRevision(entry.revision())
            .setCoordinatedAt(record.getTimestamp())
            .setDueAt(record.getTimestamp())
            .setFinished(true);
    switch (entry.taskType()) {
      case RECOVERY_SCAN, CLUSTER_EXPANSION -> expand(entry, result);
      case PARTITION_COORDINATION -> {
        final var topology =
            state
                .getBrokerPartitionTopologyState()
                .partitionTopology(entry.clusterId(), entry.partitionId())
                .orElse(null);
        if (topology != null
            && topology.status() == PartitionStatus.ACTIVE
            && !state
                .getBrokerTopologyChangeState()
                .hasPendingPlanForPartition(entry.clusterId(), entry.partitionId())) {
          final var planned = plan(topology);
          if (planned.isPresent()) {
            if (state
                    .getBrokerTopologyCoordinatorState()
                    .reservedPartitions(record.getTimestamp(), 256)
                >= 256) {
              result.setFinished(false).setDueAt(record.getTimestamp() + 100);
              writers.state().appendFollowUpEvent(0, TopologyIntent.COORDINATED, result);
              return;
            }
            final var change = planned.orElseThrow();
            final String id =
                change.kind()
                    + ":"
                    + change.brokerId()
                    + ":"
                    + topology.partitionId()
                    + ":"
                    + UUID.randomUUID();
            final var operations =
                change.operations().stream()
                    .map(
                        operation ->
                            new TopologyOperation(
                                id + ":" + operation.opId(),
                                operation.type(),
                                operation.targetBroker(),
                                operation.priority()))
                    .toList();
            final long now = record.getTimestamp();
            final var plan =
                new ChangePlan(
                    new ChangePlan.Definition(
                        new ChangePlan.Identity(topology.clusterId(), id, topology.partitionId()),
                        new ChangePlan.Versions(state.getVersion(), state.getVersion() + 1),
                        operations,
                        topology.initializing() && change.kind().equals("rebalance")
                            ? ChangePlan.Purpose.BOOTSTRAP
                            : ChangePlan.Purpose.MIGRATION),
                    new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
                    new ChangePlan.Deadline(now, now + planTimeoutMillis));
            // Persist observation even if SUBMIT subsequently loses ownership or is rejected.
            result.setFinished(false).setDueAt(now + 15_000).setObservingSubmit(true);
            writers
                .command()
                .appendFollowUpCommand(
                    0,
                    ValueType.PARTITION_CHANGE,
                    BrokerPartitionChangeIntent.SUBMIT,
                    BrokerPartitionChangeRecordMapper.fromPlan(plan));
          }
        }
      }
    }
    writers.state().appendFollowUpEvent(0, TopologyIntent.COORDINATED, result);
  }

  private void expand(final Entry entry, final BrokerTopologyCoordinationRecord result) {
    final boolean recovery = entry.taskType() == TaskType.RECOVERY_SCAN;
    final var page =
        state
            .getBrokerPartitionTopologyState()
            .partitionTopologiesAfter(
                recovery ? entry.cursorCluster() : entry.clusterId(), entry.cursorPartition(), 100);
    final var resources = new ArrayList<Resource>();
    boolean finished = page.size() < 100;
    for (final var topology : page) {
      if (!recovery && !topology.clusterId().equals(entry.clusterId())) {
        finished = true;
        break;
      }
      resources.add(new Resource(topology.clusterId(), topology.partitionId()));
      result.setCursorCluster(topology.clusterId()).setCursorPartition(topology.partitionId());
    }
    result.setResources(resources).setFinished(finished);
  }

  private Optional<PlannedChange> plan(final PartitionTopology topology) {
    final var configuration =
        state.getBrokerClusterConfigState().findClusterConfig(topology.clusterId()).orElse(null);
    if (configuration == null) {
      return Optional.empty();
    }
    final var registrations =
        state.getBrokerRegistrationState().registrations(topology.clusterId());
    final Map<String, BrokerRegistration> byId = new LinkedHashMap<>();
    registrations.forEach(registration -> byId.put(registration.brokerId(), registration));
    if (QuorumRecoveryCoordinator.hasLostQuorum(topology, byId)) {
      return quorumRecovery
          .planRecovery(topology, byId)
          .map(operations -> new PlannedChange("quorum-recovery", "", operations));
    }
    final var eligible =
        BrokerTopologyPlanner.eligibleRegistrations(
            state.getBrokerRegistrationState(), topology.clusterId());
    final var members = new ArrayList<>(topology.currentVoters());
    topology.learners().stream().filter(member -> !members.contains(member)).forEach(members::add);
    for (final var brokerId : members) {
      final var registration = byId.get(brokerId);
      if (registration != null && registration.status() != BrokerStatus.OFFLINE) {
        continue;
      }
      final var operations =
          planning.calculateBrokerRecovery(
              topology,
              brokerId,
              eligible.stream()
                  .filter(
                      candidate ->
                          !state
                              .getBrokerRegistrationState()
                              .isVoterExcluded(topology.clusterId(), candidate.brokerId()))
                  .toList());
      if (!operations.isEmpty()) {
        return Optional.of(new PlannedChange("recover", brokerId, operations));
      }
    }
    return rebalance(
        topology, configuration, registrations, planning.calculateOnlineBrokers(eligible));
  }

  private Optional<PlannedChange> rebalance(
      final PartitionTopology topology,
      final BrokerClusterConfig configuration,
      final List<BrokerRegistration> registrations,
      final List<BrokerRegistration> online) {
    if (registrations.stream()
        .anyMatch(registration -> registration.status() == BrokerStatus.DRAINING)) {
      return Optional.empty();
    }
    final var voters = planning.calculateVoterBrokers(configuration, online);
    if (voters.isEmpty()) {
      return Optional.empty();
    }
    final var desired =
        planning
            .calculateClusterCreation(
                new BrokerClusterCreation(
                    topology.clusterId(), configuration, topology.partitionId(), online, voters))
            .topology();
    if (!topology.initializing()
        && new HashSet<>(topology.currentVoters()).equals(new HashSet<>(desired.desiredVoters()))
        && new HashSet<>(topology.learners()).equals(new HashSet<>(desired.learners()))
        && topology.priorities().equals(desired.priorities())
        && topology.currentVoters().stream()
            .allMatch(
                voter -> {
                  final var observed = topology.replicaStates().get(voter);
                  return observed == null
                      || observed.priority() == topology.priorities().getOrDefault(voter, -1);
                })) {
      return Optional.empty();
    }
    long highestTerm = -1;
    String leaderId = "";
    for (final var registration : online) {
      for (final var partition : registration.partitions()) {
        if (partition.partitionId() == topology.partitionId()
            && partition.role() == MemberRole.LEADER
            && partition.term() > highestTerm) {
          highestTerm = partition.term();
          leaderId = registration.brokerId();
        }
      }
    }
    final var rebalance = new BrokerTopologyRebalance(topology, desired, leaderId);
    List<TopologyOperation> operations =
        (topology.initializing()
                ? planning.calculateTopologyRebalanceToTarget(rebalance)
                : planning.calculateTopologyRebalance(rebalance))
            .operations();
    if (operations.isEmpty()
        && topology.initializing()
        && voters.size() >= configuration.clusterSize()
        && topology.currentVoters().size() >= topology.replicationFactor()
        && desired.desiredPrimary().equals(leaderId)) {
      operations =
          List.of(
              TopologyOperation.transferLeader(topology.partitionId(), desired.desiredPrimary()));
    }
    return operations.isEmpty()
        ? Optional.empty()
        : Optional.of(
            new PlannedChange("rebalance", operations.getFirst().targetBroker(), operations));
  }

  private record PlannedChange(String kind, String brokerId, List<TopologyOperation> operations) {}
}
