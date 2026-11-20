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

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeIds;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService.BrokerClusterConfigurationChange;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService.BrokerClusterCreation;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService.BrokerTopologyQuorumRecovery;
import io.taskite.placementdriver.broker.cluster.BrokerClusterConfiguration;
import io.taskite.placementdriver.broker.cluster.BrokerClusterInvariantValidator;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.BrokerTopologyOperationRequests;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.broker.topology.QuorumRecoveryCoordinator;
import io.taskite.placementdriver.common.codec.ChangePlanRecordCodec;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperation;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationStatus;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationType;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOutcome;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.protocol.record.value.placementdriver.topology.ClusterConfigurationOutcome;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.Writers;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.MemberRole;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class TopologyConfigureClusterProcessor
    implements TypedRecordProcessor<ClusterConfigRecord> {
  private final Writers writers;
  private static final String CLUSTER_NOT_FOUND =
      "CLUSTER_NOT_REGISTERED: Expected to configure cluster '%s', but it is not registered";
  private static final String FREEZE_FAILED =
      "%s\nExpected to configure cluster '%s' for change '%s', but freezing embedded topology plans failed";
  private static final String INVALID_ADMIN_REQUEST =
      "Expected to apply admin operation '%s' to cluster '%s' with a changeId and positive requestedAtEpochMillis, but received change '%s' and timestamp '%d'";
  private static final String CHANGE_IN_PROGRESS =
      "TOPOLOGY_CHANGE_IN_PROGRESS:%s\nExpected to apply admin operation '%s' for change '%s' in cluster '%s', but the listed topology changes are still pending";
  private static final String UNSUPPORTED_OPERATION =
      "Expected to configure cluster '%s' for change '%s', but admin operation '%s' is unsupported";
  private static final String OVERLAPPING_BROKERS =
      "INCONSISTENT_CONFIGURATION_PATCH: Expected to configure cluster '%s' for change '%s' with distinct additions and removals, but they overlap (additions '%s', removals '%s')";
  private static final String ADDED_BROKER_OFFLINE =
      "BROKER_COUNT_UNREACHABLE: Expected to configure cluster '%s' for change '%s', but one or more added brokers '%s' are missing or not ONLINE";
  private static final String REMOVED_BROKER_NOT_FOUND =
      "BROKER_NOT_FOUND: Expected to configure cluster '%s' for change '%s', but one or more removed brokers '%s' are not registered";
  private static final String COUNT_UNREACHABLE =
      "BROKER_COUNT_UNREACHABLE: Expected to configure cluster '%s' for change '%s' with broker count '%d', but the target roster contains '%d' brokers";
  private static final String PARTITION_DECREASE =
      "PARTITION_SIZE_DECREASE_NOT_ALLOWED: Expected to configure cluster '%s' for change '%s', but partition count cannot decrease from '%d' to '%d'";
  private static final String CONFIRMATION_REQUIRED =
      "CONFIRMATION_REQUIRED: Expected to recover quorum in cluster '%s' for change '%s', but confirmation must be true";
  private static final String FENCING_PROOF_REQUIRED =
      "FENCING_PROOF_REQUIRED: Expected to recover quorum in cluster '%s' for change '%s', but fencingProof is blank";
  private static final String INVALID_PARTITION =
      "INVALID_PARTITION_ID: Expected to recover quorum in cluster '%s' for change '%s', but requestedPartitionId is missing";
  private static final String PARTITION_NOT_FOUND =
      "PARTITION_NOT_FOUND: Expected to recover quorum in cluster '%s' for partition '%d' and change '%s', but no partition topology was found";
  private static final String RECOVERY_IN_PROGRESS =
      "QUORUM_RECOVERY_IN_PROGRESS: Expected to recover quorum in cluster '%s' for partition '%d' and change '%s', but recovery change '%s' is already pending";
  private static final String QUORUM_NOT_LOST =
      "QUORUM_NOT_LOST: Expected to recover quorum in cluster '%s' for partition '%d', but the current voters still have quorum";
  private static final String NO_SURVIVOR =
      "NO_SURVIVOR: Expected to recover quorum in cluster '%s' for partition '%d', but survivor broker '%s' is missing, not a current voter or not ONLINE";
  private static final String INVALID_CONFIGURATION =
      "Expected to configure cluster '%s' for change '%s' with broker count '%d', partition count '%d' and replicationFactor '%d', but configuration validation failed: %s";
  private static final String INSUFFICIENT_ONLINE_BROKERS =
      "INSUFFICIENT_ONLINE_BROKERS: Expected to configure cluster '%s' for change '%s' with replicationFactor '%d', but only '%d' eligible ONLINE voters are available";
  private static final String BELOW_QUORUM =
      "Expected to configure cluster '%s' for change '%s' with replicationFactor '%d', but it cannot be reduced below current quorum '%d'";
  private static final String PENDING_CHANGE =
      "TOPOLOGY_CHANGE_IN_PROGRESS\nExpected to configure cluster '%s', but an active cluster change or pending partition plan already exists";
  private static final String INSUFFICIENT_ROSTER =
      "Expected to configure cluster '%s' for change '%s' with replicationFactor '%d', but target voter roster '%s' no longer has enough ONLINE brokers";
  private static final String INVALID_EMBEDDED_PLANS =
      "Expected to configure cluster '%s' for change '%s' at source topology version '%d', but embedded plan '%s' has a different identity, source version or duplicate partition (plan cluster '%s', partition '%d', source version '%d')";
  private static final String INVALID_EMBEDDED_ENCODING =
      "Expected to configure cluster '%s' for change '%s', but decoding an embedded topology plan failed: %s";
  private final ProcessingState state;
  private final KeyGenerator keyGenerator;
  public static final String CONFIGURATION_PATCH = "CONFIGURATION_PATCH";
  public static final String REBALANCE = "REBALANCE";
  public static final String QUORUM_RECOVERY = "QUORUM_RECOVERY";
  private static final long CHANGE_TIMEOUT_MILLIS = 5 * 60 * 1_000L;

  private final BrokerClusterChangePlanningService planningService;

  public TopologyConfigureClusterProcessor(
      final TypedRecordProcessorContext context,
      final BrokerClusterChangePlanningService planningService) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
    keyGenerator = context.getKeyGenerator();
    this.planningService = planningService;
  }

  @Override
  public void processRecord(final TypedRecord<ClusterConfigRecord> record) {
    ClusterConfigRecord value = record.getValue();
    final var current = state.getBrokerClusterConfigState().findClusterConfig(value.getClusterId());
    if (current.isEmpty()) {
      reject(record, RejectionType.NOT_FOUND, CLUSTER_NOT_FOUND.formatted(value.getClusterId()));
      return;
    }
    if (!value.getAdminOperation().isBlank()) {
      value = calculateAdminEvent(record, value, current.orElseThrow());
      if (value == null) {
        return;
      }
    }
    if (!validateConfiguration(record, value, current.orElseThrow())
        || !validEmbeddedPlans(record, value)) {
      return;
    }
    try {
      value.setEncodedChangePlans(
          value.getEncodedChangePlans().stream()
              .map(encoded -> ChangePlanRecordCodec.fromBytes(Base64.getDecoder().decode(encoded)))
              .map(plan -> BrokerTopologyOperationRequests.freeze(state, plan))
              .map(ChangePlanRecordCodec::toBytes)
              .map(Base64.getEncoder()::encodeToString)
              .toList());
    } catch (final IllegalArgumentException | IllegalStateException error) {
      reject(
          record,
          RejectionType.INVALID_STATE,
          FREEZE_FAILED.formatted(error.getMessage(), value.getClusterId(), value.getChangeId()));
      return;
    }
    appendMemberChangeIfNeeded(value, current.orElseThrow());
    writers.state().appendFollowUpEvent(record.getKey(), TopologyIntent.CLUSTER_CONFIGURED, value);
    writers
        .response()
        .writeEventOnCommand(record.getKey(), TopologyIntent.CLUSTER_CONFIGURED, value, record);
  }

  private void appendMemberChangeIfNeeded(
      final ClusterConfigRecord value, final BrokerClusterConfig current) {
    final boolean memberRequest =
        CONFIGURATION_PATCH.equals(value.getAdminOperation())
            && (!value.getRequestedBrokerAdditions().isEmpty()
                || !value.getRequestedBrokerRemovals().isEmpty()
                || value.getRequestedBrokerCount() != null);
    if (!memberRequest || value.getChangeId().isBlank()) {
      return;
    }
    final List<String> childChangeIds =
        value.getEncodedChangePlans().stream()
            .map(encoded -> Base64.getDecoder().decode(encoded))
            .map(ChangePlanRecordCodec::fromBytes)
            .map(ChangePlan::changeId)
            .toList();
    final List<MemberChangeOperation> operations = new ArrayList<>();
    value.getVoterRoster().stream()
        .filter(memberId -> !current.voterRoster().contains(memberId))
        .forEach(
            memberId ->
                operations.add(memberOperation(MemberChangeOperationType.MEMBER_JOIN, memberId)));
    value
        .getRemovedBrokers()
        .forEach(
            memberId ->
                operations.add(memberOperation(MemberChangeOperationType.MEMBER_LEAVE, memberId)));
    final boolean immediate = childChangeIds.isEmpty();
    if (immediate) {
      operations.forEach(operation -> operation.setStatus(MemberChangeOperationStatus.COMPLETED));
    }
    final MemberChangeRecord memberChange =
        new MemberChangeRecord()
            .setScope(MemberChangeScope.BROKER)
            .setResourceId(value.getClusterId())
            .setChangeId(value.getChangeId())
            .setRequestedAdditions(value.getRequestedBrokerAdditions())
            .setRequestedRemovals(value.getRequestedBrokerRemovals())
            .setRequestedCount(value.getRequestedBrokerCount())
            .setRequestedAtEpochMillis(value.getRequestedAtEpochMillis())
            .setSourceEpoch(Math.max(0L, current.rosterEpoch()))
            .setSourceVoters(current.voterRoster())
            .setTargetEpoch(Math.max(0L, current.rosterEpoch()) + 1)
            .setTargetCount(value.getClusterSize())
            .setTargetVoters(value.getVoterRoster())
            .setRemovedMembers(value.getRemovedBrokers())
            .setStatus(immediate ? MemberChangeStatus.COMPLETED : MemberChangeStatus.PENDING)
            .setNextOperationIndex(immediate ? operations.size() : 0)
            .setOperations(operations)
            .setChildPartitionChangeIds(childChangeIds)
            .setOutcome(MemberChangeOutcome.ACCEPTED);
    writers
        .state()
        .appendFollowUpEvent(
            keyGenerator.nextKey(),
            ValueType.MEMBER_CHANGE,
            MemberChangeIntent.PLANNED,
            memberChange);
  }

  private static MemberChangeOperation memberOperation(
      final MemberChangeOperationType type, final String memberId) {
    return new MemberChangeOperation()
        .setOperationId(type.name().toLowerCase() + "-" + memberId)
        .setType(type)
        .setMemberId(memberId);
  }

  private ClusterConfigRecord calculateAdminEvent(
      final TypedRecord<ClusterConfigRecord> record,
      final ClusterConfigRecord command,
      final BrokerClusterConfig current) {
    if (command.getChangeId().isBlank() || command.getRequestedAtEpochMillis() <= 0) {
      return reject(
          record,
          RejectionType.INVALID_ARGUMENT,
          INVALID_ADMIN_REQUEST.formatted(
              command.getAdminOperation(),
              command.getClusterId(),
              command.getChangeId(),
              command.getRequestedAtEpochMillis()));
    }
    final List<BrokerRegistration> registrations =
        state.getBrokerRegistrationState().registrations(command.getClusterId());
    if (QUORUM_RECOVERY.equals(command.getAdminOperation())) {
      // Fenced disaster recovery restores availability before ordinary topology work can resume.
      return calculateQuorumRecovery(record, command, current, registrations);
    }
    final var activeClusterChange =
        state.getBrokerTopologyChangeState().activeClusterChange(command.getClusterId());
    final var activeMemberChange =
        state
            .getMemberChangeState()
            .active(MemberChangeRecord.MemberChangeScope.BROKER, command.getClusterId());
    final List<String> pendingChangeIds =
        activeMemberChange
            .map(change -> List.of(change.getChangeId()))
            .orElseGet(
                () ->
                    activeClusterChange
                        .map(change -> List.of(change.changeId()))
                        .orElseGet(
                            () ->
                                state
                                    .getBrokerTopologyChangeState()
                                    .pendingChangePlans(command.getClusterId())
                                    .stream()
                                    .map(ChangePlan::changeId)
                                    .distinct()
                                    .toList()));
    if (!pendingChangeIds.isEmpty()) {
      return reject(
          record,
          RejectionType.INVALID_STATE,
          CHANGE_IN_PROGRESS.formatted(
              String.join(",", pendingChangeIds),
              command.getAdminOperation(),
              command.getChangeId(),
              command.getClusterId()));
    }
    return switch (command.getAdminOperation()) {
      case CONFIGURATION_PATCH ->
          calculateConfigurationPatch(record, command, current, registrations);
      case REBALANCE -> calculateRebalance(record, command, current, registrations);
      default ->
          reject(
              record,
              RejectionType.INVALID_ARGUMENT,
              UNSUPPORTED_OPERATION.formatted(
                  command.getClusterId(), command.getChangeId(), command.getAdminOperation()));
    };
  }

  private ClusterConfigRecord calculateConfigurationPatch(
      final TypedRecord<ClusterConfigRecord> record,
      final ClusterConfigRecord command,
      final BrokerClusterConfig current,
      final List<BrokerRegistration> registrations) {
    final List<String> additions =
        command.getRequestedBrokerAdditions().stream().distinct().toList();
    final List<String> removals = command.getRequestedBrokerRemovals().stream().distinct().toList();
    if (additions.stream().anyMatch(removals::contains)) {
      return reject(
          record,
          RejectionType.INVALID_ARGUMENT,
          OVERLAPPING_BROKERS.formatted(
              command.getClusterId(), command.getChangeId(), additions, removals));
    }
    final Map<String, BrokerRegistration> byId = registrationsById(registrations);
    if (additions.stream()
        .anyMatch(
            brokerId ->
                !byId.containsKey(brokerId)
                    || byId.get(brokerId).status() != BrokerStatus.ONLINE)) {
      return reject(
          record,
          RejectionType.INVALID_STATE,
          ADDED_BROKER_OFFLINE.formatted(command.getClusterId(), command.getChangeId(), additions));
    }
    if (removals.stream().anyMatch(brokerId -> !byId.containsKey(brokerId))) {
      return reject(
          record,
          RejectionType.NOT_FOUND,
          REMOVED_BROKER_NOT_FOUND.formatted(
              command.getClusterId(), command.getChangeId(), removals));
    }

    final LinkedHashSet<String> targetRoster = new LinkedHashSet<>(current.voterRoster());
    final LinkedHashSet<String> originalRoster = new LinkedHashSet<>(current.voterRoster());
    targetRoster.removeAll(removals);
    targetRoster.addAll(additions);
    final boolean brokerAction =
        !additions.isEmpty() || !removals.isEmpty() || command.getRequestedBrokerCount() != null;
    final int clusterSize;
    if (command.getRequestedBrokerCount() != null) {
      clusterSize = command.getRequestedBrokerCount();
      if (additions.isEmpty() && removals.isEmpty()) {
        // An explicit resize may readmit a returned startup; automatic roster repair may not.
        resizeRoster(
            targetRoster,
            clusterSize,
            registrations.stream()
                .filter(
                    registration ->
                        !state
                            .getBrokerRegistrationState()
                            .isLearnerExcluded(
                                command.getClusterId(),
                                registration.brokerId(),
                                registration.incarnationId()))
                .toList());
      } else if (targetRoster.size() != clusterSize) {
        return reject(
            record,
            RejectionType.INVALID_STATE,
            COUNT_UNREACHABLE.formatted(
                command.getClusterId(), command.getChangeId(), clusterSize, targetRoster.size()));
      }
    } else {
      clusterSize =
          brokerAction
              ? current.clusterSize()
                  + Math.toIntExact(additions.stream().filter(targetRoster::contains).count())
                  - Math.toIntExact(removals.stream().filter(originalRoster::contains).count())
              : current.clusterSize();
    }
    if (brokerAction && !current.voterRoster().isEmpty() && targetRoster.size() != clusterSize) {
      return reject(
          record,
          RejectionType.INVALID_STATE,
          COUNT_UNREACHABLE.formatted(
              command.getClusterId(), command.getChangeId(), clusterSize, targetRoster.size()));
    }
    final int partitionSize =
        command.getRequestedPartitionCount() == null
            ? current.partitionSize()
            : command.getRequestedPartitionCount();
    final int replicationFactor =
        command.getRequestedReplicationFactor() == null
            ? current.replicationFactor()
            : command.getRequestedReplicationFactor();
    if (partitionSize < current.partitionSize()) {
      return reject(
          record,
          RejectionType.INVALID_STATE,
          PARTITION_DECREASE.formatted(
              command.getClusterId(),
              command.getChangeId(),
              current.partitionSize(),
              partitionSize));
    }
    final List<String> effectiveRoster =
        brokerAction ? List.copyOf(targetRoster) : current.voterRoster();
    final BrokerClusterConfig target =
        new BrokerClusterConfig(
            clusterSize,
            partitionSize,
            replicationFactor,
            effectiveRoster,
            BrokerClusterConfig.MANAGED_ROSTER_EPOCH);
    final LinkedHashSet<String> removed = new LinkedHashSet<>(removals);
    originalRoster.stream()
        .filter(brokerId -> !targetRoster.contains(brokerId))
        .forEach(removed::add);
    return prepareConfigurationEvent(
        record,
        command,
        current,
        target,
        registrations,
        brokerAction,
        List.copyOf(removed),
        "configuration-patch");
  }

  private ClusterConfigRecord calculateRebalance(
      final TypedRecord<ClusterConfigRecord> record,
      final ClusterConfigRecord command,
      final BrokerClusterConfig current,
      final List<BrokerRegistration> registrations) {
    return prepareConfigurationEvent(
        record, command, current, current, registrations, false, List.of(), "rebalance");
  }

  private ClusterConfigRecord calculateQuorumRecovery(
      final TypedRecord<ClusterConfigRecord> record,
      final ClusterConfigRecord command,
      final BrokerClusterConfig current,
      final List<BrokerRegistration> registrations) {
    if (!"true".equalsIgnoreCase(command.getConfirmation())) {
      return reject(
          record,
          RejectionType.INVALID_ARGUMENT,
          CONFIRMATION_REQUIRED.formatted(command.getClusterId(), command.getChangeId()));
    }
    if (command.getFencingProof().isBlank()) {
      return reject(
          record,
          RejectionType.INVALID_ARGUMENT,
          FENCING_PROOF_REQUIRED.formatted(command.getClusterId(), command.getChangeId()));
    }
    if (command.getRequestedPartitionId() == null) {
      return reject(
          record,
          RejectionType.INVALID_ARGUMENT,
          INVALID_PARTITION.formatted(command.getClusterId(), command.getChangeId()));
    }
    final int partitionId = command.getRequestedPartitionId();
    final PartitionTopology topology =
        state
            .getBrokerPartitionTopologyState()
            .partitionTopology(command.getClusterId(), partitionId)
            .orElse(null);
    if (topology == null) {
      return reject(
          record,
          RejectionType.NOT_FOUND,
          PARTITION_NOT_FOUND.formatted(
              command.getClusterId(), partitionId, command.getChangeId()));
    }
    final ChangePlan previous =
        state.getBrokerTopologyChangeState().pendingChangePlans(command.getClusterId()).stream()
            .filter(plan -> plan.partitionId() == partitionId)
            .findFirst()
            .orElse(null);
    if (previous != null && previous.isQuorumRecovery()) {
      return reject(
          record,
          RejectionType.INVALID_STATE,
          RECOVERY_IN_PROGRESS.formatted(
              command.getClusterId(), partitionId, command.getChangeId(), previous.changeId()));
    }
    final Map<String, BrokerRegistration> byId = registrationsById(registrations);
    if (!QuorumRecoveryCoordinator.hasLostQuorum(topology, byId)) {
      return reject(
          record,
          RejectionType.INVALID_STATE,
          QUORUM_NOT_LOST.formatted(command.getClusterId(), partitionId));
    }
    final String survivor =
        command.getSurvivorBrokerId().isBlank()
            ? QuorumRecoveryCoordinator.chooseSurvivor(topology, byId).orElse("")
            : command.getSurvivorBrokerId();
    if (survivor.isBlank()
        || !topology.currentVoters().contains(survivor)
        || !byId.containsKey(survivor)
        || byId.get(survivor).status() != BrokerStatus.ONLINE) {
      return reject(
          record,
          RejectionType.INVALID_STATE,
          NO_SURVIVOR.formatted(command.getClusterId(), partitionId, survivor));
    }
    final int recoveryEpoch = (int) Math.min(Integer.MAX_VALUE, state.getVersion() + 1);
    final List<TopologyOperation> operations =
        planningService
            .calculateQuorumRecovery(
                new BrokerTopologyQuorumRecovery(topology, byId, survivor, recoveryEpoch))
            .operations();
    ChangePlan plan = changePlan(command, "quorum-recovery", partitionId, operations);
    if (previous != null) {
      plan =
          plan.withExecution(
              new ChangePlan.Execution(
                  previous.execution().generation() + 1,
                  0,
                  previous.changeId(),
                  "",
                  previous.rootChangeId(),
                  PlanBlockedReason.NONE,
                  0));
    }
    return applyTarget(
            command,
            current,
            current.voterRoster(),
            List.of(),
            List.of(plan),
            ClusterConfigurationOutcome.RECOVERY_PLANNED)
        .setSurvivorBrokerId(survivor);
  }

  private ClusterConfigRecord prepareConfigurationEvent(
      final TypedRecord<ClusterConfigRecord> record,
      final ClusterConfigRecord command,
      final BrokerClusterConfig current,
      final BrokerClusterConfig target,
      final List<BrokerRegistration> registrations,
      final boolean rosterChanged,
      final List<String> removedBrokers,
      final String operation) {
    final BrokerClusterConfiguration configuration =
        new BrokerClusterConfiguration(
            command.getClusterId(),
            target.clusterSize(),
            target.partitionSize(),
            target.replicationFactor());
    final String validation =
        BrokerClusterInvariantValidator.validateBrokerClusterConfiguration(configuration);
    if (validation != null) {
      return reject(
          record,
          RejectionType.INVALID_ARGUMENT,
          INVALID_CONFIGURATION.formatted(
              command.getClusterId(),
              command.getChangeId(),
              target.clusterSize(),
              target.partitionSize(),
              target.replicationFactor(),
              validation));
    }
    final List<BrokerRegistration> eligible =
        registrations.stream()
            .filter(registration -> registration.status() == BrokerStatus.ONLINE)
            .filter(registration -> !removedBrokers.contains(registration.brokerId()))
            .filter(
                registration ->
                    command.getRequestedBrokerAdditions().contains(registration.brokerId())
                        || !state
                            .getBrokerRegistrationState()
                            .isLearnerExcluded(
                                command.getClusterId(),
                                registration.brokerId(),
                                registration.incarnationId()))
            .toList();
    final List<BrokerRegistration> voters = planningService.calculateVoterBrokers(target, eligible);
    if (voters.size() < target.replicationFactor()) {
      return reject(
          record,
          RejectionType.INVALID_STATE,
          INSUFFICIENT_ONLINE_BROKERS.formatted(
              command.getClusterId(),
              command.getChangeId(),
              target.replicationFactor(),
              voters.size()));
    }
    final List<ChangePlan> plans = new ArrayList<>();
    for (int partitionId = 1; partitionId <= target.partitionSize(); partitionId++) {
      final var creation =
          planningService.calculateClusterCreation(
              new BrokerClusterCreation(
                  command.getClusterId(), target, partitionId, eligible, voters));
      final PartitionTopology existing =
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology(command.getClusterId(), partitionId)
              .orElse(null);
      final List<TopologyOperation> operations;
      if (existing == null) {
        operations = creation.operations();
      } else {
        final var change =
            new BrokerClusterConfigurationChange(
                existing,
                creation.topology(),
                registrations,
                observedLeader(eligible, partitionId));
        operations =
            (REBALANCE.equals(command.getAdminOperation())
                    ? planningService.calculateExplicitRebalance(change)
                    : planningService.calculateConfigurationChange(change))
                .operations();
      }
      if (!operations.isEmpty()) {
        plans.add(changePlan(command, operation, partitionId, operations));
      }
    }
    final boolean changed =
        target.clusterSize() != current.clusterSize()
            || target.partitionSize() != current.partitionSize()
            || target.replicationFactor() != current.replicationFactor()
            || rosterChanged
            || !removedBrokers.isEmpty()
            || !plans.isEmpty();
    return applyTarget(
        command,
        target,
        rosterChanged ? target.voterRoster() : current.voterRoster(),
        removedBrokers,
        plans,
        changed
            ? ClusterConfigurationOutcome.CHANGE_PLANNED
            : ClusterConfigurationOutcome.NO_CHANGES);
  }

  private ClusterConfigRecord applyTarget(
      final ClusterConfigRecord command,
      final BrokerClusterConfig target,
      final List<String> voterRoster,
      final List<String> removedBrokers,
      final List<ChangePlan> plans,
      final ClusterConfigurationOutcome outcome) {
    final BrokerClusterConfig current =
        state.getBrokerClusterConfigState().clusterConfig(command.getClusterId());
    return command
        .setClusterSize(target.clusterSize())
        .setPartitionSize(target.partitionSize())
        .setReplicationFactor(target.replicationFactor())
        .setVoterRoster(voterRoster)
        .setRemovedBrokers(removedBrokers)
        .setSourceTopologyVersion(state.getVersion())
        .setSourceClusterSize(current.clusterSize())
        .setSourcePartitionSize(current.partitionSize())
        .setSourceReplicationFactor(current.replicationFactor())
        .setSourceVoterRoster(current.voterRoster())
        .setEncodedChangePlans(
            plans.stream()
                .map(ChangePlanRecordCodec::toBytes)
                .map(Base64.getEncoder()::encodeToString)
                .toList())
        .setRosterManaged(true)
        .setOutcome(outcome);
  }

  private ChangePlan changePlan(
      final ClusterConfigRecord command,
      final String operation,
      final int partitionId,
      final List<TopologyOperation> operations) {
    final String planId =
        BrokerClusterChangeIds.createAdminChangeId(operation, command.getChangeId(), partitionId);
    return new ChangePlan(
        new ChangePlan.Definition(
            new ChangePlan.Identity(command.getClusterId(), planId, partitionId),
            new ChangePlan.Versions(state.getVersion(), state.getVersion() + 1),
            BrokerClusterChangeIds.scopeOperations(planId, operations),
            operations.stream().anyMatch(op -> op.type() == TopologyOperationType.ADD)
                ? ChangePlan.Purpose.BOOTSTRAP
                : ChangePlan.Purpose.MIGRATION),
        new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
        new ChangePlan.Deadline(
            command.getRequestedAtEpochMillis(),
            command.getRequestedAtEpochMillis() + CHANGE_TIMEOUT_MILLIS));
  }

  private boolean validateConfiguration(
      final TypedRecord<ClusterConfigRecord> record,
      final ClusterConfigRecord value,
      final BrokerClusterConfig current) {
    final String validation =
        BrokerClusterInvariantValidator.validateBrokerClusterConfiguration(
            new BrokerClusterConfiguration(
                value.getClusterId(),
                value.getClusterSize(),
                value.getPartitionSize(),
                value.getReplicationFactor()));
    if (validation != null) {
      reject(
          record,
          RejectionType.INVALID_ARGUMENT,
          INVALID_CONFIGURATION.formatted(
              value.getClusterId(),
              value.getChangeId(),
              value.getClusterSize(),
              value.getPartitionSize(),
              value.getReplicationFactor(),
              validation));
      return false;
    }
    if (value.getPartitionSize() < current.partitionSize()) {
      reject(
          record,
          RejectionType.INVALID_STATE,
          PARTITION_DECREASE.formatted(
              value.getClusterId(),
              value.getChangeId(),
              current.partitionSize(),
              value.getPartitionSize()));
      return false;
    }
    final int currentQuorum = current.replicationFactor() / 2 + 1;
    if (value.getReplicationFactor() < current.replicationFactor()
        && value.getReplicationFactor() < currentQuorum) {
      reject(
          record,
          RejectionType.INVALID_ARGUMENT,
          BELOW_QUORUM.formatted(
              value.getClusterId(),
              value.getChangeId(),
              value.getReplicationFactor(),
              currentQuorum));
      return false;
    }
    if (value.getAdminOperation().isBlank()
        && (state
                .getBrokerTopologyChangeState()
                .activeClusterChange(value.getClusterId())
                .isPresent()
            || !state
                .getBrokerTopologyChangeState()
                .pendingChangePlans(value.getClusterId())
                .isEmpty())) {
      reject(record, RejectionType.INVALID_STATE, PENDING_CHANGE.formatted(value.getClusterId()));
      return false;
    }
    final Map<String, BrokerRegistration> byId =
        registrationsById(state.getBrokerRegistrationState().registrations(value.getClusterId()));
    if (!QUORUM_RECOVERY.equals(value.getAdminOperation())
        && !value.getVoterRoster().isEmpty()
        && value.getVoterRoster().stream()
                .map(byId::get)
                .filter(Objects::nonNull)
                .filter(registration -> registration.status() == BrokerStatus.ONLINE)
                .count()
            < value.getReplicationFactor()) {
      reject(
          record,
          RejectionType.INVALID_STATE,
          INSUFFICIENT_ROSTER.formatted(
              value.getClusterId(),
              value.getChangeId(),
              value.getReplicationFactor(),
              value.getVoterRoster()));
      return false;
    }
    return true;
  }

  private boolean validEmbeddedPlans(
      final TypedRecord<ClusterConfigRecord> record, final ClusterConfigRecord value) {
    if (value.getEncodedChangePlans().isEmpty()) {
      return true;
    }
    final HashSet<Integer> partitions = new HashSet<>();
    try {
      for (final String encoded : value.getEncodedChangePlans()) {
        final ChangePlan plan =
            ChangePlanRecordCodec.fromBytes(Base64.getDecoder().decode(encoded));
        if (!value.getClusterId().equals(plan.clusterId())
            || !BrokerClusterChangeIds.extractOperationId(plan.changeId())
                .equals(value.getChangeId())
            || plan.sourceTopologyVersion() != value.getSourceTopologyVersion()
            || !partitions.add(plan.partitionId())) {
          reject(
              record,
              RejectionType.INVALID_ARGUMENT,
              INVALID_EMBEDDED_PLANS.formatted(
                  value.getClusterId(),
                  value.getChangeId(),
                  value.getSourceTopologyVersion(),
                  plan.changeId(),
                  plan.clusterId(),
                  plan.partitionId(),
                  plan.sourceTopologyVersion()));
          return false;
        }
      }
    } catch (final RuntimeException error) {
      reject(
          record,
          RejectionType.INVALID_ARGUMENT,
          INVALID_EMBEDDED_ENCODING.formatted(
              value.getClusterId(), value.getChangeId(), error.getMessage()));
      return false;
    }
    return true;
  }

  private static Map<String, BrokerRegistration> registrationsById(
      final List<BrokerRegistration> registrations) {
    final Map<String, BrokerRegistration> byId = new LinkedHashMap<>();
    registrations.forEach(registration -> byId.put(registration.brokerId(), registration));
    return byId;
  }

  private static void resizeRoster(
      final LinkedHashSet<String> roster,
      final int targetSize,
      final List<BrokerRegistration> registrations) {
    while (roster.size() > targetSize) {
      roster.remove(roster.getLast());
    }
    registrations.stream()
        .filter(registration -> registration.status() == BrokerStatus.ONLINE)
        .map(BrokerRegistration::brokerId)
        .sorted()
        .filter(brokerId -> !roster.contains(brokerId))
        .limit(Math.max(0, targetSize - roster.size()))
        .forEach(roster::add);
  }

  private static String observedLeader(
      final List<BrokerRegistration> registrations, final int partitionId) {
    long highestTerm = -1;
    String leader = "";
    for (final BrokerRegistration registration : registrations) {
      for (final BrokerPartitionState partition : registration.partitions()) {
        if (partition.partitionId() == partitionId
            && partition.role() == MemberRole.LEADER
            && partition.term() > highestTerm) {
          highestTerm = partition.term();
          leader = registration.brokerId();
        }
      }
    }
    return leader;
  }

  private ClusterConfigRecord reject(
      final TypedRecord<ClusterConfigRecord> record,
      final RejectionType type,
      final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(record, type, reason);
    return null;
  }
}
