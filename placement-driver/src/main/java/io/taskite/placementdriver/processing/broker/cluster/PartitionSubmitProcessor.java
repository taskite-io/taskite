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
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.BrokerTopologyOperationRequests;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.Writers;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.IntStream;

public final class PartitionSubmitProcessor implements TypedRecordProcessor<PartitionChangeRecord> {
  private final Writers writers;
  private static final String INVALID_PLAN =
      "Expected to submit topology change '%s' in cluster '%s' for partition '%d', but decoding the plan failed: %s";
  private static final String CONFLICTING_PLAN =
      "Expected to submit topology change '%s' in cluster '%s' for partition '%d', but its change id is already used by a different plan";
  private static final String PENDING_ADMIN_CHANGE =
      "Expected to submit topology change '%s' in cluster '%s', but a pending admin topology change is exclusive";
  private static final String PENDING_CLUSTER_CHANGE =
      "Expected to submit topology change '%s' in cluster '%s', but pending plans belong to a different topology change batch";
  private static final String PENDING_PARTITION_CHANGE =
      "Expected to submit topology change '%s' in cluster '%s' for partition '%d', but that partition already has a pending change";
  private static final String FREEZE_FAILED =
      "%s\nExpected to freeze topology change '%s' in cluster '%s' for partition '%d', but configuration preparation failed";
  private static final String MISSING_CLUSTER =
      "Expected to submit topology change '%s' for partition '%d', but clusterId is blank";
  private static final String MISSING_CHANGE_ID =
      "Expected to submit a topology change in cluster '%s' for partition '%d', but changeId is blank";
  private static final String INVALID_PARTITION =
      "Expected to submit topology change '%s' in cluster '%s' with a positive partitionId, but was '%d'";
  private static final String INVALID_INITIAL_OPERATIONS =
      "Expected to submit new topology change '%s' in cluster '%s' starting at operation index 0 with at least one operation, but received index '%d' and operation count '%d'";
  private static final String PARTITION_NOT_FOUND =
      "Expected to submit topology change '%s' in cluster '%s' for partition '%d', but the cluster or partition does not exist and the plan does not add the next adjacent partition";
  private static final String INVALID_OPERATION_IDENTITY =
      "Expected topology change '%s' in cluster '%s' to contain unique nonblank operation ids and nonblank target brokers, but operation '%s' has blank or duplicate identity (target broker '%s')";
  private static final String INVALID_SEQUENCE =
      "Expected to submit topology change '%s' in cluster '%s' for partition '%d' with a valid operation sequence, but operation '%s' of type '%s' at index '%d' targeting broker '%s' is invalid: %s";
  private static final String BROKER_NOT_REGISTERED = "target broker is not registered";
  private static final String INVALID_ADD = "ADD must be the first operation of a new partition";
  private static final String ALREADY_VOTER = "JOIN target is already a voter";
  private static final String MISSING_JOIN =
      "PROMOTE_LEARNER must follow JOIN for the target broker";
  private static final String NOT_MEMBER = "LEAVE target is not a member";
  private static final String LAST_VOTER = "LEAVE cannot remove the last voter";
  private static final String INVALID_PRIORITY =
      "RECONFIGURE_PRIORITY target must be a voter with positive priority";
  private static final String SINGLE_VOTER_PRIORITY =
      "Single-voter priority-only changes are not supported";
  private static final String NOT_VOTER = "leader transition target is not a voter";
  private static final String INVALID_RESET = "RESET_PEERS must be the only operation in its plan";
  private final ProcessingState state;
  private final KeyGenerator keyGenerator;
  private final TypedRecordProcessorContext context;
  private static final long DEFAULT_PLAN_TIMEOUT_MILLIS = Duration.ofMinutes(5).toMillis();

  public PartitionSubmitProcessor(final TypedRecordProcessorContext context) {
    this.context = context;
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
    keyGenerator = context.getKeyGenerator();
  }

  @Override
  public void processRecord(final TypedRecord<PartitionChangeRecord> record) {
    final PartitionChangeRecord value = record.getValue();
    final ChangePlan requested;
    try {
      final ChangePlan decoded =
          BrokerPartitionChangeRecordMapper.toPlan(value, BrokerPartitionChangeStatus.QUEUED);
      final long now = context.getProcessingTime();
      requested =
          decoded.startedAtEpochMillis() > 0 && decoded.deadlineEpochMillis() > 0
              ? decoded
              : decoded.withDeadline(now, now + DEFAULT_PLAN_TIMEOUT_MILLIS);
    } catch (final IllegalArgumentException error) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_ARGUMENT,
              INVALID_PLAN.formatted(
                  value.getChangeId(),
                  value.getClusterId(),
                  value.getPartitionId(),
                  error.getMessage()));
      return;
    }
    if (!validate(requested)) {
      return;
    }
    final var existing =
        state
            .getBrokerTopologyChangeState()
            .changePlan(requested.clusterId(), requested.changeId());
    if (existing.isPresent()) {
      if (!samePlan(existing.get(), requested)) {
        writers
            .rejection()
            .appendRejection(
                RejectionType.ALREADY_EXISTS,
                CONFLICTING_PLAN.formatted(
                    requested.changeId(), requested.clusterId(), requested.partitionId()));
        return;
      }
      return;
    }
    final var pendingPlans =
        state.getBrokerTopologyChangeState().pendingChangePlans(requested.clusterId());
    final String requestedBatch = BrokerClusterChangeIds.extractBatchKey(requested.changeId());
    if (requestedBatch.isBlank()) {
      // Automatic per-partition plan: only an in-flight admin batch is exclusive against it.
      // Concurrency across distinct partitions is intentionally allowed and guarded
      // per-partition by the hasPendingPlanForPartition check below.
      final boolean adminBatchPending =
          pendingPlans.stream()
              .anyMatch(
                  pending -> !BrokerClusterChangeIds.extractBatchKey(pending.changeId()).isBlank());
      if (adminBatchPending) {
        writers
            .rejection()
            .appendRejection(
                RejectionType.INVALID_STATE,
                PENDING_ADMIN_CHANGE.formatted(requested.changeId(), requested.clusterId()));
        return;
      }
    } else if (!pendingPlans.isEmpty()) {
      // Admin batch plan: every pending plan must belong to the same batch, preserving batch
      // atomicity and exclusivity against automatic plans and other admin batches.
      final boolean sameAdminBatch =
          pendingPlans.stream()
              .allMatch(
                  pending ->
                      requestedBatch.equals(
                          BrokerClusterChangeIds.extractBatchKey(pending.changeId())));
      if (!sameAdminBatch) {
        writers
            .rejection()
            .appendRejection(
                RejectionType.INVALID_STATE,
                PENDING_CLUSTER_CHANGE.formatted(requested.changeId(), requested.clusterId()));
        return;
      }
    }
    if (state
        .getBrokerTopologyChangeState()
        .hasPendingPlanForPartition(requested.clusterId(), requested.partitionId())) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_STATE,
              PENDING_PARTITION_CHANGE.formatted(
                  requested.changeId(), requested.clusterId(), requested.partitionId()));
      return;
    }
    try {
      final ChangePlan frozen = BrokerTopologyOperationRequests.freeze(state, requested);
      writers
          .state()
          .appendFollowUpEvent(
              keyGenerator.nextKey(),
              BrokerPartitionChangeIntent.SUBMITTED,
              BrokerPartitionChangeRecordMapper.fromPlan(frozen));
    } catch (final IllegalArgumentException | IllegalStateException error) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_STATE,
              FREEZE_FAILED.formatted(
                  error.getMessage(),
                  requested.changeId(),
                  requested.clusterId(),
                  requested.partitionId()));
    }
  }

  private static boolean samePlan(final ChangePlan left, final ChangePlan right) {
    return left.clusterId().equals(right.clusterId())
        && left.changeId().equals(right.changeId())
        && left.partitionId() == right.partitionId()
        && left.sourceTopologyVersion() == right.sourceTopologyVersion()
        && left.targetTopologyVersion() == right.targetTopologyVersion()
        && left.operations().size() == right.operations().size()
        && IntStream.range(0, left.operations().size())
            .allMatch(
                index -> {
                  final var l = left.operations().get(index);
                  final var r = right.operations().get(index);
                  return l.opId().equals(r.opId())
                      && l.type() == r.type()
                      && l.targetBroker().equals(r.targetBroker())
                      && l.priority() == r.priority()
                      && (r.configuration() == null || r.configuration().equals(l.configuration()));
                });
  }

  private boolean validate(final ChangePlan plan) {
    if (plan.clusterId().isBlank()) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_ARGUMENT,
              MISSING_CLUSTER.formatted(plan.changeId(), plan.partitionId()));
      return false;
    }
    if (plan.changeId().isBlank()) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_ARGUMENT,
              MISSING_CHANGE_ID.formatted(plan.clusterId(), plan.partitionId()));
      return false;
    }
    if (plan.partitionId() <= 0) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_ARGUMENT,
              INVALID_PARTITION.formatted(plan.changeId(), plan.clusterId(), plan.partitionId()));
      return false;
    }
    if (plan.nextOperationIndex() != 0 || plan.operations().isEmpty()) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_ARGUMENT,
              INVALID_INITIAL_OPERATIONS.formatted(
                  plan.changeId(),
                  plan.clusterId(),
                  plan.nextOperationIndex(),
                  plan.operations().size()));
      return false;
    }
    final var config = state.getBrokerClusterConfigState().findClusterConfig(plan.clusterId());
    final boolean addsNextPartition =
        config.filter(topology -> plan.partitionId() == topology.partitionSize() + 1).isPresent()
            && plan.operations().getFirst().type() == TopologyOperationType.ADD;
    if (config.isEmpty()
        || (plan.partitionId() > config.get().partitionSize() && !addsNextPartition)) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.NOT_FOUND,
              PARTITION_NOT_FOUND.formatted(plan.changeId(), plan.clusterId(), plan.partitionId()));
      return false;
    }
    final Set<String> opIds = new HashSet<>();
    for (final var operation : plan.operations()) {
      if (operation.opId().isBlank()
          || operation.targetBroker().isBlank()
          || !opIds.add(operation.opId())) {
        writers
            .rejection()
            .appendRejection(
                RejectionType.INVALID_ARGUMENT,
                INVALID_OPERATION_IDENTITY.formatted(
                    plan.changeId(), plan.clusterId(), operation.opId(), operation.targetBroker()));
        return false;
      }
    }
    return validateOperationSequence(plan);
  }

  private boolean validateOperationSequence(final ChangePlan plan) {
    final Set<String> registeredBrokers = new HashSet<>();
    state
        .getBrokerRegistrationState()
        .registrations(plan.clusterId())
        .forEach(registration -> registeredBrokers.add(registration.brokerId()));
    final boolean addPlan =
        plan.operations().stream()
            .anyMatch(operation -> operation.type() == TopologyOperationType.ADD);
    final Set<String> voters = new LinkedHashSet<>();
    final Set<String> learners = new LinkedHashSet<>();
    if (!addPlan) {
      state
          .getBrokerPartitionTopologyState()
          .partitionTopology(plan.clusterId(), plan.partitionId())
          .ifPresent(
              topology -> {
                voters.addAll(topology.currentVoters());
                learners.addAll(topology.learners());
              });
    }
    for (int index = 0; index < plan.operations().size(); index++) {
      final var operation = plan.operations().get(index);
      if (!registeredBrokers.contains(operation.targetBroker())) {
        return invalidSequence(plan, index, BROKER_NOT_REGISTERED);
      }
      switch (operation.type()) {
        case ADD -> {
          if (index != 0 || !voters.isEmpty()) {
            return invalidSequence(plan, index, INVALID_ADD);
          }
          learners.remove(operation.targetBroker());
          voters.add(operation.targetBroker());
        }
        case JOIN -> {
          if (voters.contains(operation.targetBroker())) {
            return invalidSequence(plan, index, ALREADY_VOTER);
          }
          learners.add(operation.targetBroker());
        }
        case PROMOTE_LEARNER -> {
          if (!learners.remove(operation.targetBroker())) {
            return invalidSequence(plan, index, MISSING_JOIN);
          }
          voters.add(operation.targetBroker());
        }
        case LEAVE -> {
          if (!voters.remove(operation.targetBroker())
              && !learners.remove(operation.targetBroker())) {
            return invalidSequence(plan, index, NOT_MEMBER);
          }
          if (voters.isEmpty()) {
            return invalidSequence(plan, index, LAST_VOTER);
          }
        }
        case RECONFIGURE_PRIORITY -> {
          if (!voters.contains(operation.targetBroker()) || operation.priority() <= 0) {
            return invalidSequence(plan, index, INVALID_PRIORITY);
          }
          if (voters.size() == 1) {
            return invalidSequence(plan, index, SINGLE_VOTER_PRIORITY);
          }
        }
        case TRANSFER_LEADER, STEP_DOWN_IF_NOT_PRIMARY -> {
          if (!voters.contains(operation.targetBroker())) {
            return invalidSequence(plan, index, NOT_VOTER);
          }
        }
        case RESET_PEERS -> {
          // Fenced disaster-recovery reset (M5/Q4): must be the only operation in its plan and
          // collapses membership to the single survivor.
          if (plan.operations().size() != 1) {
            return invalidSequence(plan, index, INVALID_RESET);
          }
          voters.clear();
          learners.clear();
          voters.add(operation.targetBroker());
        }
      }
    }
    return true;
  }

  private boolean invalidSequence(final ChangePlan plan, final int index, final String reason) {
    final var operation = plan.operations().get(index);
    writers
        .rejection()
        .appendRejection(
            RejectionType.INVALID_ARGUMENT,
            INVALID_SEQUENCE.formatted(
                plan.changeId(),
                plan.clusterId(),
                plan.partitionId(),
                operation.opId(),
                operation.type(),
                index,
                operation.targetBroker(),
                reason));
    return false;
  }
}
