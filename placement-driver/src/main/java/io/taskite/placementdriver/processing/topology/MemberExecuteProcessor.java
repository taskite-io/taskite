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

import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.state.appliers.ClusterChangeOperationProgress;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.immutable.TopologyMembershipChangeState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperation;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationStatus;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationType;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberExecutionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberExecutionRecord.Phase;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberExecutionRecord.Schedule;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;
import io.taskite.topology.MemberRole;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Advances Broker and PD member tasks; operation Appliers persist durable business progress. */
public final class MemberExecuteProcessor implements TypedRecordProcessor<MemberChangeRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(MemberExecuteProcessor.class);
  private static final long OBSERVATION_INTERVAL = 1000;
  private static final long ADVANCE_TIMEOUT = 15_000;
  private static final long MAX_PROMOTION_LAG = 16;
  private static final String MISSING_ENDPOINT =
      "Expected to execute PD member operation '%s', but member '%s' has no registered Raft endpoint";

  private final ProcessingState state;
  private final Writers writers;
  private final TopologyMembershipChangeExecutor executor;

  public MemberExecuteProcessor(
      final TypedRecordProcessorContext context, final TopologyMembershipChangeExecutor executor) {
    state = context.getProcessingState(ProcessingState.class);
    writers = context.getWriters();
    this.executor = executor;
  }

  @Override
  public void processRecord(final TypedRecord<MemberChangeRecord> record) {
    if (!executor.active()) {
      return;
    }
    final var change = current(record.getValue().getScope(), record.getValue().getResourceId());
    if (change == null
        || !TopologyMembershipChangeState.identity(change)
            .equals(TopologyMembershipChangeState.identity(record.getValue()))) {
      return;
    }
    var entry =
        state
            .getTopologyMembershipChangeState()
            .member(change.getScope(), change.getResourceId())
            .orElse(null);
    final long now = record.getTimestamp();
    if (entry == null
        || !entry.operation().equals(TopologyMembershipChangeState.identity(change))
        || entry.dueAt() > now) {
      return;
    }
    if (entry.phase() == Phase.WAIT_ADVANCE && entry.deadline() > now) {
      return;
    }
    if (change.getScope() == MemberChangeScope.BROKER && !brokerConverged(change)) {
      update(change.getScope(), entry.ready(now + OBSERVATION_INTERVAL));
      return;
    }
    if (change.getNextOperationIndex() >= change.getOperations().size()) {
      advance(change, null, entry, now);
      return;
    }
    final var operation = change.getOperations().get(change.getNextOperationIndex());
    if (change.getScope() == MemberChangeScope.BROKER) {
      if (brokerConverged(change)) {
        advance(change, operation, entry, now);
      } else {
        update(change.getScope(), entry.ready(now + OBSERVATION_INTERVAL));
      }
      return;
    }
    if (converged(operation)) {
      advance(change, operation, entry, now);
      topology();
      return;
    }
    if (entry.phase() == Phase.RPC && entry.deadline() > now) {
      update(
          change.getScope(),
          new Schedule(
              entry.operation(),
              Phase.RPC,
              entry.attempt(),
              Math.min(now + OBSERVATION_INTERVAL, entry.deadline()),
              entry.deadline(),
              entry.readySamples(),
              entry.sampledAt()));
      return;
    }
    if (operation.getType() == MemberChangeOperationType.MEMBER_PROMOTE_VOTER) {
      entry = sample(entry, now);
      if (entry.readySamples() < 2) {
        progress(change, operation, MemberChangeStatus.WAITING_CATCH_UP, entry, now);
        return;
      }
    }
    if (change.getStatus() != MemberChangeStatus.IN_PROGRESS
        || operation.getStatus() != MemberChangeOperationStatus.IN_PROGRESS) {
      progress(change, operation, MemberChangeStatus.IN_PROGRESS, entry, now);
      return;
    }
    final List<RaftPeer> peers;
    try {
      peers =
          operation.getType() == MemberChangeOperationType.TRANSFER_LEADER
              ? List.of()
              : peers(operation);
    } catch (final IllegalStateException missingEndpoint) {
      update(change.getScope(), entry.ready(now + OBSERVATION_INTERVAL));
      LOG.debug(
          "Cannot prepare PD change '{}' operation '{}'",
          change.getChangeId(),
          operation.getOperationId(),
          missingEndpoint);
      return;
    }
    executor.beginMember(change.getScope(), entry, peers, record.getPosition(), now);
  }

  public TypedRecordProcessor<MemberExecutionRecord> results() {
    return new MemberExecutionResultProcessor();
  }

  public final class MemberExecutionResultProcessor
      implements TypedRecordProcessor<MemberExecutionRecord> {
    @Override
    public void processRecord(final TypedRecord<MemberExecutionRecord> record) {
      if (!executor.active()) {
        return;
      }
      final var expected = record.getValue().getSchedule();
      final var change =
          expected == null
              ? null
              : current(record.getValue().getScope(), expected.operation().resourceId());
      if (expected == null || change == null) {
        return;
      }
      final var entry =
          state
              .getTopologyMembershipChangeState()
              .member(change.getScope(), change.getResourceId())
              .orElse(null);
      if (entry == null
          || entry.phase() != Phase.RPC
          || expected.phase() != Phase.RPC
          || !entry.operation().equals(expected.operation())
          || !entry.operation().equals(TopologyMembershipChangeState.identity(change))
          || entry.attempt() != expected.attempt()) {
        return;
      }
      update(change.getScope(), entry.ready(record.getTimestamp() + OBSERVATION_INTERVAL));
      if (record.getValue().isSuccess()) {
        topology();
      }
      // RPC success is not an operation ACK; the next EXECUTE observes actual Raft effects.
    }
  }

  private Schedule sample(final Schedule entry, final long now) {
    final var members = topologyMembers();
    final var member = members.get(entry.operation().memberId());
    final var leaderMember = members.get(executor.leaderId());
    final long applied =
        leaderMember == null ? executor.lastApplied() : leaderMember.getLastApplied();
    if (member == null
        || member.getLastApplied() < entry.operation().barrierIndex()
        || Math.max(0, applied - member.getLastApplied()) > MAX_PROMOTION_LAG) {
      return entry.sample(0, 0);
    }
    if (entry.readySamples() == 0 || now - entry.sampledAt() >= OBSERVATION_INTERVAL) {
      return entry.sample(Math.min(2, entry.readySamples() + 1), now);
    }
    return entry;
  }

  private boolean converged(final MemberChangeOperation operation) {
    final var role = roles().get(operation.getMemberId());
    return switch (operation.getType()) {
      case MEMBER_ADD_LEARNER ->
          role == RaftRole.LEARNER || role == RaftRole.FOLLOWER || role == RaftRole.LEADER;
      case MEMBER_PROMOTE_VOTER -> role == RaftRole.FOLLOWER || role == RaftRole.LEADER;
      case TRANSFER_LEADER -> operation.getMemberId().equals(executor.leaderId());
      case MEMBER_LEAVE, MEMBER_REMOVE -> role == null;
      case MEMBER_JOIN -> role != null;
    };
  }

  private List<RaftPeer> peers(final MemberChangeOperation operation) {
    final var roles = roles();
    switch (operation.getType()) {
      case MEMBER_LEAVE, MEMBER_REMOVE -> roles.remove(operation.getMemberId());
      default ->
          roles.put(
              operation.getMemberId(),
              operation.getType() == MemberChangeOperationType.MEMBER_PROMOTE_VOTER
                  ? RaftRole.FOLLOWER
                  : RaftRole.LEARNER);
    }
    final var endpoints = topologyMembers();
    final List<RaftPeer> peers = new ArrayList<>();
    roles.forEach(
        (id, role) -> {
          final var endpoint = endpoints.get(id);
          if (endpoint == null || endpoint.getHost().isBlank() || endpoint.getRaftPort() <= 0) {
            throw new IllegalStateException(
                MISSING_ENDPOINT.formatted(operation.getOperationId(), id));
          }
          peers.add(
              new RaftPeer(
                  id, endpoint.getHost(), endpoint.getRaftPort(), role != RaftRole.LEARNER));
        });
    return List.copyOf(peers);
  }

  private Map<String, RaftRole> roles() {
    final Map<String, RaftRole> roles = new LinkedHashMap<>();
    executor.members().forEach(member -> roles.put(member.memberId(), member.role()));
    return roles;
  }

  private Map<String, MemberTopologyRecord.MemberRecord> topologyMembers() {
    final Map<String, MemberTopologyRecord.MemberRecord> members = new LinkedHashMap<>();
    state
        .getPlacementDriverTopologyState()
        .topology()
        .ifPresent(
            topology ->
                topology.getMembers().forEach(member -> members.put(member.getMemberId(), member)));
    return members;
  }

  private void topology() {
    final var members =
        executor.members().stream()
            .map(
                member ->
                    new MemberTopologyRecord.MemberRecord()
                        .setMemberId(member.memberId())
                        .setRole(MemberRole.parse(member.role().name())))
            .toList();
    writers
        .command()
        .appendFollowUpCommand(
            0,
            ValueType.TOPOLOGY,
            TopologyIntent.UPDATE,
            new MemberTopologyRecord()
                .setMembers(members)
                .setLeaderId(executor.leaderId())
                .setLocalMemberId(executor.leaderId())
                .setTerm(executor.term()));
  }

  private void advance(
      final MemberChangeRecord change,
      final MemberChangeOperation operation,
      final Schedule entry,
      final long now) {
    final var advanced = change.copy();
    if (operation != null) {
      advanced
          .getOperations()
          .get(change.getNextOperationIndex())
          .setStatus(MemberChangeOperationStatus.COMPLETED);
      advanced.setNextOperationIndex(change.getNextOperationIndex() + 1);
    }
    advanced.setStatus(
        advanced.getNextOperationIndex() >= advanced.getOperations().size()
            ? MemberChangeStatus.COMPLETED
            : MemberChangeStatus.IN_PROGRESS);
    submit(advanced, entry, now);
  }

  private void progress(
      final MemberChangeRecord change,
      final MemberChangeOperation operation,
      final MemberChangeStatus status,
      final Schedule entry,
      final long now) {
    if (change.getStatus() == status
        && operation.getStatus() == MemberChangeOperationStatus.IN_PROGRESS) {
      update(change.getScope(), entry.ready(now + OBSERVATION_INTERVAL));
      return;
    }
    final var updated = change.copy().setStatus(status);
    updated
        .getOperations()
        .get(change.getNextOperationIndex())
        .setStatus(MemberChangeOperationStatus.IN_PROGRESS);
    submit(updated, entry, now);
  }

  private void submit(final MemberChangeRecord progress, final Schedule entry, final long now) {
    update(
        progress.getScope(),
        new Schedule(
            entry.operation(),
            Phase.WAIT_ADVANCE,
            entry.attempt(),
            now + ADVANCE_TIMEOUT,
            now + ADVANCE_TIMEOUT,
            entry.readySamples(),
            entry.sampledAt()));
    writers
        .command()
        .appendFollowUpCommand(
            0,
            ValueType.MEMBER_CHANGE,
            progress.getNextOperationIndex() > entry.operation().index()
                ? TopologyMembershipChangeBehavior.command(entry.operation().type())
                : MemberChangeIntent.ADVANCE,
            progress);
  }

  private void update(final MemberChangeScope scope, final Schedule entry) {
    writers
        .state()
        .appendFollowUpEvent(
            0,
            MemberChangeIntent.EXECUTION_UPDATED,
            new MemberExecutionRecord().setScope(scope).setSchedule(entry));
  }

  private MemberChangeRecord current(final MemberChangeScope scope, final String clusterId) {
    return state
        .getMemberChangeState()
        .active(scope, clusterId)
        .filter(change -> !change.isTerminal())
        .orElse(null);
  }

  private boolean brokerConverged(final MemberChangeRecord change) {
    return change.getChildPartitionChangeIds().stream()
        .allMatch(
            id ->
                Optional.ofNullable(
                        ClusterChangeOperationProgress.progressPlan(
                            state, change.getResourceId(), id))
                    .map(plan -> plan.status() == BrokerPartitionChangeStatus.COMPLETED)
                    .orElse(false));
  }
}
