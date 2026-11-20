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

import io.taskite.cluster.raft.RaftMember;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeTransport;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.BrokerTopologyOperationRequests;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.request.BrokerInstallPartitionAuthorityRequest;
import io.taskite.placementdriver.broker.request.BrokerReadPartitionConfigurationRequest;
import io.taskite.placementdriver.processing.member.PdMemberChangeTransport;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.broker.BrokerPartitionConfigurationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationType;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberExecutionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionExecutionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionExecutionRecord.Phase;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionExecutionRecord.Schedule;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.StreamProcessorLifecycleAware;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;
import io.taskite.topology.BrokerEndpoint;
import java.time.Duration;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared transport lifetime, durable quota reservation and post-commit result command delivery. */
public final class TopologyMembershipChangeExecutor implements StreamProcessorLifecycleAware {
  private static final Logger LOG = LoggerFactory.getLogger(TopologyMembershipChangeExecutor.class);
  private static final String READY_REQUEST = "Cannot send a READY topology execution";
  public static final int MAX_REQUESTS = 256;
  private final ProcessingState state;
  private final Writers writers;
  private final BooleanSupplier leader;
  private final Supplier<BrokerClusterChangeTransport> brokerTransports;
  private final Supplier<PdMemberChangeTransport> memberTransports;
  private final long partitionTimeout;
  private final long memberTimeout;
  private volatile ProcessingScheduleService scheduling;
  private volatile long generation;
  private BrokerClusterChangeTransport brokerTransport;
  private PdMemberChangeTransport memberTransport;

  public TopologyMembershipChangeExecutor(
      final TypedRecordProcessorContext context,
      final BooleanSupplier leader,
      final Supplier<BrokerClusterChangeTransport> brokerTransports,
      final Supplier<PdMemberChangeTransport> memberTransports,
      final Duration partitionTimeout,
      final Duration memberTimeout) {
    state = context.getProcessingState(ProcessingState.class);
    writers = context.getWriters();
    this.leader = leader;
    this.brokerTransports = brokerTransports;
    this.memberTransports = memberTransports;
    this.partitionTimeout = partitionTimeout.toMillis();
    this.memberTimeout = memberTimeout.toMillis();
  }

  public boolean active() {
    return scheduling != null && leader.getAsBoolean();
  }

  private boolean active(final long expected) {
    return expected == generation && active();
  }

  public List<RaftMember> members() {
    return memberTransport.members();
  }

  public String leaderId() {
    return memberTransport.leaderId();
  }

  public long term() {
    return memberTransport.term();
  }

  public long lastApplied() {
    return memberTransport.lastApplied();
  }

  public void beginPartition(
      final ChangePlan plan,
      final Schedule previous,
      final Phase phase,
      final BrokerEndpoint endpoint,
      final BrokerPartitionConfigurationRecord source,
      final long attempt,
      final long now) {
    if (!canReserve(attempt, now)) {
      partitionObservation(previous.ready(now + 1000));
      return;
    }
    final var request =
        new Schedule(
            previous.plan(), phase, attempt, now + 1000, now + partitionTimeout, endpoint, source);
    partitionObservation(request);
    final long expected = generation;
    writers.postCommit().append(() -> sendPartition(plan, request, expected));
  }

  public void beginMember(
      final MemberChangeScope scope,
      final MemberExecutionRecord.Schedule previous,
      final List<RaftPeer> peers,
      final long attempt,
      final long now) {
    if (!canReserve(attempt, now)) {
      memberObservation(scope, previous.ready(now + 1000));
      return;
    }
    final var request =
        new MemberExecutionRecord.Schedule(
            previous.operation(),
            MemberExecutionRecord.Phase.RPC,
            attempt,
            now + 1000,
            now + memberTimeout,
            previous.readySamples(),
            previous.sampledAt());
    memberObservation(scope, request);
    final long expected = generation;
    writers.postCommit().append(() -> sendMember(scope, request, peers, expected));
  }

  private boolean canReserve(final long attempt, final long now) {
    return active()
        && attempt > 0
        && state.getTopologyMembershipChangeState().reserved(now, MAX_REQUESTS) < MAX_REQUESTS;
  }

  private void partitionObservation(final Schedule request) {
    writers
        .state()
        .appendFollowUpEvent(
            0,
            BrokerPartitionChangeIntent.EXECUTION_UPDATED,
            new PartitionExecutionRecord().setSchedule(request));
  }

  private void memberObservation(
      final MemberChangeScope scope, final MemberExecutionRecord.Schedule request) {
    writers
        .state()
        .appendFollowUpEvent(
            0,
            MemberChangeIntent.EXECUTION_UPDATED,
            new MemberExecutionRecord().setScope(scope).setSchedule(request));
  }

  private void sendPartition(final ChangePlan plan, final Schedule request, final long expected) {
    if (!active(expected)) {
      return;
    }
    try {
      switch (request.phase()) {
        case APPLY, BOOTSTRAP ->
            brokerTransport
                .dispatchBrokerClusterChange(
                    request.endpoint(),
                    BrokerTopologyOperationRequests.build(plan, plan.nextOperation()))
                .onComplete(
                    (response, error) ->
                        partitionResult(
                            request,
                            expected,
                            error == null
                                && response != null
                                && response.applied()
                                && plan.nextOperation().opId().equals(response.operationId()),
                            null,
                            failure(request.phase(), error)));
        case READ ->
            brokerTransport
                .readConfiguration(
                    request.endpoint(),
                    new BrokerReadPartitionConfigurationRequest(
                        plan.clusterId(), plan.partitionId()))
                .onComplete(
                    (response, error) ->
                        partitionResult(
                            request,
                            expected,
                            error == null && response != null,
                            response,
                            failure(request.phase(), error)));
        case INSTALL -> {
          final var source = request.source();
          final String owner =
              plan.status() == BrokerPartitionChangeStatus.SUPERSEDING
                  ? plan.execution().successor()
                  : plan.changeId();
          brokerTransport
              .installAuthority(
                  request.endpoint(),
                  new BrokerInstallPartitionAuthorityRequest(
                      plan.clusterId(),
                      plan.partitionId(),
                      source.executionGeneration(),
                      source.executionGeneration() + 1,
                      owner,
                      source.recoveryEpoch()))
              .onComplete(
                  (response, error) ->
                      partitionResult(
                          request,
                          expected,
                          error == null && response != null,
                          response,
                          failure(request.phase(), error)));
        }
        case READY -> throw new IllegalStateException(READY_REQUEST);
      }
    } catch (final RuntimeException error) {
      partitionResult(request, expected, false, null, failure(request.phase(), error));
      LOG.debug(
          "Failed topology change '{}' partition '{}' attempt '{}'",
          plan.changeId(),
          plan.partitionId(),
          request.attempt(),
          error);
    }
  }

  private void partitionResult(
      final Schedule request,
      final long expected,
      final boolean success,
      final BrokerPartitionConfigurationRecord configuration,
      final PlanBlockedReason failure) {
    final var service = scheduling;
    if (service != null && active(expected)) {
      final var result =
          new PartitionExecutionRecord()
              .setSchedule(request)
              .setSuccess(success)
              .setConfiguration(configuration)
              .setFailure(failure);
      service.schedule(
          Duration.ZERO,
          writer -> {
            if (active(expected)) {
              writer.tryAppend(
                  0,
                  ValueType.PARTITION_CHANGE,
                  BrokerPartitionChangeIntent.EXECUTION_RESULT,
                  result);
            }
          });
    }
  }

  private void sendMember(
      final MemberChangeScope scope,
      final MemberExecutionRecord.Schedule request,
      final List<RaftPeer> peers,
      final long expected) {
    if (!active(expected)) {
      return;
    }
    try {
      final var future =
          request.operation().type() == MemberChangeOperationType.TRANSFER_LEADER
              ? memberTransport.transfer(request.operation().memberId())
              : memberTransport.update(peers);
      future.onComplete((ignored, error) -> memberResult(scope, request, expected, error));
    } catch (final RuntimeException error) {
      memberResult(scope, request, expected, error);
    }
  }

  private void memberResult(
      final MemberChangeScope scope,
      final MemberExecutionRecord.Schedule request,
      final long expected,
      final Throwable error) {
    final var service = scheduling;
    if (service == null || !active(expected)) {
      return;
    }
    if (error != null) {
      LOG.debug(
          "Member change '{}' operation '{}' attempt '{}' failed",
          request.operation().changeId(),
          request.operation().operationId(),
          request.attempt(),
          error);
    }
    final var result =
        new MemberExecutionRecord().setScope(scope).setSchedule(request).setSuccess(error == null);
    service.schedule(
        Duration.ZERO,
        writer -> {
          if (active(expected)) {
            writer.tryAppend(
                0, ValueType.MEMBER_CHANGE, MemberChangeIntent.EXECUTION_RESULT, result);
          }
        });
  }

  private static PlanBlockedReason failure(final Phase phase, final Throwable error) {
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      final String message = cause.getMessage();
      if (message == null) {
        continue;
      }
      if (phase == Phase.READ && message.contains(PlanBlockedReason.NO_QUORUM.name())) {
        return PlanBlockedReason.NO_QUORUM;
      }
      if (phase == Phase.READ && message.contains("NOT_LEADER")) {
        return PlanBlockedReason.LEADER_UNAVAILABLE;
      }
      if ((phase == Phase.APPLY || phase == Phase.BOOTSTRAP)
          && (message.contains("STALE_CONFIGURATION")
              || message.contains("PARTITION_OPERATION_SUPERSEDED"))) {
        return PlanBlockedReason.CONFIGURATION_CONFLICT;
      }
    }
    return switch (phase) {
      case READ -> PlanBlockedReason.CONFIGURATION_READ_UNAVAILABLE;
      case INSTALL -> PlanBlockedReason.AUTHORITY_INSTALL_UNAVAILABLE;
      default -> PlanBlockedReason.BROKER_UNAVAILABLE;
    };
  }

  @Override
  public void onRecovered(final ProcessingScheduleService scheduling) {
    onClose();
    brokerTransport = brokerTransports == null ? null : brokerTransports.get();
    memberTransport = memberTransports == null ? null : memberTransports.get();
    this.scheduling = scheduling;
  }

  @Override
  public void onClose() {
    generation++;
    scheduling = null;
    if (brokerTransport != null) {
      try {
        brokerTransport.close();
      } catch (final Exception error) {
        LOG.warn("Failed to close topology execution transport", error);
      }
      brokerTransport = null;
    }
    memberTransport = null;
  }
}
