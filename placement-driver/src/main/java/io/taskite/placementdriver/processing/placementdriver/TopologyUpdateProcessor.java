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
package io.taskite.placementdriver.processing.placementdriver;

import io.taskite.placementdriver.PlacementDriverCluster;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationStatus;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationType;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyUpdateType;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RegistrationStatus;
import io.taskite.util.HostPort;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

public final class TopologyUpdateProcessor implements TypedRecordProcessor<MemberTopologyRecord> {
  private final Writers writers;
  private static final String INVALID_MEMBER_COUNT =
      "INVALID_PD_MEMBER_REGISTRATION: Expected topology update '%s' to contain exactly one PD member, but received '%d'";
  private static final String INVALID_MEMBER =
      "INVALID_PD_MEMBER_REGISTRATION: Expected topology update '%s' to contain memberId, host, positive grpcPort and raftPort, and incarnationId, but received member '%s', host '%s', gRPC port '%d', Raft port '%d' and incarnation '%s'";
  private static final String REGISTER_BEFORE_RENEW =
      "PD_MEMBER_MUST_REGISTER_BEFORE_RENEW: Expected to renew PD member '%s' with incarnation '%s', but no registration was found";
  private static final String STALE_ENDPOINT =
      "STALE_PD_MEMBER_ENDPOINT_EPOCH: Expected to renew PD member '%s' with incarnation '%s' and endpoint epoch '%d', but the registered incarnation is '%s' with endpoint epoch '%d'";
  private static final String MEMBER_ID_CONFLICT =
      "PD_MEMBER_ID_CONFLICT: Member '%s' is registered at host '%s', gRPC port '%d' and Raft port '%d'; cannot register at host '%s', gRPC port '%d' and Raft port '%d'";
  private final ProcessingState state;

  public TopologyUpdateProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
  }

  @Override
  public void processRecord(final TypedRecord<MemberTopologyRecord> record) {
    final MemberTopologyRecord requested = record.getValue();
    final MemberTopologyRecord updated;
    if (requested.getUpdateType() == TopologyUpdateType.REGISTER
        || requested.getUpdateType() == TopologyUpdateType.RENEW
        || requested.getUpdateType() == TopologyUpdateType.REBIND) {
      updated = mergeRegistration(record);
      if (updated == null) {
        return;
      }
    } else {
      updated = mergeConfiguration(requested);
    }
    writers.state().appendFollowUpEvent(record.getKey(), TopologyIntent.UPDATED, updated);
    writers
        .response()
        .writeEventOnCommand(record.getKey(), TopologyIntent.UPDATED, updated, record);
  }

  private MemberTopologyRecord mergeRegistration(final TypedRecord<MemberTopologyRecord> record) {
    final MemberTopologyRecord requested = record.getValue();
    if (requested.getMembers().size() != 1) {
      reject(
          record,
          RejectionType.INVALID_ARGUMENT,
          INVALID_MEMBER_COUNT.formatted(requested.getUpdateType(), requested.getMembers().size()));
      return null;
    }
    final MemberTopologyRecord.MemberRecord incoming = requested.getMembers().getFirst();
    if (incoming.getMemberId().isBlank()
        || incoming.getHost().isBlank()
        || incoming.getGrpcPort() <= 0
        || incoming.getRaftPort() <= 0
        || incoming.getIncarnationId().isBlank()) {
      reject(
          record,
          RejectionType.INVALID_ARGUMENT,
          INVALID_MEMBER.formatted(
              requested.getUpdateType(),
              incoming.getMemberId(),
              incoming.getHost(),
              incoming.getGrpcPort(),
              incoming.getRaftPort(),
              incoming.getIncarnationId()));
      return null;
    }
    final MemberTopologyRecord current =
        state.getPlacementDriverTopologyState().topology().orElseGet(MemberTopologyRecord::new);
    final LinkedHashMap<String, MemberTopologyRecord.MemberRecord> members = membersOf(current);
    final MemberTopologyRecord.MemberRecord existing = members.get(incoming.getMemberId());
    if (existing != null
        && (!HostPort.normalizeHost(incoming.getHost())
                .equals(HostPort.normalizeHost(existing.getHost()))
            || incoming.getGrpcPort() != existing.getGrpcPort()
            || incoming.getRaftPort() != existing.getRaftPort())) {
      reject(
          record,
          RejectionType.INVALID_STATE,
          MEMBER_ID_CONFLICT.formatted(
              incoming.getMemberId(),
              existing.getHost(),
              existing.getGrpcPort(),
              existing.getRaftPort(),
              incoming.getHost(),
              incoming.getGrpcPort(),
              incoming.getRaftPort()));
      return null;
    }
    if (existing == null && requested.getUpdateType() == TopologyUpdateType.RENEW) {
      reject(
          record,
          RejectionType.INVALID_STATE,
          REGISTER_BEFORE_RENEW.formatted(incoming.getMemberId(), incoming.getIncarnationId()));
      return null;
    }
    final boolean newIncarnation =
        existing != null && !incoming.getIncarnationId().equals(existing.getIncarnationId());
    if (newIncarnation && requested.getUpdateType() == TopologyUpdateType.RENEW) {
      reject(
          record,
          RejectionType.INVALID_STATE,
          STALE_ENDPOINT.formatted(
              incoming.getMemberId(),
              incoming.getIncarnationId(),
              incoming.getEndpointEpoch(),
              existing.getIncarnationId(),
              existing.getEndpointEpoch()));
      return null;
    }
    final long endpointEpoch =
        existing == null
            ? Math.max(1, incoming.getEndpointEpoch())
            : newIncarnation ? existing.getEndpointEpoch() + 1 : existing.getEndpointEpoch();
    incoming
        .setRole(existing == null ? MemberRole.REGISTERED : existing.getRole())
        .setStatus(RegistrationStatus.ONLINE)
        .setEndpointEpoch(endpointEpoch);
    members.put(incoming.getMemberId(), incoming);
    return copyHeader(current)
        .setLocalMemberId(incoming.getMemberId())
        .setUpdateType(newIncarnation ? TopologyUpdateType.REBIND : requested.getUpdateType())
        .setConfigurationEpoch(current.getConfigurationEpoch())
        .setMembers(members.values().stream().toList());
  }

  private MemberTopologyRecord mergeConfiguration(final MemberTopologyRecord requested) {
    final MemberTopologyRecord current =
        state.getPlacementDriverTopologyState().topology().orElseGet(MemberTopologyRecord::new);
    final LinkedHashMap<String, MemberTopologyRecord.MemberRecord> existing = membersOf(current);
    requested
        .getMembers()
        .forEach(
            member -> {
              final MemberTopologyRecord.MemberRecord observed = existing.get(member.getMemberId());
              if (observed != null) {
                // Raft configuration snapshots may have been captured before REGISTER/REBIND.
                // Once registered, endpoint identity belongs to registration, not configuration.
                if (member.getHost().isBlank() || observed.getEndpointEpoch() > 0) {
                  member
                      .setEndpoint(observed.getEndpoint())
                      .setHost(observed.getHost())
                      .setHttpPort(observed.getHttpPort())
                      .setGrpcPort(observed.getGrpcPort())
                      .setRaftPort(observed.getRaftPort())
                      .setIncarnationId(observed.getIncarnationId())
                      .setEndpointEpoch(observed.getEndpointEpoch())
                      .setStatus(observed.getStatus());
                }
                member
                    .setCommitIndex(observed.getCommitIndex())
                    .setLastApplied(observed.getLastApplied())
                    .setLastLogIndex(observed.getLastLogIndex())
                    .setReadyForPromotion(observed.isReadyForPromotion());
              }
            });
    final var members = new ArrayList<>(requested.getMembers());
    existing.values().stream()
        // Learner promotion commits removal from learners before adding the voter.
        // Keep registration while that exact durable operation is still executing.
        .filter(
            member ->
                member.getRole() == MemberRole.REGISTERED
                    || (member.getRole() == MemberRole.LEARNER
                        && promotionInProgress(member.getMemberId())))
        .filter(
            member ->
                members.stream()
                    .noneMatch(observed -> observed.getMemberId().equals(member.getMemberId())))
        .forEach(members::add);
    requested.setMembers(members);
    requested.getMembers().stream()
        .filter(member -> requested.getLeaderId().equals(member.getMemberId()))
        .map(MemberTopologyRecord.MemberRecord::getEndpoint)
        .filter(endpoint -> !endpoint.isBlank())
        .findFirst()
        .ifPresent(requested::setLeaderEndpoint);
    return requested
        .setUpdateType(TopologyUpdateType.CONFIGURATION)
        .setConfigurationEpoch(
            current.getConfigurationEpoch() + (membershipChanged(current, requested) ? 1 : 0));
  }

  private boolean promotionInProgress(final String memberId) {
    return state
        .getMemberChangeState()
        .active(MemberChangeScope.PLACEMENT_DRIVER, PlacementDriverCluster.CLUSTER_ID)
        .filter(change -> change.getStatus() == MemberChangeStatus.IN_PROGRESS)
        .filter(change -> change.getNextOperationIndex() < change.getOperations().size())
        .map(change -> change.getOperations().get(change.getNextOperationIndex()))
        .filter(operation -> operation.getType() == MemberChangeOperationType.MEMBER_PROMOTE_VOTER)
        .filter(operation -> operation.getStatus() == MemberChangeOperationStatus.IN_PROGRESS)
        .filter(operation -> memberId.equals(operation.getMemberId()))
        .isPresent();
  }

  private void reject(
      final TypedRecord<MemberTopologyRecord> record,
      final RejectionType type,
      final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(record, type, reason);
  }

  private static LinkedHashMap<String, MemberTopologyRecord.MemberRecord> membersOf(
      final MemberTopologyRecord topology) {
    final LinkedHashMap<String, MemberTopologyRecord.MemberRecord> members = new LinkedHashMap<>();
    topology.getMembers().forEach(member -> members.put(member.getMemberId(), member));
    return members;
  }

  private static MemberTopologyRecord copyHeader(final MemberTopologyRecord topology) {
    return new MemberTopologyRecord()
        .setLocalMemberId(topology.getLocalMemberId())
        .setLeaderId(topology.getLeaderId())
        .setLeaderEndpoint(topology.getLeaderEndpoint())
        .setTerm(topology.getTerm());
  }

  private static boolean membershipChanged(
      final MemberTopologyRecord current, final MemberTopologyRecord requested) {
    return !committedMembership(current).equals(committedMembership(requested));
  }

  private static Map<String, MemberRole> committedMembership(final MemberTopologyRecord topology) {
    final Map<String, MemberRole> members = new TreeMap<>();
    topology
        .getMembers()
        .forEach(
            member -> {
              final MemberRole role =
                  switch (member.getRole()) {
                    case LEADER, FOLLOWER, VOTER -> MemberRole.VOTER;
                    case LEARNER -> MemberRole.LEARNER;
                    default -> MemberRole.UNKNOWN;
                  };
              if (role != MemberRole.UNKNOWN) {
                members.put(member.getMemberId(), role);
              }
            });
    return members;
  }
}
