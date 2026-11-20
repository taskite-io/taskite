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
package io.taskite.placementdriver.common.codec;

import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.broker.topology.ReplicaLifecycleState;
import io.taskite.placementdriver.broker.topology.ReplicaState;
import io.taskite.protocol.record.MessageHeaderDecoder;
import io.taskite.protocol.record.MessageHeaderEncoder;
import io.taskite.protocol.record.PartitionTopologyRecordDecoder;
import io.taskite.protocol.record.PartitionTopologyRecordEncoder;
import io.taskite.topology.BrokerPartitionElectionState;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import io.taskite.topology.RaftConfigurationReference;
import io.taskite.topology.RaftNodeState;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * SBE codec for a persisted {@link PartitionTopology} stored in the {@code default} column family.
 * Voter/learner sets are carried in repeating groups; the status is stored by name so the enum can
 * evolve without a wire break.
 */
public final class PartitionTopologyRecordCodec {
  private PartitionTopologyRecordCodec() {}

  public static byte[] toBytes(final PartitionTopology topology) {
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final PartitionTopologyRecordEncoder body = new PartitionTopologyRecordEncoder();
    body.wrapAndApplyHeader(buffer, 0, header)
        .partitionId(topology.partitionId())
        .replicationFactor(topology.replicationFactor())
        .recoveryEpoch(topology.recoveryEpoch())
        .initializing((short) (topology.initializing() ? 1 : 0));
    final PartitionTopologyRecordEncoder.DesiredVotersEncoder desired =
        body.desiredVotersCount(topology.desiredVoters().size());
    for (final String brokerId : topology.desiredVoters()) {
      desired.next().brokerId(brokerId);
    }
    final PartitionTopologyRecordEncoder.CurrentVotersEncoder current =
        body.currentVotersCount(topology.currentVoters().size());
    for (final String brokerId : topology.currentVoters()) {
      current.next().brokerId(brokerId);
    }
    final PartitionTopologyRecordEncoder.LearnersEncoder learners =
        body.learnersCount(topology.learners().size());
    for (final String brokerId : topology.learners()) {
      learners.next().brokerId(brokerId);
    }
    final PartitionTopologyRecordEncoder.PrioritiesEncoder priorities =
        body.prioritiesCount(topology.priorities().size());
    for (final Map.Entry<String, Integer> priority : topology.priorities().entrySet()) {
      priorities.next().priority(priority.getValue()).brokerId(priority.getKey());
    }
    final PartitionTopologyRecordEncoder.ReplicaStatesEncoder replicaStates =
        body.replicaStatesCount(topology.replicaStates().size());
    for (final Map.Entry<String, ReplicaState> entry : topology.replicaStates().entrySet()) {
      final ReplicaState state = entry.getValue();
      replicaStates
          .next()
          .matchIndex(state.matchIndex())
          .lag(state.lag())
          .lastReportTimeEpochMillis(state.lastReportTimeEpochMillis())
          .readyForPromotion((short) (state.readyForPromotion() ? 1 : 0))
          .priority(state.priority())
          .replicaRecoveryEpoch(state.recoveryEpoch())
          .configurationIndex(state.election().configurationIndex())
          .desiredPriority(state.election().desiredPriority())
          .targetPriority(state.election().targetPriority())
          .serviceReady((short) (state.election().serviceReady() ? 1 : 0))
          .observedAtEpochMillis(state.election().observedAtEpochMillis())
          .brokerId(entry.getKey())
          .role(state.role().name())
          .lifecycleState(state.lifecycle().name())
          .lastOpId(state.lastOpId())
          .lastOpStatus(state.lastOpStatus().name())
          .configurationDigest(state.election().configurationDigest())
          .nodeState(state.election().nodeState().name())
          .leaderId(state.election().leaderId());
    }
    // v5 (dark): the fenced-recovery group and trailing recovery var-data are always written so the
    // buffer layout is complete, but stay empty until the fenced quorum-recovery state machine
    // (Pass 3) populates them. Groups must precede var-data in SBE wire order.
    body.fencedBrokerIdsCount(0);
    putVar(body::putClusterId, topology.clusterId());
    putVar(body::putStatus, topology.status().name());
    putVar(body::putDesiredPrimary, topology.desiredPrimary());
    putVar(body::putRecoveryPhase, "");
    putVar(body::putSurvivorBrokerId, "");
    return Arrays.copyOf(
        buffer.byteArray(), MessageHeaderEncoder.ENCODED_LENGTH + body.encodedLength());
  }

  public static PartitionTopology toTopology(final byte[] encoded) {
    final UnsafeBuffer buffer = new UnsafeBuffer(encoded);
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    final PartitionTopologyRecordDecoder body = new PartitionTopologyRecordDecoder();
    header.wrap(buffer, 0);
    body.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
    final int partitionId = body.partitionId();
    final int replicationFactor = body.replicationFactor();
    final long recoveryEpoch = body.recoveryEpoch();
    final boolean initializing = header.version() >= 14 && body.initializing() == 1;
    final List<String> desiredVoters = new ArrayList<>();
    for (final PartitionTopologyRecordDecoder.DesiredVotersDecoder group : body.desiredVoters()) {
      desiredVoters.add(group.brokerId());
    }
    final List<String> currentVoters = new ArrayList<>();
    for (final PartitionTopologyRecordDecoder.CurrentVotersDecoder group : body.currentVoters()) {
      currentVoters.add(group.brokerId());
    }
    final List<String> learners = new ArrayList<>();
    for (final PartitionTopologyRecordDecoder.LearnersDecoder group : body.learners()) {
      learners.add(group.brokerId());
    }
    final Map<String, Integer> priorities = new LinkedHashMap<>();
    for (final PartitionTopologyRecordDecoder.PrioritiesDecoder group : body.priorities()) {
      priorities.put(group.brokerId(), group.priority());
    }
    final Map<String, ReplicaState> replicaStates = new LinkedHashMap<>();
    for (final PartitionTopologyRecordDecoder.ReplicaStatesDecoder group : body.replicaStates()) {
      // SBE var-data getters advance the decoder. Read each field once, in wire order.
      final String brokerId = group.brokerId();
      final MemberRole role = MemberRole.parse(group.role());
      final ReplicaLifecycleState lifecycle = ReplicaLifecycleState.parse(group.lifecycleState());
      replicaStates.put(
          brokerId,
          new ReplicaState(
              new BrokerPartitionState.Status(
                  role,
                  0,
                  BrokerPartitionLifecycle.valueOf(
                      (lifecycle == null ? ReplicaLifecycleState.ABSENT : lifecycle).name()),
                  group.priority(),
                  group.replicaRecoveryEpoch()),
              new BrokerPartitionState.Replication(
                  group.matchIndex(), group.lag(), group.readyForPromotion() == 1),
              new BrokerPartitionState.Operation(
                  group.lastOpId(), BrokerTopologyOperationStatus.parse(group.lastOpStatus())),
              group.lastReportTimeEpochMillis(),
              header.version() < 10
                  ? BrokerPartitionElectionState.UNKNOWN
                  : new BrokerPartitionElectionState(
                      new RaftConfigurationReference(
                          group.configurationIndex(), group.configurationDigest()),
                      new BrokerPartitionElectionState.Priorities(
                          group.desiredPriority(), group.targetPriority()),
                      new BrokerPartitionElectionState.Observation(
                          RaftNodeState.parse(group.nodeState()),
                          group.leaderId(),
                          group.serviceReady() == 1,
                          group.observedAtEpochMillis()))));
    }
    // v5 (dark): drain the fenced-broker group in wire order so the trailing var-data stays
    // aligned;
    // the fencing set is not surfaced until the recovery state machine (Pass 3) consumes it. Old
    // v3/v4
    // records report actingVersion < 5, so the generated decoder yields an empty group here.
    for (final PartitionTopologyRecordDecoder.FencedBrokerIdsDecoder group :
        body.fencedBrokerIds()) {
      group.brokerId();
    }
    final String clusterId = body.clusterId();
    final String status = body.status();
    final String desiredPrimary = body.desiredPrimary();
    // v5 (dark): read the trailing recovery var-data to keep the buffer position consistent.
    body.recoveryPhase();
    body.survivorBrokerId();
    return new PartitionTopology(
        new PartitionTopology.Identity(clusterId, partitionId),
        new PartitionTopology.Placement(replicationFactor, desiredVoters, desiredPrimary),
        new PartitionTopology.Membership(currentVoters, learners, priorities, recoveryEpoch),
        parseStatus(status),
        replicaStates,
        initializing);
  }

  private static PartitionStatus parseStatus(final String status) {
    if (status == null || status.isEmpty()) {
      return PartitionStatus.ACTIVE;
    }
    try {
      return PartitionStatus.valueOf(status);
    } catch (final IllegalArgumentException e) {
      return PartitionStatus.ACTIVE;
    }
  }

  @FunctionalInterface
  private interface VarWriter {
    void put(byte[] src, int offset, int length);
  }

  private static void putVar(final VarWriter writer, final String value) {
    final byte[] bytes = bytes(value);
    writer.put(bytes, 0, bytes.length);
  }

  private static byte[] bytes(final String value) {
    return value == null || value.isEmpty() ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
  }
}
