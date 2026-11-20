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
package io.taskite.placementdriver.common.mapper;

import static io.taskite.placementdriver.protocol.BrokerPartitionElectionState.newBuilder;

import io.taskite.placementdriver.protocol.BrokerPartitionElectionState.Builder;
import io.taskite.placementdriver.protocol.BrokerPartitionElectionStateOrBuilder;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.topology.BrokerPartitionElectionState;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.RaftConfigurationReference;
import io.taskite.topology.RaftNodeState;
import java.time.Instant;
import java.util.List;

public final class BrokerPartitionStateMapper {
  private BrokerPartitionStateMapper() {}

  public static List<BrokerPartitionState> fromRecord(final BrokerRegistrationRecord registration) {
    return fromRecord(registration, null);
  }

  public static List<BrokerPartitionState> fromRecord(
      final BrokerRegistrationRecord registration, final Instant receivedAt) {
    return registration.getPartitions().stream()
        .map(
            partition ->
                new BrokerPartitionState(
                    partition.getPartitionId(),
                    new BrokerPartitionState.Status(
                        partition.getRole(),
                        partition.getTerm(),
                        partition.getLifecycleState(),
                        partition.getPriority(),
                        partition.getRecoveryEpoch()),
                    new BrokerPartitionState.Replication(
                        partition.getMatchIndex(),
                        partition.getLag(),
                        partition.isReadyForPromotion()),
                    new BrokerPartitionState.Operation(
                        partition.getLastOpId(), partition.getLastOpStatus()),
                    receivedAt == null || partition.getElection().configurationDigest().isBlank()
                        ? partition.getElection()
                        : received(partition.getElection(), receivedAt.toEpochMilli())))
        .toList();
  }

  private static BrokerPartitionElectionState received(
      final BrokerPartitionElectionState state, final long timestamp) {
    return new BrokerPartitionElectionState(
        state.configuration(),
        state.priorities(),
        new BrokerPartitionElectionState.Observation(
            state.nodeState(), state.leaderId(), state.serviceReady(), timestamp));
  }

  public static BrokerPartitionElectionState fromProto(
      final BrokerPartitionElectionStateOrBuilder state) {
    return new BrokerPartitionElectionState(
        new RaftConfigurationReference(
            state.getConfigurationIndex(), state.getConfigurationDigest()),
        new BrokerPartitionElectionState.Priorities(
            state.getDesiredPriority(), state.getTargetPriority()),
        new BrokerPartitionElectionState.Observation(
            RaftNodeState.parse(state.getNodeState()),
            state.getLeaderId(),
            state.getServiceReady(),
            state.getObservedAtEpochMillis()));
  }

  public static Builder toProto(final BrokerPartitionElectionState state) {
    return newBuilder()
        .setConfigurationIndex(state.configurationIndex())
        .setConfigurationDigest(state.configurationDigest())
        .setDesiredPriority(state.desiredPriority())
        .setTargetPriority(state.targetPriority())
        .setNodeState(state.nodeState().name())
        .setLeaderId(state.leaderId())
        .setServiceReady(state.serviceReady())
        .setObservedAtEpochMillis(state.observedAtEpochMillis());
  }
}
