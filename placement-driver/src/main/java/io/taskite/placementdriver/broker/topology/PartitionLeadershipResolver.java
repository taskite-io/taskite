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
package io.taskite.placementdriver.broker.topology;

import io.taskite.placementdriver.broker.registry.BrokerPartitionReports;
import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RaftNodeState;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Resolves the authoritative partition term and Leader from online Broker observations. */
public final class PartitionLeadershipResolver {
  private PartitionLeadershipResolver() {}

  public static Observation resolve(
      final List<BrokerRegistration> onlineRegistrations,
      final int partitionId,
      final List<String> currentVoters) {
    return resolve(onlineRegistrations, partitionId, currentVoters, 0);
  }

  public static Observation resolve(
      final List<BrokerRegistration> onlineRegistrations,
      final int partitionId,
      final List<String> currentVoters,
      final long recoveryEpoch) {
    return resolve(onlineRegistrations, partitionId, currentVoters, recoveryEpoch, true);
  }

  /** Management discovery precedes linearizable configuration and execution-authority checks. */
  public static Observation resolveForManagement(
      final List<BrokerRegistration> onlineRegistrations,
      final int partitionId,
      final List<String> currentVoters,
      final long recoveryEpoch) {
    return resolve(onlineRegistrations, partitionId, currentVoters, recoveryEpoch, false);
  }

  private static Observation resolve(
      final List<BrokerRegistration> onlineRegistrations,
      final int partitionId,
      final List<String> currentVoters,
      final long recoveryEpoch,
      final boolean requireServiceReady) {
    final Set<String> voterIds = Set.copyOf(currentVoters);
    final Set<String> leadersAtHighestTerm = new LinkedHashSet<>();
    long highestTerm = 0;

    for (final BrokerRegistration registration : onlineRegistrations) {
      if (!voterIds.contains(registration.brokerId())) {
        continue;
      }
      for (final BrokerPartitionState partition : registration.partitions()) {
        if (partition.partitionId() != partitionId
            || partition.recoveryEpoch() != recoveryEpoch
            || !BrokerPartitionReports.current(registration, partition)) {
          continue;
        }
        if (partition.term() > highestTerm) {
          highestTerm = partition.term();
          leadersAtHighestTerm.clear();
        }
        if (partition.term() == highestTerm
            && partition.role() == MemberRole.LEADER
            && (requireServiceReady
                ? partition.election().serviceReady()
                : partition.election().nodeState() == RaftNodeState.STATE_LEADER
                    && registration.brokerId().equals(partition.election().leaderId()))) {
          leadersAtHighestTerm.add(registration.brokerId());
        }
      }
    }

    final String leaderId =
        leadersAtHighestTerm.size() == 1 ? leadersAtHighestTerm.iterator().next() : "";
    return new Observation(leaderId, highestTerm);
  }

  public record Observation(String leaderId, long term) {
    public Observation {
      leaderId = leaderId == null ? "" : leaderId;
      term = Math.max(0, term);
    }
  }
}
