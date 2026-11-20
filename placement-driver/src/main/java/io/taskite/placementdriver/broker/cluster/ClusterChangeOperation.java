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
package io.taskite.placementdriver.broker.cluster;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

/** Durable aggregate manifest for one accepted cluster configuration PATCH. */
public record ClusterChangeOperation(
    String clusterId,
    String changeId,
    long sourceTopologyVersion,
    int targetClusterSize,
    int targetPartitionSize,
    int targetReplicationFactor,
    List<String> targetVoterRoster,
    List<String> removedBrokers,
    List<String> selectedBrokerIds,
    List<ExpectedPartition> expectedTopology,
    List<String> planChangeIds,
    @JsonProperty("phase") BrokerClusterChangeStatus status,
    long startedAtEpochMillis,
    long deadlineEpochMillis) {

  public ClusterChangeOperation {
    targetVoterRoster = List.copyOf(targetVoterRoster == null ? List.of() : targetVoterRoster);
    removedBrokers = List.copyOf(removedBrokers == null ? List.of() : removedBrokers);
    selectedBrokerIds = List.copyOf(selectedBrokerIds == null ? List.of() : selectedBrokerIds);
    expectedTopology = List.copyOf(expectedTopology == null ? List.of() : expectedTopology);
    planChangeIds = List.copyOf(planChangeIds == null ? List.of() : planChangeIds);
    status = status == null ? BrokerClusterChangeStatus.PENDING : status;
  }

  public ClusterChangeOperation withStatus(final BrokerClusterChangeStatus nextStatus) {
    return new ClusterChangeOperation(
        clusterId,
        changeId,
        sourceTopologyVersion,
        targetClusterSize,
        targetPartitionSize,
        targetReplicationFactor,
        targetVoterRoster,
        removedBrokers,
        selectedBrokerIds,
        expectedTopology,
        planChangeIds,
        nextStatus,
        startedAtEpochMillis,
        deadlineEpochMillis);
  }

  public record ExpectedPartition(
      int partitionId,
      List<String> desiredVoters,
      List<String> currentVoters,
      List<String> learners,
      String desiredPrimary,
      Map<String, Integer> priorities) {
    public ExpectedPartition {
      desiredVoters = List.copyOf(desiredVoters == null ? List.of() : desiredVoters);
      currentVoters = List.copyOf(currentVoters == null ? List.of() : currentVoters);
      learners = List.copyOf(learners == null ? List.of() : learners);
      desiredPrimary = desiredPrimary == null ? "" : desiredPrimary;
      priorities = Map.copyOf(priorities == null ? Map.of() : priorities);
    }
  }
}
