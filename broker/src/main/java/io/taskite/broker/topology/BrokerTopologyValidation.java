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
package io.taskite.broker.topology;

import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.ClusterTopology;
import io.taskite.topology.Partition;

/** Ensures a topology can describe the complete Raft voter configuration for a local group. */
public final class BrokerTopologyValidation {
  private BrokerTopologyValidation() {}

  public static void requireVoterEndpoints(
      final ClusterTopology topology, final Partition partition, final String localBrokerId) {
    if (partition.voters().isEmpty()) {
      throw incomplete(topology, partition, "no voters");
    }
    for (final String voter : partition.voters()) {
      // The local endpoint comes from the Broker's validated startup configuration.
      if (voter.equals(localBrokerId)) {
        continue;
      }
      final BrokerEndpoint endpoint = topology.brokers().get(voter);
      if (endpoint == null
          || endpoint.host() == null
          || endpoint.host().isBlank()
          || endpoint.raftPort() <= 0
          || endpoint.raftPort() > 65_535) {
        throw incomplete(
            topology, partition, "missing or invalid Raft endpoint for voter " + voter);
      }
    }
  }

  private static IllegalStateException incomplete(
      final ClusterTopology topology, final Partition partition, final String reason) {
    return new IllegalStateException(
        "Incomplete topology for cluster "
            + topology.clusterId()
            + ", partition "
            + partition.partitionId()
            + ", version "
            + topology.version()
            + ": "
            + reason
            + "; voters="
            + partition.voters()
            + ", learners="
            + partition.learners());
  }
}
