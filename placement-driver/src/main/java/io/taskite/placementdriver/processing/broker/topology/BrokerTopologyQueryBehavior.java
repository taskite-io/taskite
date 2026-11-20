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
package io.taskite.placementdriver.processing.broker.topology;

import io.taskite.placementdriver.broker.topology.BrokerTopologyCalculationException;
import io.taskite.placementdriver.broker.topology.BrokerTopologyCalculator;
import io.taskite.placementdriver.broker.topology.BrokerTopologySnapshot;
import io.taskite.placementdriver.processing.query.TopologyQueryBehavior;
import io.taskite.placementdriver.processing.query.TopologyQueryRejection;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.QueryType;
import io.taskite.topology.ClusterTopology;
import io.taskite.topology.RegistrationStatus;
import java.time.Instant;
import java.util.List;

public final class BrokerTopologyQueryBehavior implements TopologyQueryBehavior {
  private static final String TOPOLOGY_QUERY_FAILED =
      "%s\nExpected to query Broker topology in cluster '%s' at timestamp '%d', but topology calculation failed";

  private final BrokerTopologyCalculator topologyCalculator;

  public BrokerTopologyQueryBehavior(final ProcessingState state) {
    topologyCalculator = new BrokerTopologyCalculator(state);
  }

  @Override
  public QueryType queryType() {
    return QueryType.BROKER_TOPOLOGY;
  }

  @Override
  public UnifiedRecordValue createResponseValue(final TopologyQueryRecord query) {
    try {
      return toTopologyRecord(
          topologyCalculator.calculateBrokerTopologySnapshot(
              query.getClusterId(), Instant.ofEpochMilli(query.getRequestedAtEpochMillis())));
    } catch (final BrokerTopologyCalculationException error) {
      final String reason =
          TOPOLOGY_QUERY_FAILED.formatted(
              error.getMessage(), query.getClusterId(), query.getRequestedAtEpochMillis());
      throw error.status() == BrokerTopologyCalculationException.Status.NOT_FOUND
          ? TopologyQueryRejection.notFound(reason)
          : TopologyQueryRejection.unavailable(reason);
    }
  }

  private static TopologyRecord toTopologyRecord(final BrokerTopologySnapshot snapshot) {
    final ClusterTopology topology = snapshot.topology();
    final List<TopologyRecord.BrokerEndpointRecord> brokers =
        snapshot.brokers().stream()
            .map(
                broker ->
                    new TopologyRecord.BrokerEndpointRecord()
                        .setBrokerId(broker.brokerId())
                        .setHost(broker.host())
                        .setPort(broker.port())
                        .setRaftPort(broker.raftPort())
                        .setVersion(broker.version())
                        .setIncarnationId(broker.incarnationId())
                        .setRegisteredAtEpochMillis(toEpochMillis(broker.registeredAt()))
                        .setLastRenewalEpochMillis(toEpochMillis(broker.lastRenewal()))
                        .setRenewalTimeoutMillis(broker.renewalTimeoutMillis())
                        .setStatus(
                            broker.status() == null
                                ? RegistrationStatus.UNKNOWN
                                : RegistrationStatus.valueOf(broker.status().name()))
                        .setPartitions(
                            broker.partitions().stream()
                                .map(
                                    partition ->
                                        new TopologyRecord.BrokerPartitionRecord()
                                            .setPartitionId(partition.partitionId())
                                            .setRole(partition.role())
                                            .setTerm(partition.term())
                                            .setLifecycleState(partition.lifecycleState())
                                            .setMatchIndex(partition.matchIndex())
                                            .setLag(partition.lag())
                                            .setReadyForPromotion(partition.readyForPromotion())
                                            .setPriority(partition.priority())
                                            .setLastOpId(partition.lastOpId())
                                            .setLastOpStatus(partition.lastOpStatus())
                                            .setRecoveryEpoch(partition.recoveryEpoch())
                                            .setElection(partition.election()))
                                .toList()))
            .toList();
    final List<TopologyRecord.PartitionRecord> partitions =
        topology.partitions().stream()
            .map(
                partition ->
                    new TopologyRecord.PartitionRecord()
                        .setPartitionId(partition.partitionId())
                        .setLeaderId(partition.leaderId())
                        .setServiceReady(partition.serviceReady())
                        .setReplicas(partition.replicas())
                        .setLearners(partition.learners())
                        .setTerm(partition.term())
                        .setRecoveryEpoch(partition.recoveryEpoch())
                        .setStatus(partition.status())
                        .setPriorities(partition.priorities()))
            .toList();
    return new TopologyRecord()
        .setClusterId(topology.clusterId())
        .setVersion(topology.version())
        .setClusterSize(snapshot.clusterSize())
        .setPartitionSize(topology.partitionSize())
        .setReplicationFactor(topology.replicationFactor())
        .setUpdatedAtEpochMillis(topology.updatedAt().toEpochMilli())
        .setBrokers(brokers)
        .setPartitions(partitions);
  }

  private static long toEpochMillis(final Instant value) {
    return value == null ? 0L : value.toEpochMilli();
  }
}
