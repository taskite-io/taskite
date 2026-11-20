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

import io.taskite.placementdriver.broker.registry.BrokerPartitionReports;
import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.PartitionLeadershipResolver;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import java.util.List;

/** Pure verification used at both dispatch and command processing boundaries. */
public final class BrokerOperationEffectVerifier {
  private BrokerOperationEffectVerifier() {}

  public static boolean applied(
      final PartitionTopology topology,
      final TopologyOperation operation,
      final List<BrokerRegistration> registrations) {
    final var configuration = operation.configuration();
    if (configuration == null) {
      return false;
    }
    final var online =
        registrations.stream()
            .filter(registration -> topology.clusterId().equals(registration.clusterId()))
            .filter(registration -> registration.status() != BrokerStatus.OFFLINE)
            .toList();
    final String digest = configuration.targetDigest(topology.clusterId());
    if (operation.type() == TopologyOperationType.TRANSFER_LEADER
        || operation.type() == TopologyOperationType.STEP_DOWN_IF_NOT_PRIMARY) {
      final var leader =
          PartitionLeadershipResolver.resolve(
              online, topology.partitionId(), topology.currentVoters(), topology.recoveryEpoch());
      if (!operation.targetBroker().equals(leader.leaderId()) || leader.term() <= 0) {
        return false;
      }
      return online.stream()
          .filter(registration -> registration.brokerId().equals(leader.leaderId()))
          .flatMap(
              registration ->
                  registration.partitions().stream()
                      .filter(partition -> BrokerPartitionReports.current(registration, partition)))
          .anyMatch(
              partition ->
                  partition.partitionId() == topology.partitionId()
                      && partition.role() == MemberRole.LEADER
                      && partition.term() == leader.term()
                      && partition.recoveryEpoch() == configuration.targetRecoveryEpoch()
                      && partition.election().configurationIndex() <= partition.matchIndex()
                      && partition.election().serviceReady()
                      && digest.equals(partition.election().configurationDigest()));
    }
    // Registration reports belong to the current incarnation and retain term; the persisted
    // replica projection alone cannot establish either fact.
    return online.stream()
        .flatMap(
            registration ->
                registration.partitions().stream()
                    .filter(partition -> BrokerPartitionReports.current(registration, partition)))
        .filter(partition -> partition.partitionId() == topology.partitionId())
        .anyMatch(
            replica -> {
              return operation.opId().equals(replica.lastOpId())
                  && replica.lastOpStatus() == BrokerTopologyOperationStatus.APPLIED
                  && replica.recoveryEpoch() == configuration.targetRecoveryEpoch()
                  && replica.election().configurationIndex() <= replica.matchIndex()
                  && digest.equals(replica.election().configurationDigest())
                  && (operation.type() != TopologyOperationType.TRANSFER_LEADER
                          && operation.type() != TopologyOperationType.STEP_DOWN_IF_NOT_PRIMARY
                      || operation.targetBroker().equals(replica.election().leaderId()));
            });
  }
}
