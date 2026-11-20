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

import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.topology.RaftConfigurationDigest;
import io.taskite.topology.RecoveryFence;
import java.util.List;
import java.util.Objects;

/** Immutable source and target of one operation, persisted before any dispatch. */
public record BrokerOperationConfiguration(
    List<BrokerPartitionMember> sourceMembers,
    List<BrokerPartitionMember> targetMembers,
    long expectedMembershipEpoch,
    RecoveryFence recoveryFence) {
  public BrokerOperationConfiguration {
    sourceMembers = List.copyOf(sourceMembers);
    targetMembers = List.copyOf(targetMembers);
    Objects.requireNonNull(recoveryFence, "recoveryFence");
  }

  public long expectedRecoveryEpoch() {
    return recoveryFence.expectedEpoch();
  }

  public long targetRecoveryEpoch() {
    return recoveryFence.targetEpoch();
  }

  public String fencingProofId() {
    return recoveryFence.proofId();
  }

  public String sourceDigest(final String clusterId) {
    return RaftConfigurationDigest.ofBroker(clusterId, sourceMembers);
  }

  public String targetDigest(final String clusterId) {
    return RaftConfigurationDigest.ofBroker(clusterId, targetMembers);
  }
}
