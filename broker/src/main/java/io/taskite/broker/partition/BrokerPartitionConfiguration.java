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
package io.taskite.broker.partition;

import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import java.util.List;
import java.util.Objects;

public record BrokerPartitionConfiguration(
    String clusterId,
    int partitionId,
    String leaderId,
    long leaderTerm,
    long configurationIndex,
    String configurationDigest,
    List<BrokerPartitionMember> members,
    long recoveryEpoch,
    long executionGeneration,
    String ownerChangeId,
    long authorityIndex) {
  public BrokerPartitionConfiguration {
    Objects.requireNonNull(clusterId, "clusterId");
    Objects.requireNonNull(leaderId, "leaderId");
    Objects.requireNonNull(configurationDigest, "configurationDigest");
    Objects.requireNonNull(ownerChangeId, "ownerChangeId");
    members = List.copyOf(members);
  }
}
