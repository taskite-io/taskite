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

import io.taskite.protocol.record.TopologyOperationType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.protocol.record.value.broker.BrokerPartitionOperationRecordValue;
import io.taskite.topology.*;
import java.util.List;
import java.util.Objects;

public record BrokerPartitionOperation(
    Partition partition, Operation operation, Configuration configuration, Authority authority) {

  public BrokerPartitionOperation(
      final Partition partition, final Operation operation, final Configuration configuration) {
    this(partition, operation, configuration, Authority.NONE);
  }

  public record Authority(long generation, String owner) {
    public static final Authority NONE = new Authority(0, "");

    public Authority {
      Objects.requireNonNull(owner, "owner");
      if (generation < 0 || generation > 0 && owner.isBlank()) {
        throw new IllegalArgumentException("invalid execution authority");
      }
    }
  }

  public BrokerPartitionOperation {
    Objects.requireNonNull(partition, "partition");
    Objects.requireNonNull(operation, "operation");
    Objects.requireNonNull(configuration, "configuration");
    Objects.requireNonNull(authority, "authority");
  }

  public String clusterId() {
    return partition.clusterId();
  }

  public int partitionId() {
    return partition.id();
  }

  public String opId() {
    return operation.id();
  }

  public TopologyOperationType type() {
    return operation.type();
  }

  public String targetBroker() {
    return operation.targetBroker();
  }

  public int priority() {
    return operation.priority();
  }

  public List<BrokerPartitionMember> members() {
    return configuration.members();
  }

  public long expectedMembershipEpoch() {
    return configuration.source().index();
  }

  public String expectedConfigurationDigest() {
    return configuration.source().digest();
  }

  public long expectedRecoveryEpoch() {
    return configuration.recoveryFence().expectedEpoch();
  }

  public long targetRecoveryEpoch() {
    return configuration.recoveryFence().targetEpoch();
  }

  public String fencingProofId() {
    return configuration.recoveryFence().proofId();
  }

  public record Partition(String clusterId, int id) {
    public Partition {
      clusterId = clusterId == null ? "" : clusterId;
    }
  }

  public record Operation(
      String id, TopologyOperationType type, String targetBroker, int priority) {
    public Operation {
      id = id == null ? "" : id;
      type = type == null ? TopologyOperationType.NULL_VAL : type;
      targetBroker = targetBroker == null ? "" : targetBroker;
    }
  }

  public record Configuration(
      List<BrokerPartitionMember> members,
      RaftConfigurationReference source,
      RecoveryFence recoveryFence) {
    public Configuration {
      members = List.copyOf(members == null ? List.of() : members);
      Objects.requireNonNull(source, "source");
      Objects.requireNonNull(recoveryFence, "recoveryFence");
    }
  }

  public static BrokerPartitionOperation fromRecord(
      final BrokerPartitionOperationRecordValue value, final Intent intent) {
    return new BrokerPartitionOperation(
        new Partition(value.getClusterId(), value.getPartitionId()),
        new Operation(
            value.getOpId(),
            TopologyOperationType.valueOf(intent.name()),
            value.getTargetBroker(),
            value.getPriority()),
        new Configuration(
            value.getMembers(),
            new RaftConfigurationReference(
                value.getExpectedMembershipEpoch(), value.getExpectedConfigurationDigest()),
            new RecoveryFence(
                value.getExpectedRecoveryEpoch(),
                value.getTargetRecoveryEpoch(),
                value.getFencingProofId())),
        new Authority(value.getExecutionGeneration(), value.getOwnerChangeId()));
  }

  public String targetConfigurationDigest() {
    return RaftConfigurationDigest.ofBroker(clusterId(), members());
  }
}
