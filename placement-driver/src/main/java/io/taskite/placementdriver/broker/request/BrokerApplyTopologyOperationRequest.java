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
package io.taskite.placementdriver.broker.request;

import io.taskite.broker.client.request.BrokerAdminRequest;
import io.taskite.protocol.impl.record.value.broker.BrokerPartitionOperationRecord;
import io.taskite.protocol.record.TopologyOperationType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.BrokerPartitionOperationIntent;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.topology.*;
import java.util.List;
import java.util.Objects;
import org.agrona.DirectBuffer;

public final class BrokerApplyTopologyOperationRequest
    extends BrokerAdminRequest<BrokerPartitionOperationRecord> {
  private final BrokerPartitionOperationRecord value;
  private final Operation operation;
  private final Authority authority;

  public BrokerApplyTopologyOperationRequest(
      final Partition partition, final Operation operation, final Configuration configuration) {
    this(partition, operation, configuration, Authority.NONE);
  }

  public BrokerApplyTopologyOperationRequest(
      final Partition partition,
      final Operation operation,
      final Configuration configuration,
      final Authority authority) {
    super(
        ValueType.BROKER_PARTITION_OPERATION,
        BrokerPartitionOperationIntent.valueOf(operation.type().name()));
    this.operation = operation;
    this.authority = authority;
    setClusterId(partition.clusterId());
    setPartitionId(partition.id());
    value =
        new BrokerPartitionOperationRecord()
            .setClusterId(partition.clusterId())
            .setPartitionId(partition.id())
            .setOpId(operation.id())
            .setTargetBroker(operation.targetBroker())
            .setPriority(operation.priority())
            .setMembers(configuration.members())
            .setExpectedMembershipEpoch(configuration.source().index())
            .setExpectedConfigurationDigest(configuration.source().digest())
            .setExpectedRecoveryEpoch(configuration.recoveryFence().expectedEpoch())
            .setTargetRecoveryEpoch(configuration.recoveryFence().targetEpoch())
            .setFencingProofId(configuration.recoveryFence().proofId())
            .setExecutionGeneration(authority.generation())
            .setOwnerChangeId(authority.owner());
  }

  @Override
  public BrokerPartitionOperationRecord getRequestWriter() {
    return value;
  }

  @Override
  protected BrokerPartitionOperationRecord toResponseDto(final DirectBuffer buffer) {
    final var response = new BrokerPartitionOperationRecord();
    response.wrap(buffer);
    if (!getClusterId().equals(response.getClusterId())
        || getPartitionId() != response.getPartitionId()
        || !opId().equals(response.getOpId())
        || !response.getApplied()) {
      throw new IllegalArgumentException("operation response identity or result mismatch");
    }
    return response;
  }

  public String clusterId() {
    return getClusterId();
  }

  public int partitionId() {
    return getPartitionId();
  }

  public String opId() {
    return value.getOpId();
  }

  public TopologyOperationType type() {
    return operation.type();
  }

  public String targetBroker() {
    return value.getTargetBroker();
  }

  public int priority() {
    return value.getPriority();
  }

  public List<BrokerPartitionMember> members() {
    return value.getMembers();
  }

  public long expectedMembershipEpoch() {
    return value.getExpectedMembershipEpoch();
  }

  public String expectedConfigurationDigest() {
    return value.getExpectedConfigurationDigest();
  }

  public long expectedRecoveryEpoch() {
    return value.getExpectedRecoveryEpoch();
  }

  public long targetRecoveryEpoch() {
    return value.getTargetRecoveryEpoch();
  }

  public String fencingProofId() {
    return value.getFencingProofId();
  }

  public Authority authority() {
    return authority;
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

  public String targetConfigurationDigest() {
    return RaftConfigurationDigest.ofBroker(clusterId(), members());
  }
}
