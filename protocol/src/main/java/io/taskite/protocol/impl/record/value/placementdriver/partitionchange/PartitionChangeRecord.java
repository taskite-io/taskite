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
package io.taskite.protocol.impl.record.value.placementdriver.partitionchange;

import io.taskite.protocol.record.BrokerPartitionChangePurpose;
import io.taskite.protocol.record.BrokerPartitionChangeStatus;
import io.taskite.protocol.record.ChangePlanRecordDecoder;
import io.taskite.protocol.record.ChangePlanRecordEncoder;
import io.taskite.protocol.record.MessageHeaderDecoder;
import io.taskite.protocol.record.MessageHeaderEncoder;
import io.taskite.protocol.record.TopologyOperationType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.topology.RecoveryFence;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;

/** SBE record carrying one partition change plan and its current operation. */
public final class PartitionChangeRecord extends UnifiedRecordValue {
  private String clusterId = "";
  private String changeId = "";
  private int partitionId;
  private long sourceTopologyVersion;
  private long targetTopologyVersion;
  private int nextOperationIndex;
  private long startedAtEpochMillis;
  private long deadlineEpochMillis;
  private long executionGeneration;
  private long authorityIndex;
  private long blockedSinceEpochMillis;
  private String predecessorChangeId = "";
  private String successorChangeId = "";
  private String rootChangeId = "";
  private PlanBlockedReason blockedReason = PlanBlockedReason.NONE;
  private BrokerPartitionChangeStatus status = BrokerPartitionChangeStatus.PENDING;
  private BrokerPartitionChangePurpose purpose = BrokerPartitionChangePurpose.MIGRATION;
  private final List<Operation> operations = new ArrayList<>();

  public BrokerPartitionChangePurpose getPurpose() {
    return purpose;
  }

  public PartitionChangeRecord setPurpose(final BrokerPartitionChangePurpose purpose) {
    this.purpose = Objects.requireNonNull(purpose, "purpose");
    return this;
  }

  public String getClusterId() {
    return clusterId;
  }

  public long getExecutionGeneration() {
    return executionGeneration;
  }

  public PartitionChangeRecord setExecutionGeneration(final long value) {
    executionGeneration = value;
    return this;
  }

  public long getAuthorityIndex() {
    return authorityIndex;
  }

  public PartitionChangeRecord setAuthorityIndex(final long value) {
    authorityIndex = value;
    return this;
  }

  public long getBlockedSinceEpochMillis() {
    return blockedSinceEpochMillis;
  }

  public PartitionChangeRecord setBlockedSinceEpochMillis(final long value) {
    blockedSinceEpochMillis = value;
    return this;
  }

  public String getPredecessorChangeId() {
    return predecessorChangeId;
  }

  public PartitionChangeRecord setPredecessorChangeId(final String value) {
    predecessorChangeId = value;
    return this;
  }

  public String getSuccessorChangeId() {
    return successorChangeId;
  }

  public PartitionChangeRecord setSuccessorChangeId(final String value) {
    successorChangeId = value;
    return this;
  }

  public String getRootChangeId() {
    return rootChangeId;
  }

  public PartitionChangeRecord setRootChangeId(final String value) {
    rootChangeId = value;
    return this;
  }

  public PlanBlockedReason getBlockedReason() {
    return blockedReason;
  }

  public PartitionChangeRecord setBlockedReason(final PlanBlockedReason value) {
    blockedReason = value;
    return this;
  }

  public PartitionChangeRecord setClusterId(final String clusterId) {
    this.clusterId = clusterId == null ? "" : clusterId;
    return this;
  }

  public String getChangeId() {
    return changeId;
  }

  public PartitionChangeRecord setChangeId(final String changeId) {
    this.changeId = changeId == null ? "" : changeId;
    return this;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public PartitionChangeRecord setPartitionId(final int partitionId) {
    this.partitionId = partitionId;
    return this;
  }

  public long getSourceTopologyVersion() {
    return sourceTopologyVersion;
  }

  public PartitionChangeRecord setSourceTopologyVersion(final long sourceTopologyVersion) {
    this.sourceTopologyVersion = sourceTopologyVersion;
    return this;
  }

  public long getTargetTopologyVersion() {
    return targetTopologyVersion;
  }

  public PartitionChangeRecord setTargetTopologyVersion(final long targetTopologyVersion) {
    this.targetTopologyVersion = targetTopologyVersion;
    return this;
  }

  public int getNextOperationIndex() {
    return nextOperationIndex;
  }

  public PartitionChangeRecord setNextOperationIndex(final int nextOperationIndex) {
    this.nextOperationIndex = nextOperationIndex;
    return this;
  }

  public long getStartedAtEpochMillis() {
    return startedAtEpochMillis;
  }

  public PartitionChangeRecord setStartedAtEpochMillis(final long startedAtEpochMillis) {
    this.startedAtEpochMillis = startedAtEpochMillis;
    return this;
  }

  public long getDeadlineEpochMillis() {
    return deadlineEpochMillis;
  }

  public PartitionChangeRecord setDeadlineEpochMillis(final long deadlineEpochMillis) {
    this.deadlineEpochMillis = deadlineEpochMillis;
    return this;
  }

  public BrokerPartitionChangeStatus getStatus() {
    return status;
  }

  public PartitionChangeRecord setStatus(final BrokerPartitionChangeStatus status) {
    this.status = status == null ? BrokerPartitionChangeStatus.PENDING : status;
    return this;
  }

  public List<Operation> getOperations() {
    return operations;
  }

  public PartitionChangeRecord setOperations(final List<Operation> ops) {
    operations.clear();
    if (ops != null) {
      operations.addAll(ops);
    }
    return this;
  }

  public PartitionChangeRecord addOperation(final Operation operation) {
    operations.add(operation);
    return this;
  }

  @Override
  public int getLength() {
    return encode().length;
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    buffer.putBytes(offset, encode());
  }

  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    final ChangePlanRecordDecoder body = new ChangePlanRecordDecoder();
    header.wrap(buffer, offset);
    if (header.templateId() != ChangePlanRecordDecoder.TEMPLATE_ID) {
      throw new IllegalArgumentException(
          "Expected ChangePlanRecord template but got " + header.templateId());
    }
    body.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        header.blockLength(),
        header.version());
    partitionId = body.partitionId();
    sourceTopologyVersion = body.sourceTopologyVersion();
    targetTopologyVersion = body.targetTopologyVersion();
    nextOperationIndex = body.nextOperationIndex();
    startedAtEpochMillis = body.startedAtEpochMillis();
    deadlineEpochMillis = body.deadlineEpochMillis();
    executionGeneration = header.version() >= 11 ? body.executionGeneration() : 0;
    authorityIndex = header.version() >= 11 ? body.authorityIndex() : 0;
    blockedSinceEpochMillis = header.version() >= 11 ? body.blockedSinceEpochMillis() : 0;
    purpose = header.version() >= 14 ? body.purpose() : BrokerPartitionChangePurpose.MIGRATION;
    if (purpose == BrokerPartitionChangePurpose.NULL_VAL
        || purpose == BrokerPartitionChangePurpose.SBE_UNKNOWN) {
      throw new IllegalArgumentException("unknown partition change purpose");
    }
    final BrokerPartitionChangeStatus decodedStatus = body.status();
    status =
        decodedStatus == BrokerPartitionChangeStatus.NULL_VAL
                || decodedStatus == BrokerPartitionChangeStatus.SBE_UNKNOWN
            ? BrokerPartitionChangeStatus.PENDING
            : decodedStatus;
    operations.clear();
    for (final ChangePlanRecordDecoder.OperationsDecoder operation : body.operations()) {
      final boolean frozen = operation.frozen() == 1;
      final long membershipEpoch = operation.expectedMembershipEpoch();
      final long recoveryEpoch = operation.expectedRecoveryEpoch();
      final long targetEpoch = operation.targetRecoveryEpoch();
      final List<Member> sourceMembers = new ArrayList<>();
      for (final var member : operation.sourceMembers()) {
        sourceMembers.add(new Member(member.brokerId(), member.voter() == 1, member.priority()));
      }
      final List<Member> targetMembers = new ArrayList<>();
      for (final var member : operation.targetMembers()) {
        targetMembers.add(new Member(member.brokerId(), member.voter() == 1, member.priority()));
      }
      operations.add(
          new Operation(
              operation.opId(),
              operation.operationType().value(),
              operation.targetBroker(),
              operation.priority(),
              frozen
                  ? new Configuration(
                      sourceMembers,
                      targetMembers,
                      membershipEpoch,
                      new RecoveryFence(recoveryEpoch, targetEpoch, operation.fencingProofId()))
                  : null));
      if (!frozen) {
        operation.fencingProofId();
      }
    }
    clusterId = body.clusterId();
    changeId = body.changeId();
    predecessorChangeId = header.version() >= 11 ? body.predecessorChangeId() : "";
    successorChangeId = header.version() >= 11 ? body.successorChangeId() : "";
    rootChangeId = header.version() >= 11 ? body.rootChangeId() : "";
    blockedReason =
        header.version() >= 11
            ? PlanBlockedReason.parse(body.blockedReason())
            : PlanBlockedReason.NONE;
  }

  private byte[] encode() {
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final ChangePlanRecordEncoder body = new ChangePlanRecordEncoder();
    body.wrapAndApplyHeader(buffer, 0, header)
        .partitionId(partitionId)
        .sourceTopologyVersion(sourceTopologyVersion)
        .targetTopologyVersion(targetTopologyVersion)
        .nextOperationIndex(nextOperationIndex)
        .startedAtEpochMillis(startedAtEpochMillis)
        .deadlineEpochMillis(deadlineEpochMillis)
        .executionGeneration(executionGeneration)
        .authorityIndex(authorityIndex)
        .blockedSinceEpochMillis(blockedSinceEpochMillis)
        .status(status)
        .purpose(purpose);
    final ChangePlanRecordEncoder.OperationsEncoder encodedOperations =
        body.operationsCount(operations.size());
    for (final Operation operation : operations) {
      final TopologyOperationType operationType = TopologyOperationType.get(operation.type());
      if (operationType == TopologyOperationType.NULL_VAL
          || operationType == TopologyOperationType.SBE_UNKNOWN) {
        throw new IllegalArgumentException("unknown topology operation type: " + operation.type());
      }
      final var configuration = operation.configuration();
      encodedOperations
          .next()
          .operationType(operationType)
          .priority(operation.priority())
          .frozen((short) (configuration == null ? 0 : 1))
          .expectedMembershipEpoch(
              configuration == null ? 0 : configuration.expectedMembershipEpoch())
          .expectedRecoveryEpoch(configuration == null ? 0 : configuration.expectedRecoveryEpoch())
          .targetRecoveryEpoch(configuration == null ? 0 : configuration.targetRecoveryEpoch());
      final var sources = configuration == null ? List.<Member>of() : configuration.sourceMembers();
      final var encodedSources = encodedOperations.sourceMembersCount(sources.size());
      for (final var member : sources) {
        encodedSources
            .next()
            .voter((short) (member.voter() ? 1 : 0))
            .priority(member.priority())
            .brokerId(member.brokerId());
      }
      final var targets = configuration == null ? List.<Member>of() : configuration.targetMembers();
      final var encodedTargets = encodedOperations.targetMembersCount(targets.size());
      for (final var member : targets) {
        encodedTargets
            .next()
            .voter((short) (member.voter() ? 1 : 0))
            .priority(member.priority())
            .brokerId(member.brokerId());
      }
      encodedOperations
          .opId(operation.opId())
          .targetBroker(operation.targetBroker())
          .fencingProofId(configuration == null ? "" : configuration.fencingProofId());
    }
    body.clusterId(clusterId)
        .changeId(changeId)
        .predecessorChangeId(predecessorChangeId)
        .successorChangeId(successorChangeId)
        .rootChangeId(rootChangeId)
        .blockedReason(blockedReason.wireValue());
    return Arrays.copyOf(
        buffer.byteArray(), MessageHeaderEncoder.ENCODED_LENGTH + body.encodedLength());
  }

  /**
   * A single operation entry within the record; {@code type} maps to {@code
   * io.taskite.placementdriver.broker.cluster.TopologyOperationType.ordinal()}.
   */
  public record Operation(
      String opId, short type, String targetBroker, int priority, Configuration configuration) {
    public Operation(
        final String opId, final short type, final String targetBroker, final int priority) {
      this(opId, type, targetBroker, priority, null);
    }

    public Operation {
      opId = opId == null ? "" : opId;
      targetBroker = targetBroker == null ? "" : targetBroker;
    }
  }

  public record Member(String brokerId, boolean voter, int priority) {}

  public record Configuration(
      List<Member> sourceMembers,
      List<Member> targetMembers,
      long expectedMembershipEpoch,
      RecoveryFence recoveryFence) {
    public Configuration {
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
  }
}
