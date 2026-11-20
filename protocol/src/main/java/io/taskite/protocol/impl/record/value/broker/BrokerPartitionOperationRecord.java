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
package io.taskite.protocol.impl.record.value.broker;

import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.protocol.record.value.broker.BrokerPartitionOperationRecordValue;
import java.util.List;
import java.util.Objects;

public final class BrokerPartitionOperationRecord extends UnifiedRecordValue
    implements BrokerPartitionOperationRecordValue {
  private String clusterId = "";
  private int partitionId = 0;
  private String opId = "";
  private String targetBroker = "";
  private int priority = 0;
  private List<BrokerPartitionMember> members = List.of();
  private long expectedMembershipEpoch = 0;
  private String expectedConfigurationDigest = "";
  private long expectedRecoveryEpoch = 0;
  private long targetRecoveryEpoch = 0;
  private String fencingProofId = "";
  private long executionGeneration = 0;
  private String ownerChangeId = "";
  private boolean applied = false;

  public BrokerPartitionOperationRecord() {}

  public String getClusterId() {
    return clusterId;
  }

  public BrokerPartitionOperationRecord setClusterId(final String value) {
    clusterId = Objects.requireNonNull(value, "clusterId");
    return this;
  }

  public String clusterId() {
    return clusterId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public BrokerPartitionOperationRecord setPartitionId(final int value) {
    partitionId = value;
    return this;
  }

  public int partitionId() {
    return partitionId;
  }

  public String getOpId() {
    return opId;
  }

  public BrokerPartitionOperationRecord setOpId(final String value) {
    opId = Objects.requireNonNull(value, "opId");
    return this;
  }

  public String opId() {
    return opId;
  }

  public String getTargetBroker() {
    return targetBroker;
  }

  public BrokerPartitionOperationRecord setTargetBroker(final String value) {
    targetBroker = Objects.requireNonNull(value, "targetBroker");
    return this;
  }

  public String targetBroker() {
    return targetBroker;
  }

  public int getPriority() {
    return priority;
  }

  public BrokerPartitionOperationRecord setPriority(final int value) {
    priority = value;
    return this;
  }

  public int priority() {
    return priority;
  }

  public List<BrokerPartitionMember> getMembers() {
    return members;
  }

  public BrokerPartitionOperationRecord setMembers(final List<BrokerPartitionMember> value) {
    members = List.copyOf(value);
    return this;
  }

  public List<BrokerPartitionMember> members() {
    return members;
  }

  public long getExpectedMembershipEpoch() {
    return expectedMembershipEpoch;
  }

  public BrokerPartitionOperationRecord setExpectedMembershipEpoch(final long value) {
    expectedMembershipEpoch = value;
    return this;
  }

  public long expectedMembershipEpoch() {
    return expectedMembershipEpoch;
  }

  public String getExpectedConfigurationDigest() {
    return expectedConfigurationDigest;
  }

  public BrokerPartitionOperationRecord setExpectedConfigurationDigest(final String value) {
    expectedConfigurationDigest = Objects.requireNonNull(value, "expectedConfigurationDigest");
    return this;
  }

  public String expectedConfigurationDigest() {
    return expectedConfigurationDigest;
  }

  public long getExpectedRecoveryEpoch() {
    return expectedRecoveryEpoch;
  }

  public BrokerPartitionOperationRecord setExpectedRecoveryEpoch(final long value) {
    expectedRecoveryEpoch = value;
    return this;
  }

  public long expectedRecoveryEpoch() {
    return expectedRecoveryEpoch;
  }

  public long getTargetRecoveryEpoch() {
    return targetRecoveryEpoch;
  }

  public BrokerPartitionOperationRecord setTargetRecoveryEpoch(final long value) {
    targetRecoveryEpoch = value;
    return this;
  }

  public long targetRecoveryEpoch() {
    return targetRecoveryEpoch;
  }

  public String getFencingProofId() {
    return fencingProofId;
  }

  public BrokerPartitionOperationRecord setFencingProofId(final String value) {
    fencingProofId = Objects.requireNonNull(value, "fencingProofId");
    return this;
  }

  public String fencingProofId() {
    return fencingProofId;
  }

  public long getExecutionGeneration() {
    return executionGeneration;
  }

  public BrokerPartitionOperationRecord setExecutionGeneration(final long value) {
    executionGeneration = value;
    return this;
  }

  public long executionGeneration() {
    return executionGeneration;
  }

  public String getOwnerChangeId() {
    return ownerChangeId;
  }

  public BrokerPartitionOperationRecord setOwnerChangeId(final String value) {
    ownerChangeId = Objects.requireNonNull(value, "ownerChangeId");
    return this;
  }

  public String ownerChangeId() {
    return ownerChangeId;
  }

  public boolean getApplied() {
    return applied;
  }

  public BrokerPartitionOperationRecord setApplied(final boolean value) {
    applied = value;
    return this;
  }

  public boolean applied() {
    return applied;
  }
}
