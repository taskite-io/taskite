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
import io.taskite.protocol.record.value.broker.BrokerPartitionConfigurationRecordValue;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import java.util.List;
import java.util.Objects;

public final class BrokerPartitionConfigurationRecord extends UnifiedRecordValue
    implements BrokerPartitionConfigurationRecordValue {
  private String clusterId = "";
  private int partitionId = 0;
  private String leaderId = "";
  private long leaderTerm = 0;
  private long configurationIndex = 0;
  private String configurationDigest = "";
  private List<BrokerPartitionMember> members = List.of();
  private long recoveryEpoch = 0;
  private long executionGeneration = 0;
  private String ownerChangeId = "";
  private long authorityIndex = 0;
  private long expectedGeneration = 0;
  private long generation = 0;
  private long expectedRecoveryEpoch = 0;

  public BrokerPartitionConfigurationRecord() {}

  public BrokerPartitionConfigurationRecord(
      final String clusterId,
      final int partitionId,
      final String leaderId,
      final long leaderTerm,
      final long configurationIndex,
      final String configurationDigest,
      final List<BrokerPartitionMember> members,
      final long recoveryEpoch,
      final long executionGeneration,
      final String ownerChangeId,
      final long authorityIndex) {
    setClusterId(clusterId);
    setPartitionId(partitionId);
    setLeaderId(leaderId);
    setLeaderTerm(leaderTerm);
    setConfigurationIndex(configurationIndex);
    setConfigurationDigest(configurationDigest);
    setMembers(members);
    setRecoveryEpoch(recoveryEpoch);
    setExecutionGeneration(executionGeneration);
    setOwnerChangeId(ownerChangeId);
    setAuthorityIndex(authorityIndex);
  }

  public String getClusterId() {
    return clusterId;
  }

  public BrokerPartitionConfigurationRecord setClusterId(final String value) {
    clusterId = Objects.requireNonNull(value, "clusterId");
    return this;
  }

  public String clusterId() {
    return clusterId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public BrokerPartitionConfigurationRecord setPartitionId(final int value) {
    partitionId = value;
    return this;
  }

  public int partitionId() {
    return partitionId;
  }

  public String getLeaderId() {
    return leaderId;
  }

  public BrokerPartitionConfigurationRecord setLeaderId(final String value) {
    leaderId = Objects.requireNonNull(value, "leaderId");
    return this;
  }

  public String leaderId() {
    return leaderId;
  }

  public long getLeaderTerm() {
    return leaderTerm;
  }

  public BrokerPartitionConfigurationRecord setLeaderTerm(final long value) {
    leaderTerm = value;
    return this;
  }

  public long leaderTerm() {
    return leaderTerm;
  }

  public long getConfigurationIndex() {
    return configurationIndex;
  }

  public BrokerPartitionConfigurationRecord setConfigurationIndex(final long value) {
    configurationIndex = value;
    return this;
  }

  public long configurationIndex() {
    return configurationIndex;
  }

  public String getConfigurationDigest() {
    return configurationDigest;
  }

  public BrokerPartitionConfigurationRecord setConfigurationDigest(final String value) {
    configurationDigest = Objects.requireNonNull(value, "configurationDigest");
    return this;
  }

  public String configurationDigest() {
    return configurationDigest;
  }

  public List<BrokerPartitionMember> getMembers() {
    return members;
  }

  public BrokerPartitionConfigurationRecord setMembers(final List<BrokerPartitionMember> value) {
    members = List.copyOf(value);
    return this;
  }

  public List<BrokerPartitionMember> members() {
    return members;
  }

  public long getRecoveryEpoch() {
    return recoveryEpoch;
  }

  public BrokerPartitionConfigurationRecord setRecoveryEpoch(final long value) {
    recoveryEpoch = value;
    return this;
  }

  public long recoveryEpoch() {
    return recoveryEpoch;
  }

  public long getExecutionGeneration() {
    return executionGeneration;
  }

  public BrokerPartitionConfigurationRecord setExecutionGeneration(final long value) {
    executionGeneration = value;
    return this;
  }

  public long executionGeneration() {
    return executionGeneration;
  }

  public String getOwnerChangeId() {
    return ownerChangeId;
  }

  public BrokerPartitionConfigurationRecord setOwnerChangeId(final String value) {
    ownerChangeId = Objects.requireNonNull(value, "ownerChangeId");
    return this;
  }

  public String ownerChangeId() {
    return ownerChangeId;
  }

  public long getAuthorityIndex() {
    return authorityIndex;
  }

  public BrokerPartitionConfigurationRecord setAuthorityIndex(final long value) {
    authorityIndex = value;
    return this;
  }

  public long authorityIndex() {
    return authorityIndex;
  }

  public long getExpectedGeneration() {
    return expectedGeneration;
  }

  public BrokerPartitionConfigurationRecord setExpectedGeneration(final long value) {
    expectedGeneration = value;
    return this;
  }

  public long expectedGeneration() {
    return expectedGeneration;
  }

  public long getGeneration() {
    return generation;
  }

  public BrokerPartitionConfigurationRecord setGeneration(final long value) {
    generation = value;
    return this;
  }

  public long generation() {
    return generation;
  }

  public long getExpectedRecoveryEpoch() {
    return expectedRecoveryEpoch;
  }

  public BrokerPartitionConfigurationRecord setExpectedRecoveryEpoch(final long value) {
    expectedRecoveryEpoch = value;
    return this;
  }

  public long expectedRecoveryEpoch() {
    return expectedRecoveryEpoch;
  }
}
