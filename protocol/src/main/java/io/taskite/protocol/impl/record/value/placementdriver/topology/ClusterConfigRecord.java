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
package io.taskite.protocol.impl.record.value.placementdriver.topology;

import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.placementdriver.topology.ClusterConfigRecordValue;
import io.taskite.protocol.record.value.placementdriver.topology.ClusterConfigurationOutcome;
import java.util.ArrayList;
import java.util.List;

public class ClusterConfigRecord extends UnifiedRecordValue implements ClusterConfigRecordValue {
  private String clusterId = "";
  private long clusterKey = -1L;
  private int clusterSize;
  private int partitionSize;
  private int replicationFactor;
  private List<String> voterRoster = new ArrayList<>();
  private List<String> removedBrokers = new ArrayList<>();
  private String changeId = "";
  private long sourceTopologyVersion;
  private int sourceClusterSize;
  private int sourcePartitionSize;
  private int sourceReplicationFactor;
  private List<String> sourceVoterRoster = new ArrayList<>();
  private List<String> encodedChangePlans = new ArrayList<>();
  private boolean rosterManaged;
  private String adminOperation = "";
  private List<String> requestedBrokerAdditions = new ArrayList<>();
  private List<String> requestedBrokerRemovals = new ArrayList<>();
  private Integer requestedBrokerCount;
  private Integer requestedPartitionCount;
  private Integer requestedReplicationFactor;
  private Integer requestedPartitionId;
  private String confirmation = "";
  private String fencingProof = "";
  private String survivorBrokerId = "";
  private long requestedAtEpochMillis;
  private ClusterConfigurationOutcome outcome;

  @Override
  public String getClusterId() {
    return clusterId;
  }

  @Override
  public long getClusterKey() {
    return clusterKey;
  }

  public ClusterConfigRecord setClusterKey(final long clusterKey) {
    this.clusterKey = clusterKey;
    return this;
  }

  public ClusterConfigRecord setClusterId(final String clusterId) {
    this.clusterId = clusterId == null ? "" : clusterId;
    return this;
  }

  @Override
  public int getClusterSize() {
    return clusterSize;
  }

  public ClusterConfigRecord setClusterSize(final int clusterSize) {
    this.clusterSize = clusterSize;
    return this;
  }

  @Override
  public int getPartitionSize() {
    return partitionSize;
  }

  public ClusterConfigRecord setPartitionSize(final int partitionSize) {
    this.partitionSize = partitionSize;
    return this;
  }

  @Override
  public int getReplicationFactor() {
    return replicationFactor;
  }

  public ClusterConfigRecord setReplicationFactor(final int replicationFactor) {
    this.replicationFactor = replicationFactor;
    return this;
  }

  public List<String> getVoterRoster() {
    return voterRoster;
  }

  public ClusterConfigRecord setVoterRoster(final List<String> voterRoster) {
    this.voterRoster = voterRoster == null ? new ArrayList<>() : new ArrayList<>(voterRoster);
    return this;
  }

  public List<String> getRemovedBrokers() {
    return removedBrokers;
  }

  public ClusterConfigRecord setRemovedBrokers(final List<String> removedBrokers) {
    this.removedBrokers =
        removedBrokers == null ? new ArrayList<>() : new ArrayList<>(removedBrokers);
    return this;
  }

  public String getChangeId() {
    return changeId;
  }

  public ClusterConfigRecord setChangeId(final String changeId) {
    this.changeId = changeId == null ? "" : changeId;
    return this;
  }

  public long getSourceTopologyVersion() {
    return sourceTopologyVersion;
  }

  public ClusterConfigRecord setSourceTopologyVersion(final long sourceTopologyVersion) {
    this.sourceTopologyVersion = sourceTopologyVersion;
    return this;
  }

  public int getSourceClusterSize() {
    return sourceClusterSize;
  }

  public ClusterConfigRecord setSourceClusterSize(final int sourceClusterSize) {
    this.sourceClusterSize = sourceClusterSize;
    return this;
  }

  public int getSourcePartitionSize() {
    return sourcePartitionSize;
  }

  public ClusterConfigRecord setSourcePartitionSize(final int sourcePartitionSize) {
    this.sourcePartitionSize = sourcePartitionSize;
    return this;
  }

  public int getSourceReplicationFactor() {
    return sourceReplicationFactor;
  }

  public ClusterConfigRecord setSourceReplicationFactor(final int sourceReplicationFactor) {
    this.sourceReplicationFactor = sourceReplicationFactor;
    return this;
  }

  public List<String> getSourceVoterRoster() {
    return sourceVoterRoster;
  }

  public ClusterConfigRecord setSourceVoterRoster(final List<String> sourceVoterRoster) {
    this.sourceVoterRoster =
        sourceVoterRoster == null ? new ArrayList<>() : new ArrayList<>(sourceVoterRoster);
    return this;
  }

  public List<String> getEncodedChangePlans() {
    return encodedChangePlans;
  }

  public ClusterConfigRecord setEncodedChangePlans(final List<String> encodedChangePlans) {
    this.encodedChangePlans =
        encodedChangePlans == null ? new ArrayList<>() : new ArrayList<>(encodedChangePlans);
    return this;
  }

  public boolean isRosterManaged() {
    return rosterManaged;
  }

  public ClusterConfigRecord setRosterManaged(final boolean rosterManaged) {
    this.rosterManaged = rosterManaged;
    return this;
  }

  public String getAdminOperation() {
    return adminOperation;
  }

  public ClusterConfigRecord setAdminOperation(final String adminOperation) {
    this.adminOperation = adminOperation == null ? "" : adminOperation;
    return this;
  }

  public List<String> getRequestedBrokerAdditions() {
    return requestedBrokerAdditions;
  }

  public ClusterConfigRecord setRequestedBrokerAdditions(
      final List<String> requestedBrokerAdditions) {
    this.requestedBrokerAdditions =
        requestedBrokerAdditions == null
            ? new ArrayList<>()
            : new ArrayList<>(requestedBrokerAdditions);
    return this;
  }

  public List<String> getRequestedBrokerRemovals() {
    return requestedBrokerRemovals;
  }

  public ClusterConfigRecord setRequestedBrokerRemovals(
      final List<String> requestedBrokerRemovals) {
    this.requestedBrokerRemovals =
        requestedBrokerRemovals == null
            ? new ArrayList<>()
            : new ArrayList<>(requestedBrokerRemovals);
    return this;
  }

  public Integer getRequestedBrokerCount() {
    return requestedBrokerCount;
  }

  public ClusterConfigRecord setRequestedBrokerCount(final Integer requestedBrokerCount) {
    this.requestedBrokerCount = requestedBrokerCount;
    return this;
  }

  public Integer getRequestedPartitionCount() {
    return requestedPartitionCount;
  }

  public ClusterConfigRecord setRequestedPartitionCount(final Integer requestedPartitionCount) {
    this.requestedPartitionCount = requestedPartitionCount;
    return this;
  }

  public Integer getRequestedReplicationFactor() {
    return requestedReplicationFactor;
  }

  public ClusterConfigRecord setRequestedReplicationFactor(
      final Integer requestedReplicationFactor) {
    this.requestedReplicationFactor = requestedReplicationFactor;
    return this;
  }

  public Integer getRequestedPartitionId() {
    return requestedPartitionId;
  }

  public ClusterConfigRecord setRequestedPartitionId(final Integer requestedPartitionId) {
    this.requestedPartitionId = requestedPartitionId;
    return this;
  }

  public String getConfirmation() {
    return confirmation;
  }

  public ClusterConfigRecord setConfirmation(final String confirmation) {
    this.confirmation = confirmation == null ? "" : confirmation;
    return this;
  }

  public String getFencingProof() {
    return fencingProof;
  }

  public ClusterConfigRecord setFencingProof(final String fencingProof) {
    this.fencingProof = fencingProof == null ? "" : fencingProof;
    return this;
  }

  public String getSurvivorBrokerId() {
    return survivorBrokerId;
  }

  public ClusterConfigRecord setSurvivorBrokerId(final String survivorBrokerId) {
    this.survivorBrokerId = survivorBrokerId == null ? "" : survivorBrokerId;
    return this;
  }

  public long getRequestedAtEpochMillis() {
    return requestedAtEpochMillis;
  }

  public ClusterConfigRecord setRequestedAtEpochMillis(final long requestedAtEpochMillis) {
    this.requestedAtEpochMillis = requestedAtEpochMillis;
    return this;
  }

  public ClusterConfigurationOutcome getOutcome() {
    return outcome;
  }

  public ClusterConfigRecord setOutcome(final ClusterConfigurationOutcome outcome) {
    this.outcome = outcome;
    return this;
  }
}
