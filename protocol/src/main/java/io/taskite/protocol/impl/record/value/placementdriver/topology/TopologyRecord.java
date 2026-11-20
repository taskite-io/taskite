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
import io.taskite.protocol.record.value.placementdriver.topology.TopologyRecordValue;
import io.taskite.topology.BrokerPartitionElectionState;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import io.taskite.topology.RegistrationStatus;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TopologyRecord extends UnifiedRecordValue implements TopologyRecordValue {
  private String clusterId = "";
  private long version;
  private int clusterSize;
  private int partitionSize;
  private int replicationFactor;
  private long updatedAtEpochMillis;
  private List<BrokerEndpointRecord> brokers = new ArrayList<>();
  private List<PartitionRecord> partitions = new ArrayList<>();

  @Override
  public String getClusterId() {
    return clusterId;
  }

  public TopologyRecord setClusterId(final String clusterId) {
    this.clusterId = clusterId == null ? "" : clusterId;
    return this;
  }

  @Override
  public long getVersion() {
    return version;
  }

  public TopologyRecord setVersion(final long version) {
    this.version = version;
    return this;
  }

  @Override
  public int getClusterSize() {
    return clusterSize;
  }

  public TopologyRecord setClusterSize(final int clusterSize) {
    this.clusterSize = Math.max(0, clusterSize);
    return this;
  }

  @Override
  public int getPartitionSize() {
    return partitionSize;
  }

  public TopologyRecord setPartitionSize(final int partitionSize) {
    this.partitionSize = partitionSize;
    return this;
  }

  @Override
  public int getReplicationFactor() {
    return replicationFactor;
  }

  public TopologyRecord setReplicationFactor(final int replicationFactor) {
    this.replicationFactor = replicationFactor;
    return this;
  }

  @Override
  public long getUpdatedAtEpochMillis() {
    return updatedAtEpochMillis;
  }

  public TopologyRecord setUpdatedAtEpochMillis(final long updatedAtEpochMillis) {
    this.updatedAtEpochMillis = updatedAtEpochMillis;
    return this;
  }

  @Override
  public List<BrokerEndpointRecord> getBrokers() {
    return brokers;
  }

  public TopologyRecord setBrokers(final List<BrokerEndpointRecord> brokers) {
    this.brokers = brokers == null ? new ArrayList<>() : new ArrayList<>(brokers);
    return this;
  }

  @Override
  public List<PartitionRecord> getPartitions() {
    return partitions;
  }

  public TopologyRecord setPartitions(final List<PartitionRecord> partitions) {
    this.partitions = partitions == null ? new ArrayList<>() : new ArrayList<>(partitions);
    return this;
  }

  public static class BrokerEndpointRecord
      implements TopologyRecordValue.BrokerEndpointRecordValue {
    private String brokerId = "";
    private String host = "";
    private int port;
    private int raftPort;
    private String version = "";
    private long registeredAtEpochMillis;
    private long lastRenewalEpochMillis;
    private long renewalTimeoutMillis;
    private RegistrationStatus status = RegistrationStatus.UNKNOWN;
    private String incarnationId = "";

    public String getIncarnationId() {
      return incarnationId;
    }

    public BrokerEndpointRecord setIncarnationId(final String value) {
      incarnationId = value == null ? "" : value;
      return this;
    }

    private List<BrokerPartitionRecord> partitions = new ArrayList<>();

    @Override
    public String getBrokerId() {
      return brokerId;
    }

    public BrokerEndpointRecord setBrokerId(final String brokerId) {
      this.brokerId = brokerId == null ? "" : brokerId;
      return this;
    }

    @Override
    public String getHost() {
      return host;
    }

    public BrokerEndpointRecord setHost(final String host) {
      this.host = host == null ? "" : host;
      return this;
    }

    @Override
    public int getPort() {
      return port;
    }

    public BrokerEndpointRecord setPort(final int port) {
      this.port = port;
      return this;
    }

    @Override
    public int getRaftPort() {
      return raftPort;
    }

    public BrokerEndpointRecord setRaftPort(final int raftPort) {
      this.raftPort = raftPort;
      return this;
    }

    @Override
    public String getVersion() {
      return version;
    }

    public BrokerEndpointRecord setVersion(final String version) {
      this.version = version == null ? "" : version;
      return this;
    }

    @Override
    public long getRegisteredAtEpochMillis() {
      return registeredAtEpochMillis;
    }

    public BrokerEndpointRecord setRegisteredAtEpochMillis(final long registeredAtEpochMillis) {
      this.registeredAtEpochMillis = Math.max(0L, registeredAtEpochMillis);
      return this;
    }

    @Override
    public long getLastRenewalEpochMillis() {
      return lastRenewalEpochMillis;
    }

    public BrokerEndpointRecord setLastRenewalEpochMillis(final long lastRenewalEpochMillis) {
      this.lastRenewalEpochMillis = Math.max(0L, lastRenewalEpochMillis);
      return this;
    }

    @Override
    public long getRenewalTimeoutMillis() {
      return renewalTimeoutMillis;
    }

    public BrokerEndpointRecord setRenewalTimeoutMillis(final long renewalTimeoutMillis) {
      this.renewalTimeoutMillis = Math.max(0L, renewalTimeoutMillis);
      return this;
    }

    @Override
    public RegistrationStatus getStatus() {
      return status;
    }

    public BrokerEndpointRecord setStatus(final RegistrationStatus status) {
      this.status = status == null ? RegistrationStatus.UNKNOWN : status;
      return this;
    }

    @Override
    public List<BrokerPartitionRecord> getPartitions() {
      return partitions;
    }

    public BrokerEndpointRecord setPartitions(final List<BrokerPartitionRecord> partitions) {
      this.partitions = partitions == null ? new ArrayList<>() : new ArrayList<>(partitions);
      return this;
    }
  }

  public static class BrokerPartitionRecord
      implements TopologyRecordValue.BrokerPartitionRecordValue {
    private int partitionId;
    private MemberRole role = MemberRole.UNKNOWN;
    private long term;
    private BrokerPartitionLifecycle lifecycleState = BrokerPartitionLifecycle.ABSENT;
    private long matchIndex;
    private long lag;
    private boolean readyForPromotion;
    private int priority = -1;
    private String lastOpId = "";
    private BrokerTopologyOperationStatus lastOpStatus = BrokerTopologyOperationStatus.NONE;
    private long recoveryEpoch;
    private BrokerPartitionElectionState election = BrokerPartitionElectionState.UNKNOWN;

    public BrokerPartitionElectionState getElection() {
      return election;
    }

    public BrokerPartitionRecord setElection(final BrokerPartitionElectionState value) {
      election = value;
      return this;
    }

    @Override
    public int getPartitionId() {
      return partitionId;
    }

    public BrokerPartitionRecord setPartitionId(final int partitionId) {
      this.partitionId = partitionId;
      return this;
    }

    @Override
    public MemberRole getRole() {
      return role;
    }

    public BrokerPartitionRecord setRole(final MemberRole role) {
      this.role = role == null ? MemberRole.UNKNOWN : role;
      return this;
    }

    @Override
    public long getTerm() {
      return term;
    }

    public BrokerPartitionRecord setTerm(final long term) {
      this.term = term;
      return this;
    }

    @Override
    public BrokerPartitionLifecycle getLifecycleState() {
      return lifecycleState;
    }

    public BrokerPartitionRecord setLifecycleState(final BrokerPartitionLifecycle lifecycleState) {
      this.lifecycleState =
          lifecycleState == null ? BrokerPartitionLifecycle.ABSENT : lifecycleState;
      return this;
    }

    @Override
    public long getMatchIndex() {
      return matchIndex;
    }

    public BrokerPartitionRecord setMatchIndex(final long matchIndex) {
      this.matchIndex = matchIndex;
      return this;
    }

    @Override
    public long getLag() {
      return lag;
    }

    public BrokerPartitionRecord setLag(final long lag) {
      this.lag = lag;
      return this;
    }

    @Override
    public boolean isReadyForPromotion() {
      return readyForPromotion;
    }

    public BrokerPartitionRecord setReadyForPromotion(final boolean readyForPromotion) {
      this.readyForPromotion = readyForPromotion;
      return this;
    }

    @Override
    public int getPriority() {
      return priority;
    }

    public BrokerPartitionRecord setPriority(final int priority) {
      this.priority = priority;
      return this;
    }

    @Override
    public String getLastOpId() {
      return lastOpId;
    }

    public BrokerPartitionRecord setLastOpId(final String lastOpId) {
      this.lastOpId = lastOpId == null ? "" : lastOpId;
      return this;
    }

    @Override
    public BrokerTopologyOperationStatus getLastOpStatus() {
      return lastOpStatus;
    }

    public BrokerPartitionRecord setLastOpStatus(final BrokerTopologyOperationStatus lastOpStatus) {
      this.lastOpStatus = lastOpStatus == null ? BrokerTopologyOperationStatus.NONE : lastOpStatus;
      return this;
    }

    @Override
    public long getRecoveryEpoch() {
      return recoveryEpoch;
    }

    public BrokerPartitionRecord setRecoveryEpoch(final long recoveryEpoch) {
      this.recoveryEpoch = Math.max(0L, recoveryEpoch);
      return this;
    }
  }

  public static class PartitionRecord implements TopologyRecordValue.PartitionRecordValue {
    private int partitionId;
    private String leaderId = "";
    private List<String> replicas = new ArrayList<>();
    private List<String> learners = new ArrayList<>();
    private long term;
    private long recoveryEpoch;
    private PartitionStatus status;
    private boolean serviceReady;
    private Map<String, Integer> priorities = new LinkedHashMap<>();

    @Override
    public boolean isServiceReady() {
      return serviceReady;
    }

    public PartitionRecord setServiceReady(final boolean serviceReady) {
      this.serviceReady = serviceReady;
      return this;
    }

    @Override
    public int getPartitionId() {
      return partitionId;
    }

    public PartitionRecord setPartitionId(final int partitionId) {
      this.partitionId = partitionId;
      return this;
    }

    @Override
    public String getLeaderId() {
      return leaderId;
    }

    public PartitionRecord setLeaderId(final String leaderId) {
      this.leaderId = leaderId == null ? "" : leaderId;
      return this;
    }

    @Override
    public List<String> getReplicas() {
      return replicas;
    }

    public PartitionRecord setReplicas(final List<String> replicas) {
      this.replicas = replicas == null ? new ArrayList<>() : new ArrayList<>(replicas);
      return this;
    }

    @Override
    public List<String> getLearners() {
      return learners;
    }

    public PartitionRecord setLearners(final List<String> learners) {
      this.learners = learners == null ? new ArrayList<>() : new ArrayList<>(learners);
      return this;
    }

    @Override
    public long getTerm() {
      return term;
    }

    public PartitionRecord setTerm(final long term) {
      this.term = term;
      return this;
    }

    @Override
    public long getRecoveryEpoch() {
      return recoveryEpoch;
    }

    public PartitionRecord setRecoveryEpoch(final long recoveryEpoch) {
      this.recoveryEpoch = Math.max(0L, recoveryEpoch);
      return this;
    }

    @Override
    public PartitionStatus getStatus() {
      return status;
    }

    public PartitionRecord setStatus(final PartitionStatus status) {
      this.status = status;
      return this;
    }

    @Override
    public Map<String, Integer> getPriorities() {
      return priorities;
    }

    public PartitionRecord setPriorities(final Map<String, Integer> priorities) {
      this.priorities =
          priorities == null ? new LinkedHashMap<>() : new LinkedHashMap<>(priorities);
      return this;
    }
  }
}
