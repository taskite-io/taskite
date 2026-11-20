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
import io.taskite.protocol.record.value.placementdriver.topology.MemberTopologyRecordValue;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyUpdateType;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RegistrationStatus;
import java.util.ArrayList;
import java.util.List;

public class MemberTopologyRecord extends UnifiedRecordValue implements MemberTopologyRecordValue {
  private TopologyUpdateType updateType = TopologyUpdateType.CONFIGURATION;
  private String localMemberId = "";
  private String leaderId = "";
  private String leaderEndpoint = "";
  private long term;
  private long configurationEpoch;
  private List<MemberRecord> members = new ArrayList<>();

  @Override
  public TopologyUpdateType getUpdateType() {
    return updateType;
  }

  public MemberTopologyRecord setUpdateType(final TopologyUpdateType updateType) {
    this.updateType = updateType == null ? TopologyUpdateType.CONFIGURATION : updateType;
    return this;
  }

  @Override
  public String getLocalMemberId() {
    return localMemberId;
  }

  public MemberTopologyRecord setLocalMemberId(final String localMemberId) {
    this.localMemberId = localMemberId == null ? "" : localMemberId;
    return this;
  }

  @Override
  public String getLeaderId() {
    return leaderId;
  }

  public MemberTopologyRecord setLeaderId(final String leaderId) {
    this.leaderId = leaderId == null ? "" : leaderId;
    return this;
  }

  @Override
  public String getLeaderEndpoint() {
    return leaderEndpoint;
  }

  public MemberTopologyRecord setLeaderEndpoint(final String leaderEndpoint) {
    this.leaderEndpoint = leaderEndpoint == null ? "" : leaderEndpoint;
    return this;
  }

  @Override
  public long getTerm() {
    return term;
  }

  public MemberTopologyRecord setTerm(final long term) {
    this.term = term;
    return this;
  }

  @Override
  public long getConfigurationEpoch() {
    return configurationEpoch;
  }

  public MemberTopologyRecord setConfigurationEpoch(final long configurationEpoch) {
    this.configurationEpoch = Math.max(0L, configurationEpoch);
    return this;
  }

  @Override
  public List<MemberRecord> getMembers() {
    return members;
  }

  public MemberTopologyRecord setMembers(final List<MemberRecord> members) {
    this.members = members == null ? new ArrayList<>() : new ArrayList<>(members);
    return this;
  }

  public static class MemberRecord implements MemberTopologyRecordValue.MemberRecordValue {
    private String memberId = "";
    private String endpoint = "";
    private MemberRole role = MemberRole.UNKNOWN;
    private boolean self;
    private String host = "";
    private int httpPort;
    private int grpcPort;
    private int raftPort;
    private RegistrationStatus status = RegistrationStatus.UNKNOWN;
    private String incarnationId = "";
    private long endpointEpoch;
    private long commitIndex;
    private long lastApplied;
    private long lastLogIndex;
    private boolean readyForPromotion;

    @Override
    public String getMemberId() {
      return memberId;
    }

    public MemberRecord setMemberId(final String memberId) {
      this.memberId = memberId == null ? "" : memberId;
      return this;
    }

    @Override
    public String getEndpoint() {
      return endpoint;
    }

    public MemberRecord setEndpoint(final String endpoint) {
      this.endpoint = endpoint == null ? "" : endpoint;
      return this;
    }

    @Override
    public MemberRole getRole() {
      return role;
    }

    public MemberRecord setRole(final MemberRole role) {
      this.role = role == null ? MemberRole.UNKNOWN : role;
      return this;
    }

    @Override
    public boolean isSelf() {
      return self;
    }

    public MemberRecord setSelf(final boolean self) {
      this.self = self;
      return this;
    }

    @Override
    public String getHost() {
      return host;
    }

    public MemberRecord setHost(final String host) {
      this.host = host == null ? "" : host;
      return this;
    }

    @Override
    public int getHttpPort() {
      return httpPort;
    }

    public MemberRecord setHttpPort(final int httpPort) {
      this.httpPort = httpPort;
      return this;
    }

    @Override
    public int getGrpcPort() {
      return grpcPort;
    }

    public MemberRecord setGrpcPort(final int grpcPort) {
      this.grpcPort = grpcPort;
      return this;
    }

    @Override
    public int getRaftPort() {
      return raftPort;
    }

    public MemberRecord setRaftPort(final int raftPort) {
      this.raftPort = raftPort;
      return this;
    }

    @Override
    public RegistrationStatus getStatus() {
      return status;
    }

    public MemberRecord setStatus(final RegistrationStatus status) {
      this.status = status == null ? RegistrationStatus.UNKNOWN : status;
      return this;
    }

    @Override
    public String getIncarnationId() {
      return incarnationId;
    }

    public MemberRecord setIncarnationId(final String incarnationId) {
      this.incarnationId = incarnationId == null ? "" : incarnationId;
      return this;
    }

    @Override
    public long getEndpointEpoch() {
      return endpointEpoch;
    }

    public MemberRecord setEndpointEpoch(final long endpointEpoch) {
      this.endpointEpoch = Math.max(0L, endpointEpoch);
      return this;
    }

    @Override
    public long getCommitIndex() {
      return commitIndex;
    }

    public MemberRecord setCommitIndex(final long commitIndex) {
      this.commitIndex = Math.max(0L, commitIndex);
      return this;
    }

    @Override
    public long getLastApplied() {
      return lastApplied;
    }

    public MemberRecord setLastApplied(final long lastApplied) {
      this.lastApplied = Math.max(0L, lastApplied);
      return this;
    }

    @Override
    public long getLastLogIndex() {
      return lastLogIndex;
    }

    public MemberRecord setLastLogIndex(final long lastLogIndex) {
      this.lastLogIndex = Math.max(0L, lastLogIndex);
      return this;
    }

    @Override
    public boolean isReadyForPromotion() {
      return readyForPromotion;
    }

    public MemberRecord setReadyForPromotion(final boolean readyForPromotion) {
      this.readyForPromotion = readyForPromotion;
      return this;
    }
  }
}
