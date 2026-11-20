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

import io.taskite.protocol.record.BrokerRegistrationRequestDecoder;
import io.taskite.protocol.record.BrokerRegistrationRequestEncoder;
import io.taskite.protocol.record.MessageHeaderDecoder;
import io.taskite.protocol.record.MessageHeaderEncoder;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.placementdriver.topology.BrokerRegistrationRecordValue;
import io.taskite.topology.BrokerPartitionElectionState;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RaftConfigurationReference;
import io.taskite.topology.RaftNodeState;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;

public class BrokerRegistrationRecord extends UnifiedRecordValue
    implements BrokerRegistrationRecordValue {
  public static final long DEFAULT_RENEWAL_TIMEOUT_MILLIS = 30_000L;

  private String clusterId = "";
  private String brokerId = "";
  private String host = "";
  private int port;
  private int raftPort;
  private long renewalTimeoutMillis = DEFAULT_RENEWAL_TIMEOUT_MILLIS;
  private long expectedLastRenewalEpochMillis;
  private long eventTimestampEpochMillis;
  private String version = "";
  private String incarnationId = "";
  private boolean preservePartitionObservations;

  public boolean isPreservePartitionObservations() {
    return preservePartitionObservations;
  }

  public BrokerRegistrationRecord setPreservePartitionObservations(final boolean value) {
    preservePartitionObservations = value;
    return this;
  }

  public String getIncarnationId() {
    return incarnationId;
  }

  public BrokerRegistrationRecord setIncarnationId(final String value) {
    incarnationId = value == null ? "" : value;
    return this;
  }

  private List<PartitionStateRecord> partitions = new ArrayList<>();

  @Override
  public String getClusterId() {
    return clusterId;
  }

  public BrokerRegistrationRecord setClusterId(final String clusterId) {
    this.clusterId = clusterId == null ? "" : clusterId;
    return this;
  }

  @Override
  public String getBrokerId() {
    return brokerId;
  }

  public BrokerRegistrationRecord setBrokerId(final String brokerId) {
    this.brokerId = brokerId == null ? "" : brokerId;
    return this;
  }

  @Override
  public String getHost() {
    return host;
  }

  public BrokerRegistrationRecord setHost(final String host) {
    this.host = host == null ? "" : host;
    return this;
  }

  @Override
  public int getPort() {
    return port;
  }

  public BrokerRegistrationRecord setPort(final int port) {
    this.port = port;
    return this;
  }

  @Override
  public int getRaftPort() {
    return raftPort;
  }

  public BrokerRegistrationRecord setRaftPort(final int raftPort) {
    this.raftPort = raftPort;
    return this;
  }

  @Override
  public long getRenewalTimeoutMillis() {
    return renewalTimeoutMillis;
  }

  public BrokerRegistrationRecord setRenewalTimeoutMillis(final long renewalTimeoutMillis) {
    if (renewalTimeoutMillis <= 0) {
      throw new IllegalArgumentException("renewalTimeoutMillis must be positive");
    }
    this.renewalTimeoutMillis = renewalTimeoutMillis;
    return this;
  }

  @Override
  public long getExpectedLastRenewalEpochMillis() {
    return expectedLastRenewalEpochMillis;
  }

  public BrokerRegistrationRecord setExpectedLastRenewalEpochMillis(
      final long expectedLastRenewalEpochMillis) {
    this.expectedLastRenewalEpochMillis = Math.max(0L, expectedLastRenewalEpochMillis);
    return this;
  }

  @Override
  public long getEventTimestampEpochMillis() {
    return eventTimestampEpochMillis;
  }

  public BrokerRegistrationRecord setEventTimestampEpochMillis(
      final long eventTimestampEpochMillis) {
    this.eventTimestampEpochMillis = Math.max(0L, eventTimestampEpochMillis);
    return this;
  }

  @Override
  public String getVersion() {
    return version;
  }

  public BrokerRegistrationRecord setVersion(final String version) {
    this.version = version == null ? "" : version;
    return this;
  }

  @Override
  public List<PartitionStateRecord> getPartitions() {
    return partitions;
  }

  public BrokerRegistrationRecord setPartitions(final List<PartitionStateRecord> partitions) {
    this.partitions = partitions == null ? new ArrayList<>() : new ArrayList<>(partitions);
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
    final BrokerRegistrationRequestDecoder body = new BrokerRegistrationRequestDecoder();
    header.wrap(buffer, offset);
    if (header.templateId() != BrokerRegistrationRequestDecoder.TEMPLATE_ID) {
      throw new IllegalArgumentException(
          "Expected BrokerRegistrationRequest template but got " + header.templateId());
    }
    if (header.version() < 9
        || header.version() > BrokerRegistrationRequestDecoder.SCHEMA_VERSION) {
      throw new IllegalArgumentException(
          "BrokerRegistrationRequest requires schema version "
              + BrokerRegistrationRequestDecoder.SCHEMA_VERSION
              + " but got version "
              + header.version());
    }
    final int expectedBlockLength =
        header.version() == 9 ? 32 : BrokerRegistrationRequestDecoder.BLOCK_LENGTH;
    if (header.blockLength() != expectedBlockLength) {
      throw new IllegalArgumentException(
          "BrokerRegistrationRequest requires block length "
              + BrokerRegistrationRequestDecoder.BLOCK_LENGTH
              + " but got "
              + header.blockLength());
    }
    body.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        header.blockLength(),
        header.version());
    port = body.port();
    raftPort = body.raftPort();
    renewalTimeoutMillis = body.renewalTimeoutMillis();
    if (renewalTimeoutMillis <= 0) {
      throw new IllegalArgumentException("renewalTimeoutMillis must be positive");
    }
    expectedLastRenewalEpochMillis = Math.max(0L, body.expectedLastRenewalEpochMillis());
    eventTimestampEpochMillis = Math.max(0L, body.eventTimestampEpochMillis());
    preservePartitionObservations =
        header.version() >= 10 && body.preservePartitionObservations() == 1;
    partitions.clear();
    for (final BrokerRegistrationRequestDecoder.PartitionsDecoder partition : body.partitions()) {
      if (partition.actingBlockLength()
          != (header.version() == 9
              ? 41
              : BrokerRegistrationRequestDecoder.PartitionsDecoder.sbeBlockLength())) {
        throw new IllegalArgumentException("Partition report layout does not match schema version");
      }
      partitions.add(
          new PartitionStateRecord()
              .setPartitionId(partition.partitionId())
              .setTerm(partition.term())
              .setMatchIndex(partition.matchIndex())
              .setLag(partition.lag())
              .setReadyForPromotion(partition.readyForPromotion() == 1)
              .setPriority(partition.priority())
              .setRecoveryEpoch(partition.recoveryEpoch())
              .setRole(MemberRole.parse(partition.role()))
              .setLifecycleState(BrokerPartitionLifecycle.parse(partition.lifecycleState()))
              .setLastOpId(partition.lastOpId())
              .setLastOpStatus(BrokerTopologyOperationStatus.parse(partition.lastOpStatus()))
              .setElection(
                  header.version() < 10
                      ? BrokerPartitionElectionState.UNKNOWN
                      : new BrokerPartitionElectionState(
                          new RaftConfigurationReference(
                              partition.configurationIndex(), partition.configurationDigest()),
                          new BrokerPartitionElectionState.Priorities(
                              partition.desiredPriority(), partition.targetPriority()),
                          new BrokerPartitionElectionState.Observation(
                              RaftNodeState.parse(partition.nodeState()),
                              partition.leaderId(),
                              partition.serviceReady() == 1,
                              partition.observedAtEpochMillis()))));
    }
    clusterId = body.clusterId();
    brokerId = body.brokerId();
    host = body.host();
    version = body.version();
    incarnationId = body.incarnationId();
  }

  private byte[] encode() {
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final BrokerRegistrationRequestEncoder body = new BrokerRegistrationRequestEncoder();
    body.wrapAndApplyHeader(buffer, 0, header)
        .port(port)
        .renewalTimeoutMillis(renewalTimeoutMillis)
        .expectedLastRenewalEpochMillis(expectedLastRenewalEpochMillis)
        .eventTimestampEpochMillis(eventTimestampEpochMillis)
        .raftPort(raftPort)
        .preservePartitionObservations((short) (preservePartitionObservations ? 1 : 0));
    final BrokerRegistrationRequestEncoder.PartitionsEncoder encodedPartitions =
        body.partitionsCount(partitions.size());
    for (final PartitionStateRecord partition : partitions) {
      encodedPartitions
          .next()
          .partitionId(partition.getPartitionId())
          .term(partition.getTerm())
          .matchIndex(partition.getMatchIndex())
          .lag(partition.getLag())
          .readyForPromotion((short) (partition.isReadyForPromotion() ? 1 : 0))
          .priority(partition.getPriority())
          .recoveryEpoch(partition.getRecoveryEpoch())
          .configurationIndex(partition.getElection().configurationIndex())
          .desiredPriority(partition.getElection().desiredPriority())
          .targetPriority(partition.getElection().targetPriority())
          .serviceReady((short) (partition.getElection().serviceReady() ? 1 : 0))
          .observedAtEpochMillis(partition.getElection().observedAtEpochMillis())
          .role(partition.getRole().name())
          .lifecycleState(partition.getLifecycleState().name())
          .lastOpId(partition.getLastOpId())
          .lastOpStatus(partition.getLastOpStatus().name())
          .configurationDigest(partition.getElection().configurationDigest())
          .nodeState(partition.getElection().nodeState().name())
          .leaderId(partition.getElection().leaderId());
    }
    body.clusterId(clusterId)
        .brokerId(brokerId)
        .host(host)
        .version(version)
        .incarnationId(incarnationId);
    return Arrays.copyOf(
        buffer.byteArray(), MessageHeaderEncoder.ENCODED_LENGTH + body.encodedLength());
  }

  public static class PartitionStateRecord
      implements BrokerRegistrationRecordValue.PartitionStateRecordValue {
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

    public PartitionStateRecord setElection(final BrokerPartitionElectionState value) {
      election = value == null ? BrokerPartitionElectionState.UNKNOWN : value;
      return this;
    }

    @Override
    public int getPartitionId() {
      return partitionId;
    }

    public PartitionStateRecord setPartitionId(final int partitionId) {
      this.partitionId = partitionId;
      return this;
    }

    @Override
    public MemberRole getRole() {
      return role;
    }

    public PartitionStateRecord setRole(final MemberRole role) {
      this.role = role == null ? MemberRole.UNKNOWN : role;
      return this;
    }

    @Override
    public long getTerm() {
      return term;
    }

    public PartitionStateRecord setTerm(final long term) {
      this.term = term;
      return this;
    }

    public BrokerPartitionLifecycle getLifecycleState() {
      return lifecycleState;
    }

    public PartitionStateRecord setLifecycleState(final BrokerPartitionLifecycle lifecycleState) {
      this.lifecycleState =
          lifecycleState == null ? BrokerPartitionLifecycle.ABSENT : lifecycleState;
      return this;
    }

    public long getMatchIndex() {
      return matchIndex;
    }

    public PartitionStateRecord setMatchIndex(final long matchIndex) {
      this.matchIndex = matchIndex;
      return this;
    }

    public long getLag() {
      return lag;
    }

    public PartitionStateRecord setLag(final long lag) {
      this.lag = Math.max(0, lag);
      return this;
    }

    public boolean isReadyForPromotion() {
      return readyForPromotion;
    }

    public PartitionStateRecord setReadyForPromotion(final boolean readyForPromotion) {
      this.readyForPromotion = readyForPromotion;
      return this;
    }

    public int getPriority() {
      return priority;
    }

    public PartitionStateRecord setPriority(final int priority) {
      this.priority = priority;
      return this;
    }

    public String getLastOpId() {
      return lastOpId;
    }

    public PartitionStateRecord setLastOpId(final String lastOpId) {
      this.lastOpId = lastOpId == null ? "" : lastOpId;
      return this;
    }

    public BrokerTopologyOperationStatus getLastOpStatus() {
      return lastOpStatus;
    }

    public PartitionStateRecord setLastOpStatus(final BrokerTopologyOperationStatus lastOpStatus) {
      this.lastOpStatus = lastOpStatus == null ? BrokerTopologyOperationStatus.NONE : lastOpStatus;
      return this;
    }

    public long getRecoveryEpoch() {
      return recoveryEpoch;
    }

    public PartitionStateRecord setRecoveryEpoch(final long recoveryEpoch) {
      this.recoveryEpoch = Math.max(0L, recoveryEpoch);
      return this;
    }
  }
}
