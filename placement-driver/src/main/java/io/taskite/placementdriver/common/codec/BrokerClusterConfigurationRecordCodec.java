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
package io.taskite.placementdriver.common.codec;

import io.taskite.protocol.record.ClusterTopologyRecordDecoder;
import io.taskite.protocol.record.ClusterTopologyRecordEncoder;
import io.taskite.protocol.record.MessageHeaderDecoder;
import io.taskite.protocol.record.MessageHeaderEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Persistent representation of a cluster's topology configuration (cluster/partition/replication
 * sizing), serialized as SBE bytes into the {@code default} column family.
 */
public final class BrokerClusterConfigurationRecordCodec {
  private String clusterId = "";
  private int clusterSize = 1;
  private int partitionSize = 1;
  private int replicationFactor = 1;
  private long rosterEpoch = 0;
  private List<String> voterRoster = new ArrayList<>();

  public String getClusterId() {
    return clusterId;
  }

  public BrokerClusterConfigurationRecordCodec setClusterId(String clusterId) {
    this.clusterId = clusterId == null ? "" : clusterId;
    return this;
  }

  public int getClusterSize() {
    return clusterSize;
  }

  public BrokerClusterConfigurationRecordCodec setClusterSize(int clusterSize) {
    this.clusterSize = clusterSize;
    return this;
  }

  public int getPartitionSize() {
    return partitionSize;
  }

  public BrokerClusterConfigurationRecordCodec setPartitionSize(int partitionSize) {
    this.partitionSize = partitionSize;
    return this;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public BrokerClusterConfigurationRecordCodec setReplicationFactor(int replicationFactor) {
    this.replicationFactor = replicationFactor;
    return this;
  }

  public long getRosterEpoch() {
    return rosterEpoch;
  }

  public BrokerClusterConfigurationRecordCodec setRosterEpoch(long rosterEpoch) {
    this.rosterEpoch = rosterEpoch;
    return this;
  }

  public List<String> getVoterRoster() {
    return voterRoster;
  }

  public BrokerClusterConfigurationRecordCodec setVoterRoster(List<String> voterRoster) {
    this.voterRoster = voterRoster == null ? new ArrayList<>() : new ArrayList<>(voterRoster);
    return this;
  }

  public byte[] toBytes() {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    MessageHeaderEncoder header = new MessageHeaderEncoder();
    ClusterTopologyRecordEncoder body = new ClusterTopologyRecordEncoder();
    body.wrapAndApplyHeader(buffer, 0, header)
        .clusterSize(clusterSize)
        .partitionSize(partitionSize)
        .replicationFactor(replicationFactor)
        .rosterEpoch(rosterEpoch);
    // SBE wire order: fixed block, then repeating groups, then trailing var-data.
    final ClusterTopologyRecordEncoder.VoterRosterEncoder roster =
        body.voterRosterCount(voterRoster.size());
    for (final String brokerId : voterRoster) {
      final byte[] id = bytes(brokerId);
      roster.next().putBrokerId(id, 0, id.length);
    }
    byte[] cluster = bytes(clusterId);
    body.putClusterId(cluster, 0, cluster.length);
    return Arrays.copyOf(
        buffer.byteArray(), MessageHeaderEncoder.ENCODED_LENGTH + body.encodedLength());
  }

  public BrokerClusterConfigurationRecordCodec wrap(byte[] encoded) {
    UnsafeBuffer buffer = new UnsafeBuffer(encoded);
    MessageHeaderDecoder header = new MessageHeaderDecoder();
    ClusterTopologyRecordDecoder body = new ClusterTopologyRecordDecoder();
    header.wrap(buffer, 0);
    body.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
    clusterSize = body.clusterSize();
    partitionSize = body.partitionSize();
    replicationFactor = body.replicationFactor();
    // v6 (append): old v<6 records report actingVersion < 6, so the generated decoder yields the
    // null value (0) for rosterEpoch and an empty voterRoster group here.
    rosterEpoch = body.rosterEpoch();
    voterRoster = new ArrayList<>();
    for (final ClusterTopologyRecordDecoder.VoterRosterDecoder group : body.voterRoster()) {
      voterRoster.add(group.brokerId());
    }
    clusterId = body.clusterId();
    return this;
  }

  private static byte[] bytes(String value) {
    return value == null || value.isEmpty() ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
  }
}
