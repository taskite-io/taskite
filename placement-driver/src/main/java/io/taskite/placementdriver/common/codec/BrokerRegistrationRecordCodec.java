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

import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.protocol.record.ClusterBrokerRecordDecoder;
import io.taskite.protocol.record.ClusterBrokerRecordEncoder;
import io.taskite.protocol.record.MessageHeaderDecoder;
import io.taskite.protocol.record.MessageHeaderEncoder;
import io.taskite.topology.BrokerPartitionElectionState;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RaftConfigurationReference;
import io.taskite.topology.RaftNodeState;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * SBE codec for a persisted {@link BrokerRegistration} stored in the {@code default} column family.
 * Instants are epoch millis and the partition list is carried directly in a repeating group.
 */
public final class BrokerRegistrationRecordCodec {
  private BrokerRegistrationRecordCodec() {}

  public static byte[] toBytes(BrokerRegistration registration) {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    MessageHeaderEncoder header = new MessageHeaderEncoder();
    ClusterBrokerRecordEncoder body = new ClusterBrokerRecordEncoder();
    body.wrapAndApplyHeader(buffer, 0, header)
        .port(registration.port())
        .raftPort(registration.raftPort())
        .registeredAtEpochMillis(registration.registeredAt().toEpochMilli())
        .lastRenewalEpochMillis(registration.lastRenewal().toEpochMilli())
        .renewalTimeoutMillis(registration.renewalTimeoutMillis());
    List<BrokerPartitionState> partitions = registration.partitions();
    ClusterBrokerRecordEncoder.PartitionsEncoder group = body.partitionsCount(partitions.size());
    for (BrokerPartitionState partition : partitions) {
      group
          .next()
          .partitionId(partition.partitionId())
          .term(partition.term())
          .matchIndex(partition.matchIndex())
          .lag(partition.lag())
          .readyForPromotion((short) (partition.readyForPromotion() ? 1 : 0))
          .priority(partition.priority())
          .recoveryEpoch(partition.recoveryEpoch())
          .configurationIndex(partition.election().configurationIndex())
          .desiredPriority(partition.election().desiredPriority())
          .targetPriority(partition.election().targetPriority())
          .serviceReady((short) (partition.election().serviceReady() ? 1 : 0))
          .observedAtEpochMillis(partition.election().observedAtEpochMillis())
          .role(partition.role().name())
          .lifecycleState(partition.lifecycleState().name())
          .lastOpId(partition.lastOpId())
          .lastOpStatus(partition.lastOpStatus().name())
          .configurationDigest(partition.election().configurationDigest())
          .nodeState(partition.election().nodeState().name())
          .leaderId(partition.election().leaderId());
    }
    putVar(body::putClusterId, registration.clusterId());
    putVar(body::putBrokerId, registration.brokerId());
    putVar(body::putHost, registration.host());
    putVar(body::putVersion, registration.version());
    putVar(body::putStatus, registration.status().name());
    putVar(body::putIncarnationId, registration.incarnationId());
    return Arrays.copyOf(
        buffer.byteArray(), MessageHeaderEncoder.ENCODED_LENGTH + body.encodedLength());
  }

  public static BrokerRegistration toRegistration(byte[] encoded) {
    UnsafeBuffer buffer = new UnsafeBuffer(encoded);
    MessageHeaderDecoder header = new MessageHeaderDecoder();
    ClusterBrokerRecordDecoder body = new ClusterBrokerRecordDecoder();
    header.wrap(buffer, 0);
    if (header.templateId() != ClusterBrokerRecordDecoder.TEMPLATE_ID
        || header.version() < 9
        || header.version() > ClusterBrokerRecordDecoder.SCHEMA_VERSION
        || header.blockLength() != ClusterBrokerRecordDecoder.BLOCK_LENGTH) {
      throw new IllegalArgumentException(
          "BrokerRegistrationRecordCodec requires template "
              + ClusterBrokerRecordDecoder.TEMPLATE_ID
              + " with schema version "
              + ClusterBrokerRecordDecoder.SCHEMA_VERSION
              + " with block length "
              + ClusterBrokerRecordDecoder.BLOCK_LENGTH
              + " but got template "
              + header.templateId()
              + ", version "
              + header.version()
              + ", block length "
              + header.blockLength());
    }
    body.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
    int port = body.port();
    final int raftPort = body.raftPort();
    long registeredAt = body.registeredAtEpochMillis();
    long lastRenewal = body.lastRenewalEpochMillis();
    final long renewalTimeoutMillis = body.renewalTimeoutMillis();
    List<BrokerPartitionState> partitions = new ArrayList<>();
    for (ClusterBrokerRecordDecoder.PartitionsDecoder group : body.partitions()) {
      if (group.actingBlockLength()
          != (header.version() == 9
              ? 41
              : ClusterBrokerRecordDecoder.PartitionsDecoder.sbeBlockLength())) {
        throw new IllegalArgumentException("Partition report layout does not match schema version");
      }
      int partitionId = group.partitionId();
      long term = group.term();
      partitions.add(
          new BrokerPartitionState(
              partitionId,
              new BrokerPartitionState.Status(
                  MemberRole.parse(group.role()),
                  term,
                  BrokerPartitionLifecycle.parse(group.lifecycleState()),
                  group.priority(),
                  group.recoveryEpoch()),
              new BrokerPartitionState.Replication(
                  group.matchIndex(), group.lag(), group.readyForPromotion() == 1),
              new BrokerPartitionState.Operation(
                  group.lastOpId(), BrokerTopologyOperationStatus.parse(group.lastOpStatus())),
              header.version() < 10
                  ? BrokerPartitionElectionState.UNKNOWN
                  : new BrokerPartitionElectionState(
                      new RaftConfigurationReference(
                          group.configurationIndex(), group.configurationDigest()),
                      new BrokerPartitionElectionState.Priorities(
                          group.desiredPriority(), group.targetPriority()),
                      new BrokerPartitionElectionState.Observation(
                          RaftNodeState.parse(group.nodeState()),
                          group.leaderId(),
                          group.serviceReady() == 1,
                          group.observedAtEpochMillis()))));
    }
    String clusterId = body.clusterId();
    String brokerId = body.brokerId();
    String host = body.host();
    String version = body.version();
    BrokerStatus status = BrokerStatus.valueOf(body.status());
    final String incarnationId = body.incarnationId();
    return new BrokerRegistration(
        new BrokerRegistration.Identity(clusterId, brokerId, version, incarnationId),
        new BrokerRegistration.Endpoint(host, port, raftPort),
        new BrokerRegistration.Lease(
            Instant.ofEpochMilli(registeredAt),
            Instant.ofEpochMilli(lastRenewal),
            renewalTimeoutMillis),
        status,
        partitions);
  }

  @FunctionalInterface
  private interface VarWriter {
    void put(byte[] src, int offset, int length);
  }

  private static void putVar(VarWriter writer, String value) {
    byte[] bytes = bytes(value);
    writer.put(bytes, 0, bytes.length);
  }

  private static byte[] bytes(String value) {
    return value == null || value.isEmpty() ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
  }
}
