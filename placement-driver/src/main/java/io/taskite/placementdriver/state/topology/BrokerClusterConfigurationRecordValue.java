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
package io.taskite.placementdriver.state.topology;

import io.taskite.db.DbValue;
import io.taskite.placementdriver.common.codec.BrokerClusterConfigurationRecordCodec;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/** Owns the persistent encoding of cluster configuration. */
public final class BrokerClusterConfigurationRecordValue implements DbValue {
  private BrokerClusterConfig record;
  private byte[] bytes = new byte[0];

  public BrokerClusterConfig getRecord() {
    return record;
  }

  public void setRecord(final String clusterId, final BrokerClusterConfig record) {
    bytes =
        new BrokerClusterConfigurationRecordCodec()
            .setClusterId(clusterId)
            .setClusterSize(record.clusterSize())
            .setPartitionSize(record.partitionSize())
            .setReplicationFactor(record.replicationFactor())
            .setVoterRoster(record.voterRoster())
            .setRosterEpoch(record.rosterEpoch())
            .toBytes();
    this.record = record;
  }

  @Override
  public int getLength() {
    return bytes.length;
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    buffer.putBytes(offset, bytes);
  }

  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    final byte[] encoded = new byte[length];
    buffer.getBytes(offset, encoded);
    final var decoded = new BrokerClusterConfigurationRecordCodec().wrap(encoded);
    record =
        new BrokerClusterConfig(
            decoded.getClusterSize(),
            decoded.getPartitionSize(),
            decoded.getReplicationFactor(),
            decoded.getVoterRoster(),
            decoded.getRosterEpoch());
    bytes = encoded;
  }
}
