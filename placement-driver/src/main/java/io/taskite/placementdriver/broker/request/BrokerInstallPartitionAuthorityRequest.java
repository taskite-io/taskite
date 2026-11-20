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
package io.taskite.placementdriver.broker.request;

import io.taskite.broker.client.request.BrokerAdminRequest;
import io.taskite.protocol.impl.record.value.broker.BrokerPartitionConfigurationRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.BrokerPartitionConfigurationIntent;
import org.agrona.DirectBuffer;

public final class BrokerInstallPartitionAuthorityRequest
    extends BrokerAdminRequest<BrokerPartitionConfigurationRecord> {
  private final BrokerPartitionConfigurationRecord value;

  public BrokerInstallPartitionAuthorityRequest(
      final String clusterId,
      final int partitionId,
      final long expectedGeneration,
      final long generation,
      final String ownerChangeId,
      final long expectedRecoveryEpoch) {
    super(
        ValueType.BROKER_PARTITION_CONFIGURATION,
        BrokerPartitionConfigurationIntent.INSTALL_AUTHORITY);
    if (expectedGeneration < 0
        || generation <= expectedGeneration
        || ownerChangeId == null
        || ownerChangeId.isBlank()
        || expectedRecoveryEpoch < 0) {
      throw new IllegalArgumentException("invalid execution authority");
    }
    setClusterId(clusterId);
    setPartitionId(partitionId);
    value =
        new BrokerPartitionConfigurationRecord()
            .setClusterId(clusterId)
            .setPartitionId(partitionId)
            .setExpectedGeneration(expectedGeneration)
            .setGeneration(generation)
            .setOwnerChangeId(ownerChangeId)
            .setExpectedRecoveryEpoch(expectedRecoveryEpoch);
  }

  @Override
  public BrokerPartitionConfigurationRecord getRequestWriter() {
    return value;
  }

  @Override
  protected BrokerPartitionConfigurationRecord toResponseDto(final DirectBuffer buffer) {
    final var response = new BrokerPartitionConfigurationRecord();
    response.wrap(buffer);
    if (!getClusterId().equals(response.getClusterId())
        || getPartitionId() != response.getPartitionId()) {
      throw new IllegalArgumentException("configuration response identity mismatch");
    }
    return response;
  }

  public String clusterId() {
    return getClusterId();
  }

  public int partitionId() {
    return getPartitionId();
  }

  public long expectedGeneration() {
    return value.getExpectedGeneration();
  }

  public long generation() {
    return value.getGeneration();
  }

  public String ownerChangeId() {
    return value.getOwnerChangeId();
  }

  public long expectedRecoveryEpoch() {
    return value.getExpectedRecoveryEpoch();
  }
}
