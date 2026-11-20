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

import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import org.agrona.concurrent.UnsafeBuffer;

/** Uses the same structured SBE representation for event logs and durable plans. */
public final class ChangePlanRecordCodec {
  private ChangePlanRecordCodec() {}

  public static byte[] toBytes(final ChangePlan plan) {
    final var record = BrokerPartitionChangeRecordMapper.fromPlan(plan);
    final byte[] bytes = new byte[record.getLength()];
    record.write(new UnsafeBuffer(bytes), 0);
    return bytes;
  }

  public static ChangePlan fromBytes(final byte[] encoded) {
    final var record = new PartitionChangeRecord();
    record.wrap(new UnsafeBuffer(encoded), 0, encoded.length);
    return BrokerPartitionChangeRecordMapper.toPlan(
        record, BrokerPartitionChangeStatus.valueOf(record.getStatus().name()));
  }
}
