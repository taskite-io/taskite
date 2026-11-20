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
import io.taskite.placementdriver.broker.cluster.ClusterChangeOperation;
import io.taskite.placementdriver.common.codec.ClusterChangeOperationCodec;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/** Owns the persistent encoding of cluster change operations. */
public final class ClusterChangeOperationRecordValue implements DbValue {
  private ClusterChangeOperation record;
  private byte[] bytes = new byte[0];

  public ClusterChangeOperation getRecord() {
    return record;
  }

  public void setRecord(final ClusterChangeOperation record) {
    bytes = ClusterChangeOperationCodec.toBytes(record);
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
    final var decoded = ClusterChangeOperationCodec.fromBytes(encoded);
    bytes = encoded;
    record = decoded;
  }
}
