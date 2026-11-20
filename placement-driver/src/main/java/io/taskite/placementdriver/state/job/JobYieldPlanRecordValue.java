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
package io.taskite.placementdriver.state.job;

import io.taskite.db.DbValue;
import io.taskite.protocol.impl.encoding.MsgPackConverter;
import io.taskite.protocol.impl.record.value.placementdriver.job.JobYieldPlan;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/** Persists complete yield plans as MessagePack. */
public final class JobYieldPlanRecordValue implements DbValue {
  private JobYieldPlan record;
  private byte[] bytes = new byte[0];

  public JobYieldPlan getRecord() {
    return record;
  }

  public void setRecord(final JobYieldPlan record) {
    bytes = MsgPackConverter.convertToMsgPack(record);
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
    final var decoded = MsgPackConverter.convertToObject(JobYieldPlan.class, encoded);
    bytes = encoded;
    record = decoded;
  }
}
