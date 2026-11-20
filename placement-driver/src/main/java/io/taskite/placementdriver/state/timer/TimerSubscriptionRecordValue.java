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
package io.taskite.placementdriver.state.timer;

import io.taskite.db.DbValue;
import io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/** Persists timers using the record's native encoding. */
public final class TimerSubscriptionRecordValue implements DbValue {
  private TimerSubscriptionRecord record = new TimerSubscriptionRecord();

  public TimerSubscriptionRecord getRecord() {
    return record;
  }

  public void setRecord(final TimerSubscriptionRecord record) {
    this.record = record;
  }

  @Override
  public int getLength() {
    return record.getLength();
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    record.write(buffer, offset);
  }

  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    final var decoded = new TimerSubscriptionRecord();
    decoded.wrap(buffer, offset, length);
    record = decoded;
  }
}
