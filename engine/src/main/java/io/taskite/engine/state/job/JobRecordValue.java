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
package io.taskite.engine.state.job;

import io.taskite.db.DbValue;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * A {@link DbValue} wrapper that stores a {@link JobRecord} in its native serialized form. This
 * class defines no additional encoding: {@link #getLength()}, {@link #write}, and {@link #wrap}
 * delegate directly to the wrapped record.
 */
public final class JobRecordValue implements DbValue {

  private JobRecord record = new JobRecord();

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
    record.wrap(buffer, offset, length);
  }

  public JobRecord getRecord() {
    return record;
  }

  public void setRecord(final JobRecord record) {
    this.record = record;
  }

  @Override
  public String toString() {
    return record.toJson();
  }
}
