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
package io.taskite.protocol.record;

import io.taskite.protocol.record.intent.Intent;

public interface TypedRecord<T extends UnifiedRecordValue> extends Record<T> {

  @Override
  long getKey();

  @Override
  T getValue();

  String getRequestStreamId();

  String getRequestId();

  int getLength();

  @Override
  default long getPosition() {
    return 0;
  }

  @Override
  default long getTimestamp() {
    return 0;
  }

  @Override
  default Intent getIntent() {
    return null;
  }

  @Override
  default int getPartitionId() {
    return 0;
  }

  @Override
  default RecordType getRecordType() {
    return null;
  }

  @Override
  default RejectionType getRejectionType() {
    return null;
  }

  @Override
  default String getRejectionReason() {
    return "";
  }

  @Override
  default int getRecordVersion() {
    return 0;
  }

  @Override
  default ValueType getValueType() {
    return null;
  }
}
