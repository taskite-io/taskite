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
package io.taskite.streamplatform.api.processor;

import io.taskite.streamplatform.api.state.EventAppliers;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.Writers;

public interface TypedRecordProcessorContext {
  int getPartitionId();

  KeyGenerator getKeyGenerator();

  Writers getWriters();

  /** Returns the committed timestamp of the root command for the current processing batch. */
  default long getProcessingTime() {
    throw new IllegalStateException("Processing time is not available");
  }

  /** Sets the fixed processing time before executing a root command and its admitted follow-ups. */
  default void setProcessingTime(final long processingTime) {
    throw new UnsupportedOperationException("Processing time is not supported");
  }

  Object getProcessingState();

  EventAppliers getEventAppliers();

  default <T> T getProcessingState(final Class<T> type) {
    return type.cast(getProcessingState());
  }
}
