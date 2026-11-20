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
import java.util.Objects;

public final class TypedRecordProcessorContextImpl implements TypedRecordProcessorContext {
  private final int partitionId;
  private final KeyGenerator keyGenerator;
  private final Writers writers;
  private final Object processingState;
  private final EventAppliers eventAppliers;
  private long processingTime;
  private boolean processingTimeSet;

  public TypedRecordProcessorContextImpl(
      final int partitionId,
      final KeyGenerator keyGenerator,
      final Writers writers,
      final Object processingState,
      final EventAppliers eventAppliers) {
    this.partitionId = partitionId;
    this.keyGenerator = Objects.requireNonNull(keyGenerator, "keyGenerator");
    this.writers = Objects.requireNonNull(writers, "writers");
    this.processingState = Objects.requireNonNull(processingState, "processingState");
    this.eventAppliers = Objects.requireNonNull(eventAppliers, "eventAppliers");
  }

  @Override
  public int getPartitionId() {
    return partitionId;
  }

  @Override
  public KeyGenerator getKeyGenerator() {
    return keyGenerator;
  }

  @Override
  public Writers getWriters() {
    return writers;
  }

  @Override
  public long getProcessingTime() {
    if (!processingTimeSet) {
      throw new IllegalStateException(
          "Processing time is not available outside command processing");
    }
    return processingTime;
  }

  @Override
  public void setProcessingTime(final long processingTime) {
    this.processingTime = processingTime;
    processingTimeSet = true;
  }

  @Override
  public Object getProcessingState() {
    return processingState;
  }

  @Override
  public EventAppliers getEventAppliers() {
    return eventAppliers;
  }
}
