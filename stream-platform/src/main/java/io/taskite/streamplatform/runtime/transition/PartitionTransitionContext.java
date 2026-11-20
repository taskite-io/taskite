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
package io.taskite.streamplatform.runtime.transition;

import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.snapshot.SnapshotDirector;
import io.taskite.streamplatform.logstream.LogStream;
import io.taskite.streamplatform.processing.StreamProcessor;

/** Mutable context shared by partition transition steps. */
public final class PartitionTransitionContext {

  private final int partitionId;
  private final LogStream logStream;

  private ConcurrencyControl concurrencyControl;
  private volatile StreamProcessor streamProcessor;
  private volatile SnapshotDirector snapshotDirector;
  private long currentTerm;
  private PartitionRole currentRole = PartitionRole.INACTIVE;

  public PartitionTransitionContext(final int partitionId, final LogStream logStream) {
    this.partitionId = partitionId;
    this.logStream = logStream;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public LogStream getLogStream() {
    return logStream;
  }

  public ConcurrencyControl getConcurrencyControl() {
    return concurrencyControl;
  }

  public void setConcurrencyControl(final ConcurrencyControl concurrencyControl) {
    this.concurrencyControl = concurrencyControl;
  }

  public StreamProcessor getStreamProcessor() {
    return streamProcessor;
  }

  public void setStreamProcessor(final StreamProcessor streamProcessor) {
    this.streamProcessor = streamProcessor;
  }

  public SnapshotDirector getSnapshotDirector() {
    return snapshotDirector;
  }

  public void setSnapshotDirector(final SnapshotDirector snapshotDirector) {
    this.snapshotDirector = snapshotDirector;
  }

  public long getCurrentTerm() {
    return currentTerm;
  }

  public void setCurrentTerm(final long term) {
    this.currentTerm = term;
  }

  public PartitionRole getCurrentRole() {
    return currentRole;
  }

  public void setCurrentRole(final PartitionRole role) {
    this.currentRole = role;
  }
}
