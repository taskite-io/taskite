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
package io.taskite.streamplatform.logstream;

import io.taskite.cluster.raft.RaftPartition;
import io.taskite.scheduler.ConcurrencyControl;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * {@link LogStorage} backed by a {@link RaftPartition}: appends route through raft replication and
 * commit notifications are surfaced to {@link CommitListener}s. This is the seam between the
 * logstream package and the consensus layer; the StreamProcessor above never reaches into Raft. The
 * payload bytes (an already-serialized batch from the {@link Sequencer}) are opaque here.
 */
final class RaftLogStorage implements LogStorage {
  private final RaftPartition raftPartition;
  private final ConcurrencyControl appendControl;
  private final CopyOnWriteArrayList<CommitListener> commitListeners = new CopyOnWriteArrayList<>();

  RaftLogStorage(final RaftPartition raftPartition, final ConcurrencyControl appendControl) {
    this.raftPartition = raftPartition;
    this.appendControl = appendControl;
    // Bridge raft commit notifications to logstream commit listeners.
    raftPartition.subscribe(record -> notifyCommitListeners());
  }

  @Override
  public LogStreamReader newReader() {
    return new RaftLogStreamReader(raftPartition);
  }

  @Override
  public void append(
      final long lowestPosition,
      final long highestPosition,
      final byte[] batchPayload,
      final AppendListener listener) {
    appendControl.runOnCompletion(
        raftPartition.append(lowestPosition, highestPosition, batchPayload),
        (persisted, error) -> {
          if (error != null) {
            listener.onWriteError(error);
            return;
          }
          listener.onWrite(highestPosition);
          // raft.append only returns after replication to a majority, so the entry is committed.
          listener.onCommit(highestPosition);
        });
  }

  @Override
  public void addCommitListener(final CommitListener listener) {
    commitListeners.add(listener);
  }

  @Override
  public void removeCommitListener(final CommitListener listener) {
    commitListeners.remove(listener);
  }

  @Override
  public long getCommittedPosition() {
    return raftPartition.committedPosition();
  }

  private void notifyCommitListeners() {
    for (final CommitListener listener : commitListeners) {
      listener.onCommit();
    }
  }
}
