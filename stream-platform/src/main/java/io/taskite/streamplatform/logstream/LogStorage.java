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

/**
 * Replicated append/read abstraction for the durable, consensus-backed business log. It is distinct
 * from the engine's derived {@code ProcessingStateStore}. A block written by {@link #append} is an
 * opaque serialized batch spanning {@code [lowestPosition, highestPosition]} and is stored
 * atomically.
 *
 * <p>Implementations wrap the raft partition: an append routes through raft replication, and commit
 * notifications are surfaced through {@link CommitListener}. Reads are sequential by position via
 * {@link #newReader()}. The storage never interprets the payload bytes.
 */
interface LogStorage {

  /** Opens a new sequential reader over committed entries, addressed by position. */
  LogStreamReader newReader();

  /**
   * Appends an opaque batch payload spanning {@code [lowestPosition, highestPosition]}. The write
   * is atomic: either the whole block is durable or none of it. The {@link AppendListener} is
   * notified of write/commit progress.
   */
  void append(
      long lowestPosition, long highestPosition, byte[] batchPayload, AppendListener listener);

  /** Registers a listener notified when new records are committed (by any writer). */
  void addCommitListener(CommitListener listener);

  /** Removes a previously registered commit listener. */
  void removeCommitListener(CommitListener listener);

  /** The highest committed position, or a non-positive value if nothing is committed yet. */
  long getCommittedPosition();

  /**
   * Notifies an appender of the lifecycle of a single append: written to local storage, then
   * committed (replicated to a quorum), or failed.
   */
  interface AppendListener {
    default void onWrite(long position) {}

    default void onWriteError(Throwable error) {}

    default void onCommit(long position) {}

    default void onCommitError(long position, Throwable error) {}
  }

  /**
   * Notifies any consumer (not just the writer) that new records have been committed, so a reader
   * can be drained forward.
   */
  interface CommitListener {
    void onCommit();
  }
}
