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
 * Reads committed entries in application-position order. The {@code StreamProcessor} operates only
 * on application log positions; raft indexes, segments, and byte offsets remain internal to the
 * journal layer.
 *
 * <pre>{@code
 * reader.seekToNextEvent(lastProcessedPosition);
 * while (reader.hasNext()) {
 *   final LoggedEvent entry = reader.next();
 *   // apply entry...
 * }
 * }</pre>
 */
public interface LogStreamReader extends AutoCloseable {

  /**
   * Positions the reader so the next {@link #next()} returns the first entry <em>after</em> {@code
   * position}. A negative position seeks to the first entry.
   */
  void seekToNextEvent(long position);

  /** Positions the reader so the next {@link #next()} returns the entry at {@code position}. */
  void seek(long position);

  /** The position the next {@link #next()} call would return. */
  long getNextPosition();

  /** Highest application position already covered by the restored business checkpoint. */
  default long snapshotPosition() {
    return 0L;
  }

  /** True if another committed entry is available at {@link #getNextPosition()}. */
  boolean hasNext();

  /** Returns the entry at {@link #getNextPosition()} and advances the reader. */
  LoggedEvent next();

  @Override
  void close();
}
