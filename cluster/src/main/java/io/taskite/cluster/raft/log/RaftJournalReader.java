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
package io.taskite.cluster.raft.log;

/**
 * A forward-only sequential cursor over the raft log. The reader retains a logical position and
 * maps at most the segment currently being traversed, so streaming the complete log uses constant
 * memory regardless of segment count.
 *
 * <p>Readers are cheap to create and are not thread-safe; each consumer (e.g. the apply loop)
 * should own one. A reader tolerates concurrent truncation/compaction/reset of the journal: the
 * next {@link #hasNext()}/{@link #next()} transparently re-seeks to its logical position.
 */
public interface RaftJournalReader extends AutoCloseable {

  /** Positions the cursor so the next {@link #next()} returns the entry at {@code index}. */
  void seek(long index);

  /** Positions the cursor at the first (lowest) index still present in the log. */
  void seekToFirst();

  /** The index the next {@link #next()} call would return. */
  long nextIndex();

  /** True if an entry at {@link #nextIndex()} exists and is committed-or-present in the log. */
  boolean hasNext();

  /** Returns the entry at {@link #nextIndex()} and advances the cursor. */
  PersistedRaftRecord next();

  @Override
  void close();
}
