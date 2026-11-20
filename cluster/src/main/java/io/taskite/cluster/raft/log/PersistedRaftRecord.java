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

import java.util.Arrays;

/**
 * A raft log entry. The consensus layer reasons in {@code term}/{@code index}; the application log
 * positions it spans are {@code [lowestPosition, highestPosition]} (a single entry may carry a
 * batch of business records assigned contiguous positions by the sequencer). {@code data} is the
 * application's serialized bytes — one or more framed records — never interpreted by the raft
 * layer.
 *
 * <p>{@code lowestPosition}/{@code highestPosition} are carried as first-class fields so the
 * consensus/compaction layer can reason about replicated application positions without decoding the
 * payload. When a record predates the sequencer (or is constructed index-only), the span defaults
 * to the index, preserving the 1:1 position==index behaviour.
 */
public record PersistedRaftRecord(
    long term, long index, long lowestPosition, long highestPosition, byte[] data) {
  public PersistedRaftRecord {
    data = data == null ? new byte[0] : data;
  }

  /** Index-only constructor: the application position span defaults to the raft index. */
  public PersistedRaftRecord(final long term, final long index, final byte[] data) {
    this(term, index, index, index, data);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PersistedRaftRecord other)) {
      return false;
    }
    return term == other.term
        && index == other.index
        && lowestPosition == other.lowestPosition
        && highestPosition == other.highestPosition
        && Arrays.equals(data, other.data);
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(term);
    result = 31 * result + Long.hashCode(index);
    result = 31 * result + Long.hashCode(lowestPosition);
    result = 31 * result + Long.hashCode(highestPosition);
    result = 31 * result + Arrays.hashCode(data);
    return result;
  }

  @Override
  public String toString() {
    return "PersistedRaftRecord{term="
        + term
        + ", index="
        + index
        + ", lowestPosition="
        + lowestPosition
        + ", highestPosition="
        + highestPosition
        + ", dataLength="
        + data.length
        + '}';
  }
}
