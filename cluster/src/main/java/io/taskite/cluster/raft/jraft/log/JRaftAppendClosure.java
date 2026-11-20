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
package io.taskite.cluster.raft.jraft.log;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import io.taskite.cluster.raft.log.PersistedRaftRecord;
import io.taskite.scheduler.future.CompletableActorFuture;

/**
 * The jraft done-closure for a leader-side {@code append} (migration decision D1). jraft runs it —
 * via {@code onApply}'s {@code it.done()} — exactly once the entry is committed and applied on the
 * leader. The FSM first {@link #stamp(long, long) stamps} the jraft-assigned {@code term}/{@code
 * index} onto it, so the append future resolves to the same fully-populated {@link
 * PersistedRaftRecord} the in-house {@code RaftPartition.append} contract returns (façade parity),
 * rather than only signalling success.
 *
 * <p>The caller-known {@code [lowestPosition, highestPosition]} span and payload are captured at
 * submit time; only the raft coordinates are unknown until apply, hence {@link #stamp}.
 */
public final class JRaftAppendClosure implements Closure {
  private final long lowestPosition;
  private final long highestPosition;
  private final byte[] payload;
  private final CompletableActorFuture<PersistedRaftRecord> future;
  private long term;
  private long index;

  public JRaftAppendClosure(
      final long lowestPosition,
      final long highestPosition,
      final byte[] payload,
      final CompletableActorFuture<PersistedRaftRecord> future) {
    this.lowestPosition = lowestPosition;
    this.highestPosition = highestPosition;
    this.payload = payload;
    this.future = future;
  }

  /**
   * Records the jraft-assigned coordinates of the committed entry. Called from {@code onApply} on
   * the FSM thread immediately before {@link #run(Status)}, so no cross-thread publication is
   * needed.
   */
  public void stamp(final long term, final long index) {
    this.term = term;
    this.index = index;
  }

  @Override
  public void run(final Status status) {
    if (status.isOk()) {
      future.complete(
          new PersistedRaftRecord(term, index, lowestPosition, highestPosition, payload));
    } else {
      future.completeExceptionally(new IllegalStateException("append failed: " + status));
    }
  }
}
