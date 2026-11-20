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
package io.taskite.engine.state.immutable;

import io.taskite.protocol.impl.record.value.broker.JobRecord;
import java.util.List;
import java.util.function.BiFunction;
import org.agrona.DirectBuffer;

/** Read-only access to persisted job records, lifecycle state, and activation indexes. */
public interface JobState {

  /** Returns the stored job record for {@code jobKey}, or {@code null} if absent. */
  JobRecord getJob(long jobKey);

  /** Returns the lifecycle state of {@code jobKey}, or {@link State#NOT_FOUND} if absent. */
  State getState(long jobKey);

  /** Returns whether a job with {@code jobKey} exists. */
  boolean exists(long jobKey);

  /** At most {@code limit} activated jobs, ordered by deadline and key. */
  List<DueJob> findTimedOutJobs(long timestamp, int limit);

  /** At most {@code limit} failed jobs with remaining retries and elapsed backoff. */
  List<DueJob> findBackedOffJobs(long timestamp, int limit);

  /** Earliest persisted activated-job timeout, or {@link Long#MAX_VALUE} when empty. */
  long getNextTimeoutDueDate();

  /** Earliest persisted failed-job backoff, or {@link Long#MAX_VALUE} when empty. */
  long getNextBackoffDueDate();

  /** Bounded persistent ownership-index lookup; records are copied before returning. */
  List<Long> findActivatedByWorker(String tenantId, String workerId, long token, int limit);

  record DueJob(long key, long dueDate, String tenantId, String workerId, long workerLeaseToken) {
    public DueJob(long key, long dueDate, String tenantId) {
      this(key, dueDate, tenantId, "", 0);
    }
  }

  boolean workerIndexReady();

  /**
   * Visits activatable jobs of the given type for the given tenants, in key order. The callback
   * returns {@code true} to continue iterating or {@code false} to stop.
   */
  void forEachActivatableJobs(
      DirectBuffer type, List<String> tenantIds, BiFunction<Long, JobRecord, Boolean> callback);

  /**
   * Visits persisted jobs belonging to the given subscription using its index, regardless of
   * lifecycle state, in job key order. Records are copied before invoking the callback, which may
   * read or remove jobs and returns {@code true} to continue or {@code false} to stop.
   */
  void forEachJobs(long subscriptionKey, BiFunction<Long, JobRecord, Boolean> callback);

  /** The lifecycle state of a job. Persisted by ordinal, so constants must not be reordered. */
  enum State {
    ACTIVATABLE,
    ACTIVATED,
    FAILED,
    ERROR_THROWN,
    NOT_FOUND
  }
}
