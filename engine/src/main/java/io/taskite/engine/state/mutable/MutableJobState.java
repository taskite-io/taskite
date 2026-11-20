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
package io.taskite.engine.state.mutable;

import io.taskite.engine.state.immutable.JobState;
import io.taskite.protocol.impl.record.value.broker.JobRecord;

/** Transactional write access to job records, lifecycle state, and activation indexes. */
public interface MutableJobState extends JobState {

  /** Creates a new, activatable job. */
  void create(long key, JobRecord record);

  /** Activates a job (single-step upsert of record + state), making it non-activatable. */
  void activate(long key, JobRecord record);

  /** Completes a job, removing it and its indexes. */
  void complete(long key, JobRecord record);

  /** Persists a job and maintains its activation index for the supplied lifecycle state. */
  void update(long key, JobRecord record, State state);

  /** Advances the durable ownership-index migration in a bounded event transaction. */
  void migrateWorkerIndex(int limit);
}
