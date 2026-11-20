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
package io.taskite.placementdriver.state.immutable;

import io.taskite.protocol.impl.record.value.placementdriver.job.JobYieldSyncRecord;
import java.util.List;
import java.util.Optional;

public interface JobYieldSyncState {
  Optional<JobYieldSyncRecord> get(String clusterId, int partitionId);

  List<JobYieldSyncRecord> dueBefore(long now, int limit);

  int reservedTargets(long now, int limit);

  long nextDueAt();
}
