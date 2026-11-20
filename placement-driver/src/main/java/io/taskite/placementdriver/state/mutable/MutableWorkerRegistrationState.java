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
package io.taskite.placementdriver.state.mutable;

import io.taskite.placementdriver.state.immutable.WorkerRegistrationState;
import io.taskite.placementdriver.worker.registry.WorkerRegistryKey;
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import java.time.Instant;

public interface MutableWorkerRegistrationState extends WorkerRegistrationState {
  void registerWorker(WorkerRegistrationRecord registration, String tenantId, Instant now);

  void renewWorker(WorkerRegistrationRecord renewal, String tenantId, Instant now);

  void markWorkerOffline(WorkerRegistryKey key, Instant now);

  void removeWorker(WorkerRegistryKey key);

  void deleteCluster(String clusterId);
}
