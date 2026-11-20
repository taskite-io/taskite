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
package io.taskite.placementdriver.processing.worker.registry;

import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.WorkerIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;
import java.time.Duration;

public final class WorkerProcessors {
  private WorkerProcessors() {}

  public static TypedRecordProcessors register(
      final TypedRecordProcessors processors,
      final TypedRecordProcessorContext context,
      final Duration leaseTimeout,
      final Duration offlineRetention) {
    return processors
        .onCommand(
            ValueType.WORKER_REGISTRATION,
            WorkerIntent.REGISTER,
            new WorkerRegisterProcessor(context, leaseTimeout))
        .onCommand(
            ValueType.WORKER_REGISTRATION,
            WorkerIntent.RENEW,
            new WorkerRenewProcessor(context, leaseTimeout))
        .onCommand(
            ValueType.WORKER_REGISTRATION,
            WorkerIntent.DEREGISTER,
            new WorkerDeregisterProcessor(context))
        .onCommand(
            ValueType.WORKER_REGISTRATION,
            WorkerIntent.MARK_OFFLINE,
            new WorkerMarkOfflineProcessor(context, leaseTimeout))
        .onCommand(
            ValueType.WORKER_REGISTRATION,
            WorkerIntent.DELETE,
            new WorkerDeleteProcessor(context, offlineRetention));
  }
}
