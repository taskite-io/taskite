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
package io.taskite.streamplatform.processing;

import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.streamplatform.api.metrics.ProcessingMetrics;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.record.CommandResponseObserver;
import io.taskite.streamplatform.api.record.CommittedRecordObserver;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.command.CommandFailureObserver;
import io.taskite.streamplatform.logstream.LogStreamReader;
import java.util.Objects;

/**
 * Resolved collaborators shared by replay and command processing within one processor generation.
 */
record StreamStateMachineContext(
    Identity identity,
    Processing processing,
    StreamProcessorContext.State state,
    Runtime runtime,
    Observers observers) {
  StreamStateMachineContext {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(processing, "processing");
    Objects.requireNonNull(state, "state");
    Objects.requireNonNull(runtime, "runtime");
    Objects.requireNonNull(observers, "observers");
  }

  record Identity(int partitionId, StreamProcessorMode mode) {}

  record Processing(
      TypedEventRegistry eventRegistry, TypedRecordProcessorContext processorContext) {}

  record Runtime(ConcurrencyControl actor, LogStreamReader reader) {}

  record Observers(
      CommandFailureObserver failureObserver,
      CommandResponseObserver commandResponseObserver,
      CommittedRecordObserver committedRecordObserver,
      ProcessingMetrics processingMetrics) {}
}
