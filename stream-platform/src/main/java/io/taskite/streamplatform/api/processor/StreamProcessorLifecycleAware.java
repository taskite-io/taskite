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
package io.taskite.streamplatform.api.processor;

import io.taskite.protocol.record.TypedRecord;

/** Lifecycle of one processor generation. Followers never receive {@link #onRecovered}. */
public interface StreamProcessorLifecycleAware {
  /** Runs in a transaction after leader replay, allowing initialization of derived indexes. */
  void onRecovered(ProcessingScheduleService scheduling);

  /** Runs after the command's state transaction commits. */
  default void onProcessed(TypedRecord<?> command) {}

  /** Invoked on close or failure; implementations must be idempotent. */
  default void onClose() {}
}
