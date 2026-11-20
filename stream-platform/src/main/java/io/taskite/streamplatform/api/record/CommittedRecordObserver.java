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
package io.taskite.streamplatform.api.record;

import io.taskite.streamplatform.logstream.LoggedEvent;

/** Observes committed records after their durable processing or replay has completed. */
public interface CommittedRecordObserver {
  void onProcessed(LoggedEvent record);

  void onFailure(LoggedEvent record, Throwable error);

  static CommittedRecordObserver noop() {
    return new CommittedRecordObserver() {
      @Override
      public void onProcessed(final LoggedEvent record) {}

      @Override
      public void onFailure(final LoggedEvent record, final Throwable error) {}
    };
  }
}
