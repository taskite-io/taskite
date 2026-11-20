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
package io.taskite.streamplatform.command;

import io.taskite.scheduler.future.ActorFuture;
import io.taskite.streamplatform.logstream.LogAppendEntry;
import java.util.List;

@FunctionalInterface
public interface RecordAppender {
  ActorFuture<Long> append(List<LogAppendEntry> entries, long sourcePosition);

  default ActorFuture<Long> append(final LogAppendEntry entry, final long sourcePosition) {
    return append(List.of(entry), sourcePosition);
  }

  default ActorFuture<Long> append(final LogAppendEntry entry) {
    return append(entry, -1L);
  }

  default ActorFuture<Long> append(final List<LogAppendEntry> entries) {
    return append(entries, -1L);
  }
}
