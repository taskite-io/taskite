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
package io.taskite.streamplatform.runtime.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.taskite.scheduler.future.CompletableActorFuture;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

final class StreamPartitionSnapshotLifecycleTest {
  @Test
  void bracketsSnapshotLoadWithInactiveAndRoleRestore() {
    final List<String> events = new ArrayList<>();
    final StreamPartitionSnapshotLifecycle lifecycle =
        new StreamPartitionSnapshotLifecycle(
            () -> {
              events.add("inactive");
              return CompletableActorFuture.completed(null);
            },
            () -> {
              events.add("restore");
              return CompletableActorFuture.completed(null);
            });

    lifecycle.beforeLoad().join();
    events.add("import");
    lifecycle.afterLoad().join();

    assertEquals(List.of("inactive", "import", "restore"), events);
  }
}
