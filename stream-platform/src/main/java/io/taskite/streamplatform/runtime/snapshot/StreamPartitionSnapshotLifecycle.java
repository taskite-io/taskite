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

import io.taskite.scheduler.future.ActorFuture;
import io.taskite.snapshot.SnapshotLoadLifecycle;
import java.util.Objects;
import java.util.function.Supplier;

/** Adapts snapshot load callbacks to stream partition role transitions. */
public final class StreamPartitionSnapshotLifecycle implements SnapshotLoadLifecycle {
  private final Supplier<ActorFuture<Void>> beforeLoad;
  private final Supplier<ActorFuture<Void>> afterLoad;

  public StreamPartitionSnapshotLifecycle(
      final Supplier<ActorFuture<Void>> beforeLoad, final Supplier<ActorFuture<Void>> afterLoad) {
    this.beforeLoad = Objects.requireNonNull(beforeLoad, "beforeLoad");
    this.afterLoad = Objects.requireNonNull(afterLoad, "afterLoad");
  }

  @Override
  public ActorFuture<Void> beforeLoad() {
    return beforeLoad.get();
  }

  @Override
  public ActorFuture<Void> afterLoad() {
    return afterLoad.get();
  }
}
