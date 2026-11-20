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
package io.taskite.snapshot;

import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.nio.file.Path;

/** Saves and restores a partition's business state through a self-contained directory. */
public interface SnapshotStateBackend {

  void save(Path snapshotDir);

  void load(Path snapshotDir);

  default void onSaveCompleted(
      final Path snapshotDir, final long snapshotIndex, final long snapshotTerm) {}

  default ActorFuture<Void> beforeLoad() {
    return CompletableActorFuture.completed(null);
  }

  default void onLoadCompleted(
      final Path snapshotDir, final long snapshotIndex, final long snapshotTerm) {}

  default ActorFuture<Void> afterLoad() {
    return CompletableActorFuture.completed(null);
  }
}
