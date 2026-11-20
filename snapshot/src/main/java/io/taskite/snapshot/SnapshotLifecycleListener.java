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

import java.nio.file.Path;

public interface SnapshotLifecycleListener {

  void onSnapshotSaved(Path checkpointDirectory, long snapshotIndex, long snapshotTerm);

  void onSnapshotLoaded(Path checkpointDirectory, long snapshotIndex, long snapshotTerm);

  static SnapshotLifecycleListener noop() {
    return new SnapshotLifecycleListener() {
      @Override
      public void onSnapshotSaved(
          final Path checkpointDirectory, final long snapshotIndex, final long snapshotTerm) {}

      @Override
      public void onSnapshotLoaded(
          final Path checkpointDirectory, final long snapshotIndex, final long snapshotTerm) {}
    };
  }
}
