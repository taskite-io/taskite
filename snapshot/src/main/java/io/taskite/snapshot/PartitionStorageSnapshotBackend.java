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
import io.taskite.state.PartitionStorage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/** RocksDB Checkpoint implementation of the engine-neutral snapshot state backend. */
public final class PartitionStorageSnapshotBackend implements SnapshotStateBackend {
  private final PartitionStorage storage;
  private final SnapshotLifecycleListener listener;
  private final SnapshotLoadLifecycle loadLifecycle;

  public PartitionStorageSnapshotBackend(final PartitionStorage storage) {
    this(storage, SnapshotLifecycleListener.noop(), SnapshotLoadLifecycle.noop());
  }

  public PartitionStorageSnapshotBackend(
      final PartitionStorage storage, final SnapshotLifecycleListener listener) {
    this(storage, listener, SnapshotLoadLifecycle.noop());
  }

  public PartitionStorageSnapshotBackend(
      final PartitionStorage storage,
      final SnapshotLifecycleListener listener,
      final SnapshotLoadLifecycle loadLifecycle) {
    this.storage = Objects.requireNonNull(storage, "storage");
    this.listener = Objects.requireNonNull(listener, "listener");
    this.loadLifecycle = Objects.requireNonNull(loadLifecycle, "loadLifecycle");
  }

  @Override
  public void save(final Path snapshotDir) {
    try {
      final Path parent = snapshotDir.toAbsolutePath().getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }
    } catch (final IOException e) {
      throw new IllegalStateException("failed to prepare snapshot dir " + snapshotDir, e);
    }
    storage.createCheckpoint(snapshotDir);
  }

  @Override
  public void load(final Path snapshotDir) {
    storage.importFromCheckpoint(snapshotDir);
  }

  @Override
  public void onSaveCompleted(
      final Path snapshotDir, final long snapshotIndex, final long snapshotTerm) {
    listener.onSnapshotSaved(snapshotDir, snapshotIndex, snapshotTerm);
  }

  @Override
  public ActorFuture<Void> beforeLoad() {
    return loadLifecycle.beforeLoad();
  }

  @Override
  public void onLoadCompleted(
      final Path snapshotDir, final long snapshotIndex, final long snapshotTerm) {
    listener.onSnapshotLoaded(snapshotDir, snapshotIndex, snapshotTerm);
  }

  @Override
  public ActorFuture<Void> afterLoad() {
    return loadLifecycle.afterLoad();
  }
}
