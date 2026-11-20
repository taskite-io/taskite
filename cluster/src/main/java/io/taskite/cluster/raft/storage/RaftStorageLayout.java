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
package io.taskite.cluster.raft.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Defines the flat on-disk layout for one raft partition. Given the partition base directory, it
 * names the four sibling directories used by the raft node:
 *
 * <pre>
 * &lt;partition-dir&gt;/
 * ├── runtime/     RocksDB business store (owned by the upper layer, not by this layout)
 * ├── raft-log/    jraft log storage        (NodeOptions.logUri)
 * ├── raft-meta/   jraft raft metadata      (NodeOptions.raftMetaUri)
 * ├── snapshots/   jraft committed snapshots (NodeOptions.snapshotUri) — the ONLY snapshot dir
 * └── pending/     jraft in-progress snapshot temp (NodeOptions.snapshotTempUri)
 * </pre>
 *
 * <p>This replaces the previous wrapping {@code &lt;partition-dir&gt;/jraft/{log,meta,snapshot}}
 * layout, and hoists the snapshot temp dir to a top-level {@code pending/} sibling (jraft's {@code
 * LocalSnapshotStorage} honours a {@code snapshotTempUri} as long as it is not nested under the
 * snapshot uri). The business {@code runtime/} directory is resolved here for reference but is
 * created and owned by {@code PartitionStorage}, not by {@link #create()}.
 */
public record RaftStorageLayout(
    Path partitionDirectory, Path logDir, Path metaDir, Path snapshotDir, Path tempDir) {

  public static final String RUNTIME_DIR = "runtime";
  public static final String LOG_DIR = "raft-log";
  public static final String META_DIR = "raft-meta";
  public static final String SNAPSHOTS_DIR = "snapshots";
  public static final String PENDING_DIR = "pending";

  public RaftStorageLayout {
    Objects.requireNonNull(partitionDirectory, "partitionDirectory");
    Objects.requireNonNull(logDir, "logDir");
    Objects.requireNonNull(metaDir, "metaDir");
    Objects.requireNonNull(snapshotDir, "snapshotDir");
    Objects.requireNonNull(tempDir, "tempDir");
  }

  /** Derives the flat layout for a partition rooted at {@code partitionDirectory}. */
  public static RaftStorageLayout of(final Path partitionDirectory) {
    return new RaftStorageLayout(
        partitionDirectory,
        partitionDirectory.resolve(LOG_DIR),
        partitionDirectory.resolve(META_DIR),
        partitionDirectory.resolve(SNAPSHOTS_DIR),
        partitionDirectory.resolve(PENDING_DIR));
  }

  /**
   * The business RocksDB runtime directory (owned by {@code PartitionStorage}, not created here).
   */
  public static Path runtimeDirectory(final Path partitionDirectory) {
    return partitionDirectory.resolve(RUNTIME_DIR);
  }

  /**
   * Creates the raft directories jraft needs before a group is registered (log/meta/snapshot). The
   * snapshot temp dir is created lazily by jraft's snapshot storage, so it is not pre-created here.
   */
  public RaftStorageLayout create() {
    try {
      Files.createDirectories(logDir);
      Files.createDirectories(metaDir);
      Files.createDirectories(snapshotDir);
    } catch (final Exception e) {
      throw new IllegalStateException(
          "failed to create raft storage layout under " + partitionDirectory, e);
    }
    return this;
  }
}
