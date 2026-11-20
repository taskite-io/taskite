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
package io.taskite.broker.partition;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pre-open fenced-recovery generation guard (Q7). Each partition's committed recovery epoch is
 * mirrored into a small sibling marker file next to the RocksDB/raft data directory so it can be
 * read <em>before</em> the partition storage is opened — {@link BrokerPartitionRecoveryEpochStore}
 * keeps the authoritative value inside the store, but that value is only reachable once the store
 * is already open, which is too late to decide whether the on-disk state may be reused.
 *
 * <p>When the placement driver has advanced a partition's recovery epoch past the epoch this broker
 * last recorded locally, the local raft log/meta/snapshot belong to a superseded membership
 * generation (its committed entries may conflict with the survivor's post-reset history). Reusing
 * them would let a returning node replay stale, un-fenced state. The guard therefore moves the
 * stale directory aside (kept for forensics, never deleted) and records the new epoch, so the group
 * is recreated empty and rejoins as a learner that catches up from the current leader.
 *
 * <p>The marker defaults to {@code 0} and is only ever written for a non-zero epoch, so partitions
 * that never went through fenced recovery keep byte-for-byte identical on-disk behaviour.
 */
final class BrokerPartitionRecoveryGuard {
  private static final Logger LOG = LoggerFactory.getLogger(BrokerPartitionRecoveryGuard.class);
  private static final String MARKER_SUFFIX = ".__recovery-epoch";
  private static final String QUARANTINE_SUFFIX = ".__quarantine-";

  private BrokerPartitionRecoveryGuard() {}

  /** The recovery epoch this broker last recorded for {@code partitionDir}, or {@code 0}. */
  static long localEpoch(final Path partitionDir) {
    if (partitionDir == null) {
      return 0L;
    }
    final Path marker = markerFor(partitionDir);
    if (!Files.isRegularFile(marker)) {
      return 0L;
    }
    try {
      final String text = Files.readString(marker, StandardCharsets.UTF_8).trim();
      return text.isEmpty() ? 0L : Math.max(0L, Long.parseLong(text));
    } catch (final IOException | NumberFormatException error) {
      LOG.warn("failed to read recovery-epoch marker {}, treating as 0", marker, error);
      return 0L;
    }
  }

  /** Persists {@code epoch} as the locally recorded recovery epoch for {@code partitionDir}. */
  static void recordEpoch(final Path partitionDir, final long epoch) {
    if (partitionDir == null || epoch <= 0L) {
      return;
    }
    final Path marker = markerFor(partitionDir);
    try {
      Files.createDirectories(marker.getParent());
      Files.writeString(marker, Long.toString(epoch), StandardCharsets.UTF_8);
    } catch (final IOException error) {
      throw new UncheckedIOException(
          "failed to record recovery epoch " + epoch + " for " + partitionDir, error);
    }
  }

  /**
   * Quarantines {@code partitionDir} when {@code descriptorEpoch} is newer than the epoch recorded
   * locally, then records the new epoch. A stale data directory is moved aside; the returned path
   * is the directory the group should open (recreated empty when quarantined, otherwise unchanged).
   */
  static Path quarantineIfStale(final Path partitionDir, final long descriptorEpoch) {
    if (partitionDir == null || descriptorEpoch <= 0L) {
      return partitionDir;
    }
    final long local = localEpoch(partitionDir);
    if (descriptorEpoch <= local) {
      return partitionDir;
    }
    if (Files.exists(partitionDir)) {
      final Path quarantine =
          partitionDir.resolveSibling(
              partitionDir.getFileName()
                  + QUARANTINE_SUFFIX
                  + local
                  + "-"
                  + System.currentTimeMillis());
      try {
        Files.move(partitionDir, quarantine, StandardCopyOption.ATOMIC_MOVE);
      } catch (final IOException atomicFailure) {
        try {
          Files.move(partitionDir, quarantine);
        } catch (final IOException error) {
          throw new UncheckedIOException(
              "failed to quarantine stale partition directory " + partitionDir, error);
        }
      }
      LOG.warn(
          "quarantined stale partition state {} (local epoch {} < recovery epoch {}); moved to {}"
              + " and rejoining as a fresh learner",
          partitionDir,
          local,
          descriptorEpoch,
          quarantine);
    }
    recordEpoch(partitionDir, descriptorEpoch);
    return partitionDir;
  }

  private static Path markerFor(final Path partitionDir) {
    return partitionDir.resolveSibling(partitionDir.getFileName() + MARKER_SUFFIX);
  }
}
