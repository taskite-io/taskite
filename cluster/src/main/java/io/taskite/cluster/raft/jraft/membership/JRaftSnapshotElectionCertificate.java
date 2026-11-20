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
package io.taskite.cluster.raft.jraft.membership;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.TaskiteElectionLifecycle;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.snapshot.Snapshot;
import com.alipay.sofa.jraft.storage.snapshot.local.LocalSnapshotMetaTable;
import io.taskite.cluster.raft.RaftNodeState;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

/** Reads committed snapshot metadata without opening or mutating snapshot/log storage. */
public final class JRaftSnapshotElectionCertificate {
  public static final String FILE = "taskite-election-configuration";

  private JRaftSnapshotElectionCertificate() {}

  static TaskiteElectionLifecycle.Observation load(final Path directory) throws IOException {
    try (final var paths = Files.list(directory)) {
      final Path snapshot =
          paths
              .filter(Files::isDirectory)
              .filter(path -> index(path) > 0)
              .max(Comparator.comparingLong(JRaftSnapshotElectionCertificate::index))
              .orElse(null);
      if (snapshot == null) {
        return null;
      }
      final var table = new LocalSnapshotMetaTable(new RaftOptions());
      if (!table.loadFromFile(snapshot.resolve(Snapshot.JRAFT_SNAPSHOT_META_FILE).toString())
          || !table.hasMeta()
          || table.getMeta().getLastIncludedIndex() != index(snapshot)) {
        throw new IOException("Invalid committed snapshot metadata: " + snapshot);
      }
      return read(snapshot, table.getMeta());
    }
  }

  public static TaskiteElectionLifecycle.Observation read(
      final Path snapshot, final SnapshotMeta meta) {
    final var baseline = from(meta);
    final var saved = JRaftAppliedConfigurationStore.read(snapshot.resolve(FILE));
    if (saved == null) {
      return baseline;
    }
    if (saved.coveredIndex() != meta.getLastIncludedIndex()
        || !sameConfiguration(saved.configuration(), baseline.configuration())) {
      throw new IllegalStateException("Snapshot election certificate does not match metadata");
    }
    return saved;
  }

  public static void save(
      final Path snapshot,
      final SnapshotMeta meta,
      final Function<SnapshotMeta, TaskiteElectionLifecycle.Observation> certificate) {
    final var baseline = from(meta);
    final var observed = certificate.apply(meta);
    final var saved =
        observed != null && sameConfiguration(observed.configuration(), baseline.configuration())
            ? new TaskiteElectionLifecycle.Observation(
                new TaskiteElectionLifecycle.Observation.Membership(
                    baseline.configuration(),
                    observed.configurationIndex(),
                    meta.getLastIncludedIndex()),
                baseline.runtime())
            : baseline;
    JRaftAppliedConfigurationStore.write(snapshot.resolve(FILE), saved);
  }

  private static boolean sameConfiguration(final Configuration left, final Configuration right) {
    // PeerId.equals deliberately ignores priority; certificate equality must include it.
    return names(left.listPeers()).equals(names(right.listPeers()))
        && names(left.listLearners()).equals(names(right.listLearners()));
  }

  private static List<String> names(final List<PeerId> peers) {
    return peers.stream().map(PeerId::toString).sorted().toList();
  }

  static TaskiteElectionLifecycle.Observation from(final SnapshotMeta meta) {
    if (meta == null || meta.getLastIncludedIndex() <= 0) {
      throw new IllegalStateException("Snapshot has no applied configuration index");
    }
    final Configuration configuration = new Configuration();
    // A joint snapshot cannot grant the new side's learners election eligibility early.
    final boolean joint = !meta.getOldPeersList().isEmpty();
    (joint ? meta.getOldPeersList() : meta.getPeersList())
        .forEach(value -> configuration.addPeer(parse(value)));
    (joint ? meta.getOldLearnersList() : meta.getLearnersList())
        .forEach(value -> configuration.addLearner(parse(value)));
    if (configuration.isEmpty()) {
      throw new IllegalStateException("Snapshot has no authorized voters");
    }
    return new TaskiteElectionLifecycle.Observation(
        new TaskiteElectionLifecycle.Observation.Membership(
            configuration, 0, meta.getLastIncludedIndex()),
        new TaskiteElectionLifecycle.Observation.Runtime(
            0,
            0,
            joint ? RaftNodeState.JOINT_SNAPSHOT_BASELINE : RaftNodeState.SNAPSHOT,
            meta.getLastIncludedTerm()));
  }

  private static PeerId parse(final String value) {
    final PeerId peer = PeerId.parsePeer(value);
    if (peer == null) {
      throw new IllegalStateException("Invalid snapshot peer: " + value);
    }
    return peer;
  }

  private static long index(final Path path) {
    final String name = path.getFileName().toString();
    if (!name.startsWith(Snapshot.JRAFT_SNAPSHOT_PREFIX)) {
      return -1;
    }
    try {
      return Long.parseLong(name.substring(Snapshot.JRAFT_SNAPSHOT_PREFIX.length()));
    } catch (final NumberFormatException ignored) {
      return -1;
    }
  }
}
