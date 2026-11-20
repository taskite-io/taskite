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

import static org.junit.jupiter.api.Assertions.*;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.TaskiteElectionLifecycle;
import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;
import io.taskite.cluster.raft.RaftNodeState;
import io.taskite.cluster.raft.storage.RaftStorageLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class JRaftSnapshotElectionCertificateTest {
  @Test
  void jointSnapshotPreservesOldVotersAndDoesNotAuthorizePromotion() {
    final var meta =
        SnapshotMeta.newBuilder()
            .setLastIncludedIndex(12)
            .setLastIncludedTerm(3)
            .addPeers("127.0.0.1:1234::100")
            .addPeers("127.0.0.1:1235::10")
            .addOldPeers("127.0.0.1:1234::100")
            .addOldLearners("127.0.0.1:1235")
            .build();
    final var observation = JRaftSnapshotElectionCertificate.from(meta);
    assertEquals(1, observation.configuration().listPeers().size());
    assertEquals(1, observation.configuration().listLearners().size());
    assertEquals(0, observation.configurationIndex(), "legacy snapshot has no membership index");
    assertEquals(RaftNodeState.JOINT_SNAPSHOT_BASELINE, observation.state());
  }

  @Test
  void snapshotCoverageMustNotInventMembershipVersion() {
    final var meta =
        SnapshotMeta.newBuilder()
            .setLastIncludedIndex(500)
            .setLastIncludedTerm(7)
            .addPeers("127.0.0.1:1234::100")
            .build();
    final var observation = JRaftSnapshotElectionCertificate.from(meta);
    assertEquals(0, observation.configurationIndex());
    assertEquals(RaftNodeState.SNAPSHOT, observation.state());
    assertEquals(7, observation.term());
  }

  @Test
  void existingUncertifiedLogCannotUseExternalConfiguration(@TempDir final Path directory)
      throws Exception {
    final var layout = RaftStorageLayout.of(directory).create();
    Files.writeString(layout.logDir().resolve("CURRENT"), "existing log");
    final var initial = new Configuration();
    initial.parse("127.0.0.1:1234::100");
    final var error =
        assertThrows(
            IllegalStateException.class,
            () ->
                new JRaftElectionCoordinator(
                    new JRaftElectionCoordinator.RecoveryConfiguration(
                        layout, initial, Duration.ofMillis(100)),
                    new JRaftElectionCoordinator.Context(() -> null, ignored -> {}, null)));
    assertTrue(error.getMessage().contains("UNCERTIFIED_RAFT_STORAGE"));
    assertFalse(Files.exists(directory.resolve("applied-raft-configuration")));
  }

  @Test
  void snapshotCertificatePreservesActualConfigurationIndex(@TempDir final Path directory) {
    final var meta =
        SnapshotMeta.newBuilder()
            .setLastIncludedIndex(500)
            .setLastIncludedTerm(7)
            .addPeers("127.0.0.1:1234::100")
            .build();
    final var configuration = new Configuration();
    configuration.parse("127.0.0.1:1234::100");
    final var observation =
        new TaskiteElectionLifecycle.Observation(
            new TaskiteElectionLifecycle.Observation.Membership(configuration, 12, 12),
            new TaskiteElectionLifecycle.Observation.Runtime(
                100, 100, RaftNodeState.STATE_FOLLOWER, 7));
    JRaftSnapshotElectionCertificate.save(directory, meta, ignored -> observation);
    final var restored = JRaftSnapshotElectionCertificate.read(directory, meta);
    assertEquals(12, restored.configurationIndex());
    assertEquals(500, restored.coveredIndex());
    assertEquals(100, restored.configuration().listPeers().getFirst().getPriority());
  }
}
