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

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class JRaftExecutionAuthorityTest {
  @Test
  void authoritySurvivesReopenAndSnapshotInstall(@TempDir final Path directory) throws Exception {
    final var source = new JRaftExecutionAuthority(directory);
    assertTrue(source.permits(0, "legacy"));
    source.closeAdmission();
    assertFalse(source.permits(0, "legacy"));
    source.apply(1, "successor", 20);
    source.apply(1, "successor", 21);
    assertFalse(source.permits(1, "successor"));
    source.openAdmission();
    assertTrue(source.permits(1, "successor"));
    assertFalse(source.permits(0, "legacy"));
    assertFalse(source.permits(1, "other"));
    source.apply(1, "successor", 20);
    assertEquals(source.current(), new JRaftExecutionAuthority(directory).current());

    final Path snapshot = Files.createDirectory(directory.resolve("snapshot"));
    source.saveSnapshot(snapshot);
    final Path follower = Files.createDirectory(directory.resolve("follower"));
    final var installed = new JRaftExecutionAuthority(follower);
    installed.loadSnapshot(snapshot);
    assertEquals(source.current(), installed.current());
    assertFalse(new JRaftExecutionAuthority(follower).permits(0, "legacy"));
    assertTrue(new JRaftExecutionAuthority(follower).permits(1, "successor"));
  }

  @Test
  void rejectsSkippedGenerationsWithoutChangingCertificate(@TempDir final Path directory) {
    final var authority = new JRaftExecutionAuthority(directory);
    authority.apply(2, "skip", 10);
    assertEquals(0, authority.current().generation());
    authority.apply(1, "", 10);
    assertEquals(new JRaftExecutionAuthority.Certificate(0, "", 0), authority.current());
    authority.apply(1, "new", 10);
    authority.apply(0, "old", 9);
    assertEquals(new JRaftExecutionAuthority.Certificate(1, "new", 10), authority.current());
  }

  @Test
  void lateCompetingOwnerCannotReplaceCommittedGeneration(@TempDir final Path directory) {
    final var authority = new JRaftExecutionAuthority(directory);
    authority.apply(1, "winner", 10);
    authority.apply(1, "late-rpc", 11);
    assertEquals(new JRaftExecutionAuthority.Certificate(1, "winner", 10), authority.current());
    assertFalse(authority.permits(1, "late-rpc"));
    assertEquals(authority.current(), new JRaftExecutionAuthority(directory).current());
  }

  @Test
  void olderSnapshotDoesNotRollbackDurableFence(@TempDir final Path directory) throws Exception {
    final var authority = new JRaftExecutionAuthority(directory);
    final var snapshot = Files.createDirectory(directory.resolve("snapshot"));
    authority.saveSnapshot(snapshot);
    authority.apply(1, "successor", 20);
    authority.loadSnapshot(snapshot);
    assertEquals(new JRaftExecutionAuthority.Certificate(1, "successor", 20), authority.current());
    assertFalse(authority.permits(0, "legacy"));
    assertEquals(authority.current(), new JRaftExecutionAuthority(directory).current());
  }
}
