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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.alipay.sofa.jraft.core.TaskiteElectionLifecycle;
import io.taskite.cluster.raft.RaftNodeState;
import io.taskite.cluster.raft.RaftPeer;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class JRaftAppliedConfigurationStoreTest {
  @TempDir Path directory;

  @Test
  void configurationSurvivesReopeningWithIpv6VotersAndLearners() {
    final var configuration =
        JRaftPeerMapper.toConfiguration(
            List.of(
                new RaftPeer("b0", "2001:db8::1", 26502, true, 10),
                new RaftPeer("b1", "2001:db8::2", 26502, false, 0)),
            RaftPeer::priority);
    final var observation =
        new TaskiteElectionLifecycle.Observation(
            new TaskiteElectionLifecycle.Observation.Membership(configuration, 7, 12),
            new TaskiteElectionLifecycle.Observation.Runtime(
                10, 10, RaftNodeState.STATE_LEADER, 0));
    new JRaftAppliedConfigurationStore(directory).save(observation);

    final var restored = new JRaftAppliedConfigurationStore(directory).load();
    assertEquals(configuration, restored.configuration());
    assertEquals(7, restored.configurationIndex());
    assertEquals(12, restored.coveredIndex());
    assertEquals(10, restored.configuration().listPeers().getFirst().getPriority());
    assertEquals(RaftNodeState.RECOVERED, restored.state());
  }

  @Test
  void missingCertificateHasNoObservation() {
    assertNull(new JRaftAppliedConfigurationStore(directory).load());
  }
}
