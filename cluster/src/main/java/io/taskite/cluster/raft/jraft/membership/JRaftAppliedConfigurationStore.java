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
import io.taskite.cluster.raft.RaftNodeState;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/** Crash durable certificate of a configuration that was observed stable and locally applied. */
public final class JRaftAppliedConfigurationStore {
  private final Path file;

  public JRaftAppliedConfigurationStore(final Path directory) {
    file = directory.resolve("applied-raft-configuration");
  }

  public TaskiteElectionLifecycle.Observation load() {
    return read(file);
  }

  static TaskiteElectionLifecycle.Observation read(final Path file) {
    if (!Files.exists(file)) {
      return null;
    }
    try (final DataInputStream input = new DataInputStream(Files.newInputStream(file))) {
      final int format = input.readInt();
      if (format != 1 && format != 2) {
        throw new IOException("unsupported applied configuration format");
      }
      final long index = input.readLong();
      final long covered = format == 2 ? input.readLong() : index;
      if (index < 0 || covered < index) {
        throw new IOException("invalid applied configuration index");
      }
      final Configuration configuration = new Configuration();
      readPeers(input).forEach(configuration::addPeer);
      readPeers(input).forEach(configuration::addLearner);
      return new TaskiteElectionLifecycle.Observation(
          new TaskiteElectionLifecycle.Observation.Membership(configuration, index, covered),
          new TaskiteElectionLifecycle.Observation.Runtime(0, 0, RaftNodeState.RECOVERED, 0));
    } catch (final IOException error) {
      throw new IllegalStateException("Cannot read applied configuration " + file, error);
    }
  }

  public void save(final TaskiteElectionLifecycle.Observation observation) {
    write(file, observation);
  }

  static void write(final Path file, final TaskiteElectionLifecycle.Observation observation) {
    final Path temporary = file.resolveSibling(file.getFileName() + ".tmp");
    try {
      try (final DataOutputStream output = new DataOutputStream(Files.newOutputStream(temporary))) {
        output.writeInt(2);
        output.writeLong(observation.configurationIndex());
        output.writeLong(observation.coveredIndex());
        writePeers(output, observation.configuration().listPeers());
        writePeers(output, observation.configuration().listLearners());
      }
      try (final FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
        channel.force(true);
      }
      Files.move(
          temporary, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      try (final FileChannel directory =
          FileChannel.open(file.getParent(), StandardOpenOption.READ)) {
        directory.force(true);
      }
    } catch (final IOException error) {
      throw new IllegalStateException("Cannot persist applied configuration " + file, error);
    }
  }

  private static void writePeers(final DataOutputStream output, final List<PeerId> peers)
      throws IOException {
    output.writeInt(peers.size());
    for (final PeerId peer : peers) {
      output.writeUTF(peer.toString());
    }
  }

  private static List<PeerId> readPeers(final DataInputStream input) throws IOException {
    final int count = input.readInt();
    if (count < 0 || count > 100_000) {
      throw new IOException("Invalid member count " + count);
    }
    final ArrayList<PeerId> peers = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      final PeerId peer = new PeerId();
      if (!peer.parse(input.readUTF())) {
        throw new IOException("Invalid applied member");
      }
      peers.add(peer);
    }
    return peers;
  }
}
