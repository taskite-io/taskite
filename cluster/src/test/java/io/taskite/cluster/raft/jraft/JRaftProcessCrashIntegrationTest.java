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
package io.taskite.cluster.raft.jraft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alipay.sofa.jraft.util.Endpoint;
import io.taskite.cluster.raft.RaftPartitionConfig;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Three independent JVMs: no graceful shutdown or TimeoutNow is possible on SIGKILL. */
public final class JRaftProcessCrashIntegrationTest {
  @Test
  @Timeout(120)
  void promotedVotersElectAfterLeaderProcessCrashAndRecoverCold(@TempDir final Path root)
      throws Exception {
    final int[] ports = {port(), port(), port()};
    final List<Process> processes = new ArrayList<>();
    try {
      for (int i = 0; i < 3; i++) processes.add(start(root, ports, i));
      await(() -> "b0".equals(state(root, 0).getProperty("leader")));
      command(root, 0, "promote");
      await(() -> allVoters(root));
      final long priorTerm = Long.parseLong(state(root, 1).getProperty("term"));
      processes.get(0).destroyForcibly();
      assertTrue(processes.get(0).waitFor(10, TimeUnit.SECONDS));
      Files.deleteIfExists(root.resolve("b0.status"));
      await(() -> leaderAfter(root, 1, priorTerm) || leaderAfter(root, 2, priorTerm));
      final int elected = leaderAfter(root, 1, priorTerm) ? 1 : 2;
      assertTrue(Long.parseLong(state(root, elected).getProperty("term")) > priorTerm);
      command(root, elected, "append");
      await(() -> "append".equals(state(root, elected).getProperty("completed")));
      assertEquals("3", state(root, elected).getProperty("voters"));
      processes.set(0, start(root, ports, 0));
      await(() -> allVoters(root));
      for (final Process process : processes) {
        process.destroyForcibly();
        assertTrue(process.waitFor(10, TimeUnit.SECONDS));
      }
      processes.clear();
      for (int i = 0; i < 3; i++) {
        Files.deleteIfExists(root.resolve("b" + i + ".status"));
        processes.add(start(root, ports, i));
      }
      await(() -> allVoters(root) && (isLeader(root, 0) || isLeader(root, 1) || isLeader(root, 2)));
      final int coldLeader = isLeader(root, 0) ? 0 : isLeader(root, 1) ? 1 : 2;
      command(root, coldLeader, "append");
      await(() -> "append".equals(state(root, coldLeader).getProperty("completed")));
    } finally {
      for (final Process process : processes) {
        process.destroyForcibly();
        process.waitFor(10, TimeUnit.SECONDS);
      }
    }
  }

  private static Process start(final Path root, final int[] ports, final int index)
      throws Exception {
    return new ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-cp",
            System.getProperty("java.class.path"),
            JRaftProcessCrashIntegrationTest.class.getName(),
            root.toString(),
            "" + index,
            "" + ports[0],
            "" + ports[1],
            "" + ports[2])
        .redirectErrorStream(true)
        .redirectOutput(
            ProcessBuilder.Redirect.appendTo(root.resolve("b" + index + ".log").toFile()))
        .start();
  }

  private static boolean allVoters(final Path root) {
    for (int i = 0; i < 3; i++) {
      final Properties value = state(root, i);
      if (!"3".equals(value.getProperty("voters"))
          || !("" + (i == 0 ? 100 : 10)).equals(value.getProperty("priority"))) {
        return false;
      }
    }
    return true;
  }

  private static boolean isLeader(final Path root, final int index) {
    return ("b" + index).equals(state(root, index).getProperty("leader"));
  }

  private static boolean leaderAfter(final Path root, final int index, final long priorTerm) {
    final Properties observed = state(root, index);
    return ("b" + index).equals(observed.getProperty("leader"))
        && Long.parseLong(observed.getProperty("term", "0")) > priorTerm;
  }

  private static Properties state(final Path root, final int index) {
    final Properties value = new Properties();
    try (final var input = Files.newInputStream(root.resolve("b" + index + ".status"))) {
      value.load(input);
    } catch (final Exception ignored) {
      // A status file appears only after the child has initialized its group.
    }
    return value;
  }

  private static void command(final Path root, final int index, final String command)
      throws Exception {
    Files.writeString(root.resolve("b" + index + ".command"), command);
  }

  private static void await(final BooleanSupplier condition) throws Exception {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(50);
    assertTrue(condition.getAsBoolean(), "three-process election did not converge in 25s");
  }

  private static int port() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  public static void main(final String[] args) throws Exception {
    final Path root = Path.of(args[0]);
    final int index = Integer.parseInt(args[1]);
    final List<RaftPeer> initial = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      initial.add(
          new RaftPeer(
              "b" + i, "127.0.0.1", Integer.parseInt(args[i + 2]), i == 0, i == 0 ? 100 : 0));
    }
    final var self = initial.get(index);
    try (JRaftServer server = new JRaftServer(new Endpoint(self.host(), self.port()));
        JRaftPartition partition =
            JRaftPartition.builder(
                    RaftPartitionConfig.builder("process-crash", 1, self.brokerId())
                        .peers(initial)
                        .dataDirectory(root.resolve(self.brokerId()))
                        .electionTimeout(Duration.ofMillis(500))
                        .build(),
                    server,
                    root.resolve(self.brokerId()))
                .priorityFn(RaftPeer::priority)
                .build()) {
      partition.start();
      String completed = "";
      for (; ; ) {
        final Path command = root.resolve(self.brokerId() + ".command");
        if (Files.exists(command)) {
          final String operation = Files.readString(command).trim();
          Files.delete(command);
          if (operation.equals("promote")) {
            final List<RaftPeer> voters =
                initial.stream()
                    .map(
                        peer ->
                            new RaftPeer(
                                peer.brokerId(),
                                peer.host(),
                                peer.port(),
                                true,
                                peer.brokerId().equals("b0") ? 100 : 10))
                    .toList();
            partition.updateRaftMembership(voters).get(20, TimeUnit.SECONDS);
          } else if (operation.equals("append")) {
            partition
                .append(partition.lastIndex() + 1, partition.lastIndex() + 1, new byte[] {7})
                .get(10, TimeUnit.SECONDS);
          }
          completed = operation;
        }
        final Properties status = new Properties();
        final var observation = partition.electionState();
        status.setProperty("leader", partition.leaderId() == null ? "" : partition.leaderId());
        status.setProperty("priority", "" + (observation == null ? -1 : observation.priority()));
        status.setProperty("term", "" + (observation == null ? 0 : observation.term()));
        status.setProperty(
            "voters",
            ""
                + partition.members().stream()
                    .filter(member -> member.role() != RaftRole.LEARNER)
                    .count());
        status.setProperty("completed", completed);
        final Path temporary = root.resolve(self.brokerId() + ".status.tmp");
        try (final var output = Files.newOutputStream(temporary)) {
          status.store(output, "");
        }
        Files.move(
            temporary,
            root.resolve(self.brokerId() + ".status"),
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
        Thread.sleep(50);
      }
    }
  }
}
