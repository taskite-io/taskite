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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/** A partition authority changed only by applied Raft entries and preserved by snapshots. */
public final class JRaftExecutionAuthority {
  public static final String FILE = "execution-authority";
  private final Path file;
  private volatile Certificate certificate;
  private volatile boolean admissionClosed;

  public record Certificate(long generation, String owner, long index) {
    public Certificate {
      Objects.requireNonNull(owner, "owner");
      if (generation < 0 || index < 0) {
        throw new IllegalArgumentException("negative authority");
      }
    }
  }

  public JRaftExecutionAuthority(final Path directory) {
    file = directory.resolve(FILE);
    certificate = read(file);
  }

  public Certificate current() {
    return certificate;
  }

  public void closeAdmission() {
    admissionClosed = true;
  }

  public void openAdmission() {
    admissionClosed = false;
  }

  public boolean permits(final long generation, final String owner) {
    final var current = certificate;
    return !admissionClosed
        && current.generation() == generation
        && (generation == 0 || current.owner().equals(owner));
  }

  public synchronized void apply(final long generation, final String owner, final long index) {
    if (index <= certificate.index()) {
      return;
    }
    if (generation == certificate.generation() && owner.equals(certificate.owner())) {
      return;
    }
    // A timed-out RPC may still commit after a retry reached another leader.
    // Competing owners for the same generation must be harmless applied no-ops.
    if (generation != certificate.generation() + 1 || owner.isBlank()) {
      return;
    }
    final var next = new Certificate(generation, owner, index);
    write(file, next);
    certificate = next;
  }

  public void saveSnapshot(final Path directory) {
    write(directory.resolve(FILE), certificate);
  }

  public synchronized void loadSnapshot(final Path directory) {
    final var restored = read(directory.resolve(FILE));
    if (restored.index() < certificate.index()
        || restored.generation() < certificate.generation()) {
      // Local recovery can load an older snapshot before replaying its retained suffix.
      // Preserve the durable fence throughout that recovery instead of rolling it back.
      return;
    }
    write(file, restored);
    certificate = restored;
  }

  private static Certificate read(final Path path) {
    if (!Files.exists(path)) {
      return new Certificate(0, "", 0);
    }
    try {
      final var data = ByteBuffer.wrap(Files.readAllBytes(path));
      final long generation = data.getLong();
      final long index = data.getLong();
      final byte[] owner = new byte[data.remaining()];
      data.get(owner);
      return new Certificate(generation, new String(owner, StandardCharsets.UTF_8), index);
    } catch (final IOException | RuntimeException error) {
      throw new IllegalStateException("Cannot restore execution authority: " + path, error);
    }
  }

  private static void write(final Path path, final Certificate value) {
    final byte[] owner = value.owner().getBytes(StandardCharsets.UTF_8);
    final byte[] bytes =
        ByteBuffer.allocate(16 + owner.length)
            .putLong(value.generation())
            .putLong(value.index())
            .put(owner)
            .array();
    final Path temporary = path.resolveSibling(FILE + ".tmp");
    try {
      try (final FileChannel channel =
          FileChannel.open(
              temporary,
              StandardOpenOption.CREATE,
              StandardOpenOption.TRUNCATE_EXISTING,
              StandardOpenOption.WRITE)) {
        final ByteBuffer data = ByteBuffer.wrap(bytes);
        while (data.hasRemaining()) channel.write(data);
        channel.force(true);
      }
      Files.move(
          temporary, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      try (final FileChannel directory =
          FileChannel.open(path.getParent(), StandardOpenOption.READ)) {
        directory.force(true);
      }
    } catch (final IOException error) {
      throw new IllegalStateException("Cannot persist execution authority: " + path, error);
    }
  }
}
