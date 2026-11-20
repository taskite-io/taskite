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
package io.taskite.placementdriver.runtime;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

public final class PlacementDriverTopologyManifestWriter {

  public void write(final Path dataDirectory, final PlacementDriverTopologyManifest manifest) {
    final Path target = dataDirectory.resolve(PlacementDriverTopologyManifestReader.FILE_NAME);
    final Path temporary =
        dataDirectory.resolve(PlacementDriverTopologyManifestReader.FILE_NAME + ".tmp");
    try {
      Files.createDirectories(dataDirectory);
      final byte[] bytes = PlacementDriverTopologyManifestCodec.encode(manifest);
      PlacementDriverTopologyManifestReader.validate(
          PlacementDriverTopologyManifestCodec.decode(bytes), manifest.localMemberId());
      try (FileChannel channel =
          FileChannel.open(
              temporary,
              StandardOpenOption.CREATE,
              StandardOpenOption.TRUNCATE_EXISTING,
              StandardOpenOption.WRITE)) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) {
          channel.write(buffer);
        }
        channel.force(true);
      }
      try {
        Files.move(
            temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (final AtomicMoveNotSupportedException error) {
        throw new IllegalStateException(
            "atomic move is required for topology manifest " + target, error);
      }
      try (FileChannel directory = FileChannel.open(dataDirectory, StandardOpenOption.READ)) {
        directory.force(true);
      }
    } catch (final IOException error) {
      throw new IllegalStateException("failed to write topology manifest " + target, error);
    }
  }
}
