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
package io.taskite.cluster.raft.jraft.log;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/** Compact encoded-size index using one primitive array per 1024 contiguous raft indexes. */
final class LogSizeIndex {
  private static final int BLOCK_SHIFT = 10;
  private static final int BLOCK_SIZE = 1 << BLOCK_SHIFT;
  private static final int BLOCK_MASK = BLOCK_SIZE - 1;

  private final NavigableMap<Long, Block> blocks = new TreeMap<>();
  private long totalBytes;

  void put(final long index, final int encodedBytes) {
    if (index < 0 || encodedBytes <= 0) {
      throw new IllegalArgumentException("log index and encoded bytes must be positive");
    }
    final long blockId = blockId(index);
    final Block block = blocks.computeIfAbsent(blockId, ignored -> new Block());
    final int offset = offset(index);
    final int previous = block.sizes[offset];
    block.sizes[offset] = encodedBytes;
    block.bytes += (long) encodedBytes - previous;
    totalBytes += (long) encodedBytes - previous;
  }

  void removeBefore(final long firstIndexKept) {
    final long boundaryBlockId = blockId(firstIndexKept);
    final Iterator<Map.Entry<Long, Block>> iterator = blocks.entrySet().iterator();
    while (iterator.hasNext()) {
      final Map.Entry<Long, Block> entry = iterator.next();
      if (entry.getKey() < boundaryBlockId) {
        totalBytes -= entry.getValue().bytes;
        iterator.remove();
      } else {
        break;
      }
    }
    final Block boundary = blocks.get(boundaryBlockId);
    if (boundary != null) {
      totalBytes -= boundary.clearBefore(offset(firstIndexKept));
      removeEmptyBlock(boundaryBlockId, boundary);
    }
  }

  void removeAfter(final long lastIndexKept) {
    final long boundaryBlockId = blockId(lastIndexKept);
    final Iterator<Map.Entry<Long, Block>> iterator = blocks.descendingMap().entrySet().iterator();
    while (iterator.hasNext()) {
      final Map.Entry<Long, Block> entry = iterator.next();
      if (entry.getKey() > boundaryBlockId) {
        totalBytes -= entry.getValue().bytes;
        iterator.remove();
      } else {
        break;
      }
    }
    final Block boundary = blocks.get(boundaryBlockId);
    if (boundary != null) {
      totalBytes -= boundary.clearAfter(offset(lastIndexKept));
      removeEmptyBlock(boundaryBlockId, boundary);
    }
  }

  long bytesAfter(final long indexExclusive) {
    if (blocks.isEmpty()) {
      return 0;
    }
    final long firstBlockId = blockId(indexExclusive + 1);
    long bytes = 0;
    for (final Map.Entry<Long, Block> entry : blocks.tailMap(firstBlockId, true).entrySet()) {
      if (entry.getKey() == firstBlockId) {
        bytes += entry.getValue().bytesFrom(offset(indexExclusive + 1));
      } else {
        bytes += entry.getValue().bytes;
      }
    }
    return bytes;
  }

  long totalBytes() {
    return totalBytes;
  }

  long firstIndex() {
    if (blocks.isEmpty()) {
      return 0;
    }
    final Map.Entry<Long, Block> entry = blocks.firstEntry();
    return index(entry.getKey(), entry.getValue().firstOccupiedOffset());
  }

  long lastIndex() {
    if (blocks.isEmpty()) {
      return 0;
    }
    final Map.Entry<Long, Block> entry = blocks.lastEntry();
    return index(entry.getKey(), entry.getValue().lastOccupiedOffset());
  }

  void clear() {
    blocks.clear();
    totalBytes = 0;
  }

  private void removeEmptyBlock(final long blockId, final Block block) {
    if (block.bytes == 0) {
      blocks.remove(blockId);
    }
  }

  private static long blockId(final long index) {
    return Math.floorDiv(index, BLOCK_SIZE);
  }

  private static int offset(final long index) {
    return Math.floorMod(index, BLOCK_SIZE);
  }

  private static long index(final long blockId, final int offset) {
    return Math.addExact(Math.multiplyExact(blockId, BLOCK_SIZE), offset);
  }

  private static final class Block {
    private final int[] sizes = new int[BLOCK_SIZE];
    private long bytes;

    private long clearBefore(final int boundary) {
      long removed = 0;
      for (int i = 0; i < boundary; i++) {
        removed += sizes[i];
        sizes[i] = 0;
      }
      bytes -= removed;
      return removed;
    }

    private long clearAfter(final int boundary) {
      long removed = 0;
      for (int i = boundary + 1; i < sizes.length; i++) {
        removed += sizes[i];
        sizes[i] = 0;
      }
      bytes -= removed;
      return removed;
    }

    private long bytesFrom(final int start) {
      long result = 0;
      for (int i = start; i < sizes.length; i++) {
        result += sizes[i];
      }
      return result;
    }

    private int firstOccupiedOffset() {
      for (int i = 0; i < sizes.length; i++) {
        if (sizes[i] > 0) {
          return i;
        }
      }
      throw new IllegalStateException("empty log-size block");
    }

    private int lastOccupiedOffset() {
      for (int i = sizes.length - 1; i >= 0; i--) {
        if (sizes[i] > 0) {
          return i;
        }
      }
      throw new IllegalStateException("empty log-size block");
    }
  }
}
