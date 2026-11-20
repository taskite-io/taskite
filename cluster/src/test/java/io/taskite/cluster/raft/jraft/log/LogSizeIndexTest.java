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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

final class LogSizeIndexTest {

  @Test
  void tracksBytesAcrossBlockBoundariesAndTruncation() {
    final LogSizeIndex index = new LogSizeIndex();
    index.put(1023, 10);
    index.put(1024, 20);
    index.put(1025, 30);

    assertEquals(60, index.totalBytes());
    assertEquals(50, index.bytesAfter(1023));
    assertEquals(1023, index.firstIndex());
    assertEquals(1025, index.lastIndex());

    index.removeBefore(1024);
    assertEquals(50, index.totalBytes());
    assertEquals(1024, index.firstIndex());

    index.removeAfter(1024);
    assertEquals(20, index.totalBytes());
    assertEquals(1024, index.lastIndex());
  }

  @Test
  void replacesExistingIndexWithoutDoubleCounting() {
    final LogSizeIndex index = new LogSizeIndex();
    index.put(7, 10);
    index.put(7, 25);

    assertEquals(25, index.totalBytes());
    assertEquals(25, index.bytesAfter(6));
  }
}
