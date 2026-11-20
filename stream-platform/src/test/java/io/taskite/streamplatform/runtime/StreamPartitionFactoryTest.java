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
package io.taskite.streamplatform.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.junit.jupiter.api.Test;

final class StreamPartitionFactoryTest {
  @Test
  void rollbackClosesResourcesInReverseCreationOrderAndPreservesSuppressedErrors() {
    final List<String> closed = new ArrayList<>();
    final Deque<Runnable> resources = new ArrayDeque<>();
    resources.push(() -> closed.add("actor"));
    resources.push(() -> closed.add("storage"));
    final IllegalStateException closeFailure = new IllegalStateException("close failed");
    resources.push(
        () -> {
          closed.add("raft");
          throw closeFailure;
        });
    resources.push(() -> closed.add("log"));
    final RuntimeException creationFailure = new RuntimeException("create failed");

    StreamPartitionFactory.rollback(resources, creationFailure);

    assertEquals(List.of("log", "raft", "storage", "actor"), closed);
    assertEquals(1, creationFailure.getSuppressed().length);
    assertSame(closeFailure, creationFailure.getSuppressed()[0]);
  }
}
