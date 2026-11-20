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
package io.taskite.cluster.raft.group;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.cluster.raft.RaftRole;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PartitionGroupManagerTest {
  @Test
  void reconcilesMultipleGroupsAndRecreatesWhenReusePolicyRejects() {
    AtomicInteger created = new AtomicInteger();
    try (PartitionGroupManager<TestGroup> manager =
        new PartitionGroupManager<>(
            descriptor ->
                new TestGroup(descriptor.groupId(), descriptor.role(), created.incrementAndGet()),
            (existing, descriptor) -> existing.role == descriptor.role())) {
      manager.reconcile(List.of(descriptor(0, RaftRole.LEADER), descriptor(1, RaftRole.FOLLOWER)));

      assertEquals(2, manager.groupCount());
      assertEquals(1, manager.requireGroup(0).generation);
      assertEquals(2, manager.requireGroup(1).generation);

      manager.reconcile(List.of(descriptor(0, RaftRole.FOLLOWER), descriptor(2, RaftRole.LEADER)));

      assertEquals(2, manager.groupCount());
      assertEquals(3, manager.requireGroup(0).generation);
      assertEquals(4, manager.requireGroup(2).generation);
      assertTrue(manager.group(1).isEmpty());
    }
  }

  @Test
  void reconcilesSingleGroup() {
    try (PartitionGroupManager<TestGroup> manager =
        new PartitionGroupManager<>(
            descriptor -> new TestGroup(descriptor.groupId(), descriptor.role(), 1),
            (existing, descriptor) -> true)) {
      TestGroup group = manager.reconcileOne(descriptor(0, RaftRole.FOLLOWER));

      assertEquals(0, group.groupId);
      assertEquals(1, manager.groupCount());
    }
  }

  private static PartitionGroupDescriptor descriptor(int groupId, RaftRole role) {
    return new PartitionGroupDescriptor(groupId, "test", 1, role, List.of());
  }

  private static final class TestGroup implements AutoCloseable {
    private final int groupId;
    private final RaftRole role;
    private final int generation;

    private TestGroup(int groupId, RaftRole role, int generation) {
      this.groupId = groupId;
      this.role = role;
      this.generation = generation;
    }

    @Override
    public void close() {}
  }
}
