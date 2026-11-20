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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class PartitionGroupManager<G extends AutoCloseable> implements AutoCloseable {
  private final PartitionGroupFactory<G> factory;
  private final PartitionGroupReusePolicy<G> reusePolicy;
  private final ConcurrentHashMap<Integer, G> groups = new ConcurrentHashMap<>();

  public PartitionGroupManager(
      PartitionGroupFactory<G> factory, PartitionGroupReusePolicy<G> reusePolicy) {
    this.factory = Objects.requireNonNull(factory, "factory");
    this.reusePolicy = Objects.requireNonNull(reusePolicy, "reusePolicy");
  }

  public synchronized void reconcile(Collection<PartitionGroupDescriptor> descriptors) {
    Objects.requireNonNull(descriptors, "descriptors");
    Map<Integer, PartitionGroupDescriptor> expected = new LinkedHashMap<>();
    for (PartitionGroupDescriptor descriptor : descriptors) {
      expected.put(descriptor.groupId(), descriptor);
    }

    for (Integer groupId : groups.keySet()) {
      if (!expected.containsKey(groupId)) {
        closeGroup(groups.remove(groupId));
      }
    }

    for (PartitionGroupDescriptor descriptor : expected.values()) {
      groups.compute(
          descriptor.groupId(),
          (groupId, existing) -> {
            if (existing != null && reusePolicy.canReuse(existing, descriptor)) {
              return existing;
            }
            closeGroup(existing);
            return factory.create(descriptor);
          });
    }
  }

  public synchronized G reconcileOne(PartitionGroupDescriptor descriptor) {
    upsertOne(descriptor);
    return requireGroup(descriptor.groupId());
  }

  public synchronized G upsertOne(PartitionGroupDescriptor descriptor) {
    Objects.requireNonNull(descriptor, "descriptor");
    groups.compute(
        descriptor.groupId(),
        (groupId, existing) -> {
          if (existing != null && reusePolicy.canReuse(existing, descriptor)) {
            return existing;
          }
          closeGroup(existing);
          return factory.create(descriptor);
        });
    return requireGroup(descriptor.groupId());
  }

  public Optional<G> group(int groupId) {
    return Optional.ofNullable(groups.get(groupId));
  }

  public synchronized void remove(final int groupId) {
    closeGroup(groups.remove(groupId));
  }

  public G requireGroup(int groupId) {
    return group(groupId)
        .orElseThrow(() -> new IllegalArgumentException("missing partition group " + groupId));
  }

  public Collection<G> groups() {
    return groups.values();
  }

  public int groupCount() {
    return groups.size();
  }

  @Override
  public void close() {
    for (G group : groups.values()) {
      closeGroup(group);
    }
    groups.clear();
  }

  private void closeGroup(G group) {
    if (group == null) {
      return;
    }
    try {
      group.close();
    } catch (Exception e) {
      throw new IllegalStateException("failed to close partition group", e);
    }
  }
}
