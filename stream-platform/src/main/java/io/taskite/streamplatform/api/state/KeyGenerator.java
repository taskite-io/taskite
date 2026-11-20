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
package io.taskite.streamplatform.api.state;

/**
 * Generates the globally-unique keys a command processor stamps onto the entities it creates. The
 * high-water mark is part of the partition's state (ZEP004 "Key Generation"): every generated key
 * is carried in an event record, so replaying those events must lift the high-water back up.
 * Otherwise a follower that replayed {@code *:CREATED} events produced after its snapshot would,
 * once promoted to leader, re-issue keys that are already in use.
 */
@FunctionalInterface
public interface KeyGenerator {
  long nextKey();

  /**
   * Raises the high-water mark so the next {@link #nextKey()} is strictly greater than {@code key},
   * when it belongs to the local cluster and partition. Foreign keys are ignored. The default is a
   * no-op so purely in-memory / test generators need not track a high-water.
   */
  default void setKeyIfHigher(final long key) {}
}
