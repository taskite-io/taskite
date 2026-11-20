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
package io.taskite.state;

import org.rocksdb.WriteBatch;

/**
 * A buffered set of state mutations that are applied atomically on {@link #commit()}. The backing
 * {@link WriteBatch} is exposed so callers can stage additional writes (e.g. the raft apply index
 * living in a different column family) into the same atomic batch.
 */
public interface StateBatch extends AutoCloseable {
  void put(String key, String value);

  void putBytes(String key, byte[] value);

  void delete(String key);

  WriteBatch writeBatch();

  void commit();

  @Override
  void close();
}
