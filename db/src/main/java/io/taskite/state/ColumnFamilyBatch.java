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

import java.nio.charset.StandardCharsets;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * A {@link StateBatch} bound to a single column family. The underlying {@link WriteBatch} is
 * exposed so callers can stage additional writes into other column families of the same database
 * (e.g. the raft apply index) for an atomic cross-column-family commit.
 */
public final class ColumnFamilyBatch implements StateBatch {
  private final RocksDB db;
  private final ColumnFamilyHandle columnFamily;
  private final String keyPrefix;
  private final WriteBatch writeBatch = new WriteBatch();

  public ColumnFamilyBatch(RocksDB db, ColumnFamilyHandle columnFamily, String keyPrefix) {
    this.db = db;
    this.columnFamily = columnFamily;
    this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
  }

  @Override
  public void put(String key, String value) {
    try {
      writeBatch.put(columnFamily, keyBytes(key), bytes(value));
    } catch (RocksDBException e) {
      throw new IllegalStateException("failed to buffer state key " + key, e);
    }
  }

  @Override
  public void putBytes(String key, byte[] value) {
    try {
      writeBatch.put(columnFamily, keyBytes(key), value);
    } catch (RocksDBException e) {
      throw new IllegalStateException("failed to buffer state key " + key, e);
    }
  }

  @Override
  public void delete(String key) {
    try {
      writeBatch.delete(columnFamily, keyBytes(key));
    } catch (RocksDBException e) {
      throw new IllegalStateException("failed to buffer delete of state key " + key, e);
    }
  }

  @Override
  public WriteBatch writeBatch() {
    return writeBatch;
  }

  @Override
  public void commit() {
    try (WriteOptions writeOptions = new WriteOptions()) {
      db.write(writeOptions, writeBatch);
    } catch (RocksDBException e) {
      throw new IllegalStateException("failed to commit state batch", e);
    }
  }

  @Override
  public void close() {
    writeBatch.close();
  }

  private byte[] keyBytes(String key) {
    return bytes(keyPrefix + key);
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
