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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * Shared RocksDB persistence for a single column family. Subclasses bind the store to a specific
 * column family (e.g. business state or raft metadata) and layer their own domain semantics on top.
 *
 * <p>The data lifecycle is identical regardless of what is stored: write into the column family and
 * read it back. Physical snapshots of the whole runtime database are taken as RocksDB checkpoints
 * by the cluster layer's snapshot store, so this class only owns plain key/value access plus the
 * {@link #captureState()} / {@link #replaceState(Map)} helpers used by callers that need a live
 * view of the column family.
 */
public abstract class AbstractColumnFamilyStore implements AutoCloseable {
  private final PartitionStorage storage;
  private final boolean ownsStorage;
  private final ColumnFamilyHandle columnFamily;
  private final String keyPrefix;

  protected AbstractColumnFamilyStore(
      PartitionStorage storage,
      boolean ownsStorage,
      ColumnFamilyHandle columnFamily,
      String keyPrefix) {
    this.storage = Objects.requireNonNull(storage, "storage");
    this.ownsStorage = ownsStorage;
    this.columnFamily = Objects.requireNonNull(columnFamily, "columnFamily");
    this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
  }

  public Optional<String> get(String key) {
    try {
      byte[] value = db().get(columnFamily, keyBytes(key));
      return Optional.ofNullable(value == null ? null : string(value));
    } catch (RocksDBException e) {
      throw new IllegalStateException("failed to read key " + key, e);
    }
  }

  public boolean exists(String key) {
    return get(key).isPresent();
  }

  public void put(String key, String value) {
    try {
      db().put(columnFamily, keyBytes(key), bytes(value));
    } catch (RocksDBException e) {
      throw new IllegalStateException("failed to write key " + key, e);
    }
  }

  public void delete(String key) {
    try {
      db().delete(columnFamily, keyBytes(key));
    } catch (RocksDBException e) {
      throw new IllegalStateException("failed to delete key " + key, e);
    }
  }

  /** Reads the raw byte value for {@code key}, or empty if absent. */
  public Optional<byte[]> getBytes(String key) {
    try {
      byte[] value = db().get(columnFamily, keyBytes(key));
      return Optional.ofNullable(value);
    } catch (RocksDBException e) {
      throw new IllegalStateException("failed to read key " + key, e);
    }
  }

  /** Writes a raw byte value for {@code key}. */
  public void putBytes(String key, byte[] value) {
    try {
      db().put(columnFamily, keyBytes(key), value);
    } catch (RocksDBException e) {
      throw new IllegalStateException("failed to write key " + key, e);
    }
  }

  public StateBatch newBatch() {
    return new ColumnFamilyBatch(db(), columnFamily, keyPrefix);
  }

  /** Captures the prefix-scoped contents of this column family as a key/value map. */
  protected Map<String, String> captureState() {
    Map<String, String> snapshot = new LinkedHashMap<>();
    try (RocksIterator iterator = db().newIterator(columnFamily)) {
      iterator.seekToFirst();
      while (iterator.isValid()) {
        String key = string(iterator.key());
        if (key.startsWith(keyPrefix)) {
          snapshot.put(key.substring(keyPrefix.length()), string(iterator.value()));
        }
        iterator.next();
      }
    }
    return Map.copyOf(snapshot);
  }

  /**
   * Captures the prefix-scoped keys of this column family. Unlike {@link #captureState()} this
   * never decodes values, so it is safe to use on column families whose values are opaque binary
   * (e.g. SBE-encoded records).
   */
  protected List<String> captureKeys() {
    List<String> keys = new ArrayList<>();
    try (RocksIterator iterator = db().newIterator(columnFamily)) {
      iterator.seekToFirst();
      while (iterator.isValid()) {
        String key = string(iterator.key());
        if (key.startsWith(keyPrefix)) {
          keys.add(key.substring(keyPrefix.length()));
        }
        iterator.next();
      }
    }
    return keys;
  }

  /** Replaces the prefix-scoped contents of this column family with the given key/value map. */
  protected void replaceState(Map<String, String> state) {
    try {
      clearState();
      for (Map.Entry<String, String> entry : state.entrySet()) {
        db().put(columnFamily, keyBytes(entry.getKey()), bytes(entry.getValue()));
      }
    } catch (RocksDBException e) {
      throw new IllegalStateException("failed to restore state", e);
    }
  }

  /** Deletes every prefix-scoped key from this column family. */
  protected void clearState() {
    try (RocksIterator iterator = db().newIterator(columnFamily)) {
      iterator.seekToFirst();
      while (iterator.isValid()) {
        String key = string(iterator.key());
        if (key.startsWith(keyPrefix)) {
          db().delete(columnFamily, iterator.key());
        }
        iterator.next();
      }
    } catch (RocksDBException e) {
      throw new IllegalStateException("failed to clear column family", e);
    }
  }

  @Override
  public void close() {
    if (ownsStorage) {
      storage.close();
    }
  }

  protected RocksDB db() {
    return storage.runtimeDb();
  }

  protected ColumnFamilyHandle columnFamily() {
    return columnFamily;
  }

  private byte[] keyBytes(String key) {
    return bytes(keyPrefix + key);
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static String string(byte[] value) {
    return new String(value, StandardCharsets.UTF_8);
  }
}
