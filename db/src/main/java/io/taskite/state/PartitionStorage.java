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

import io.taskite.db.TaskiteDb;
import io.taskite.db.impl.rocksdb.TaskiteTransactionDb;
import io.taskite.protocol.EnumValue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * Owns the RocksDB resources backing a single partition: a runtime database holding the business
 * state in the {@code default} column family. The database is an {@link OptimisticTransactionDB},
 * so callers can obtain a {@link TaskiteDb} transaction that commits atomically. jraft self-manages
 * the consensus log and metadata in its own storage ({@code raft-log/} and {@code raft-meta/}), so
 * the runtime store only carries business state. Snapshots are physical RocksDB checkpoints (see
 * {@link #createCheckpoint(Path)}) driven by jraft's snapshot machinery.
 */
public final class PartitionStorage implements AutoCloseable {

  static {
    RocksDB.loadLibrary();
  }

  private final DBOptions runtimeOptions;
  private final ColumnFamilyOptions defaultColumnFamilyOptions;
  private final OptimisticTransactionDB runtimeDb;
  private final ColumnFamilyHandle defaultColumnFamily;
  private final TaskiteTransactionDb<?> transactionDb;

  public PartitionStorage(final Path runtimeDirectory) {
    try {
      Files.createDirectories(runtimeDirectory);
      runtimeOptions =
          new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
      defaultColumnFamilyOptions = new ColumnFamilyOptions();
      final List<ColumnFamilyDescriptor> columnFamilies =
          List.of(
              new ColumnFamilyDescriptor(
                  RocksDB.DEFAULT_COLUMN_FAMILY, defaultColumnFamilyOptions));
      final List<ColumnFamilyHandle> handles = new ArrayList<>();
      runtimeDb =
          OptimisticTransactionDB.open(
              runtimeOptions, runtimeDirectory.toString(), columnFamilies, handles);
      defaultColumnFamily = handles.get(0);
      transactionDb = new TaskiteTransactionDb<>(runtimeDb, defaultColumnFamily);
    } catch (final Exception e) {
      throw new IllegalStateException("failed to open partition storage at " + runtimeDirectory, e);
    }
  }

  public RocksDB runtimeDb() {
    return runtimeDb;
  }

  /**
   * The transactional database facade over this partition's runtime store. Logical column families
   * share the physical {@code default} column family (distinguished by key prefix); a single
   * transaction commits atomically.
   */
  @SuppressWarnings("unchecked")
  public <ColumnFamilyType extends Enum<ColumnFamilyType> & EnumValue>
      TaskiteDb<ColumnFamilyType> transactionalDb() {
    return (TaskiteDb<ColumnFamilyType>) transactionDb;
  }

  /**
   * Creates a consistent physical checkpoint of the entire runtime database at {@code targetDir}.
   * The target must not already exist; RocksDB hard-links SST files when the target is on the same
   * filesystem, so this is cheap. Used to materialize a snapshot.
   */
  public void createCheckpoint(final Path targetDir) {
    try (final Checkpoint checkpoint = Checkpoint.create(runtimeDb)) {
      checkpoint.createCheckpoint(targetDir.toString());
    } catch (final RocksDBException e) {
      throw new IllegalStateException("failed to create checkpoint at " + targetDir, e);
    }
  }

  /**
   * Replaces the contents of the live runtime database with the data from a snapshot checkpoint
   * directory, in place (without reopening this store). Clears the runtime store, then copies every
   * key/value from the checkpoint's {@code default} column family. Used by a follower applying a
   * snapshot received from the leader.
   */
  public synchronized void importFromCheckpoint(final Path snapshotDir) {
    clearRuntime();
    try (final DBOptions options =
            new DBOptions().setCreateIfMissing(false).setCreateMissingColumnFamilies(false);
        final ColumnFamilyOptions defaultOptions = new ColumnFamilyOptions()) {
      final List<ColumnFamilyDescriptor> columnFamilies =
          List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultOptions));
      final List<ColumnFamilyHandle> handles = new ArrayList<>();
      RocksDB source = null;
      try {
        source = RocksDB.openReadOnly(options, snapshotDir.toString(), columnFamilies, handles);
        copyColumnFamily(source, handles.get(0), defaultColumnFamily);
      } finally {
        for (final ColumnFamilyHandle handle : handles) {
          handle.close();
        }
        if (source != null) {
          source.close();
        }
      }
    } catch (final RocksDBException e) {
      throw new IllegalStateException("failed to import checkpoint " + snapshotDir, e);
    }
  }

  private void copyColumnFamily(
      final RocksDB source, final ColumnFamilyHandle sourceCf, final ColumnFamilyHandle targetCf) {
    try (final RocksIterator iterator = source.newIterator(sourceCf)) {
      iterator.seekToFirst();
      while (iterator.isValid()) {
        runtimeDb.put(targetCf, iterator.key(), iterator.value());
        iterator.next();
      }
    } catch (final RocksDBException e) {
      throw new IllegalStateException("failed to copy column family from checkpoint", e);
    }
  }

  public ColumnFamilyHandle defaultColumnFamily() {
    return defaultColumnFamily;
  }

  public void clearRuntime() {
    clearColumnFamily(defaultColumnFamily);
  }

  @Override
  public synchronized void close() {
    transactionDb.close();
    defaultColumnFamily.close();
    runtimeDb.close();
    defaultColumnFamilyOptions.close();
    runtimeOptions.close();
  }

  private void clearColumnFamily(final ColumnFamilyHandle columnFamily) {
    final List<byte[]> keys = new ArrayList<>();
    try (final RocksIterator iterator = runtimeDb.newIterator(columnFamily)) {
      iterator.seekToFirst();
      while (iterator.isValid()) {
        keys.add(iterator.key().clone());
        iterator.next();
      }
    }
    try {
      for (final byte[] key : keys) {
        runtimeDb.delete(columnFamily, key);
      }
    } catch (final RocksDBException e) {
      throw new IllegalStateException("failed to clear partition runtime store", e);
    }
  }
}
