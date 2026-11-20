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
package io.taskite.db.impl.rocksdb;

import io.taskite.db.ColumnFamily;
import io.taskite.db.ConsistencyChecksSettings;
import io.taskite.db.DbException;
import io.taskite.db.DbKey;
import io.taskite.db.DbValue;
import io.taskite.db.TaskiteDb;
import io.taskite.db.TransactionContext;
import io.taskite.db.impl.DbNil;
import io.taskite.db.impl.DbNullKey;
import io.taskite.protocol.EnumValue;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksObject;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

/**
 * A transactional key/value database over a RocksDB {@link OptimisticTransactionDB}. Logical column
 * families share the physical {@code default} column family and are distinguished by explicit
 * numeric key prefixes. A single optimistic transaction therefore commits changes across all
 * logical column families atomically.
 */
public final class TaskiteTransactionDb<ColumnFamilyType extends Enum<ColumnFamilyType> & EnumValue>
    implements TaskiteDb<ColumnFamilyType>, TransactionRenovator {

  private final OptimisticTransactionDB db;
  private final ColumnFamilyHandle defaultHandle;
  private final ConsistencyChecksSettings consistencyChecksSettings;
  private final ReadOptions prefixReadOptions;
  private final ReadOptions defaultReadOptions;
  private final WriteOptions defaultWriteOptions;
  private final List<TaskiteTransaction> openTransactions;

  public TaskiteTransactionDb(
      final OptimisticTransactionDB db, final ColumnFamilyHandle defaultHandle) {
    this(db, defaultHandle, new ConsistencyChecksSettings(true, false));
  }

  public TaskiteTransactionDb(
      final OptimisticTransactionDB db,
      final ColumnFamilyHandle defaultHandle,
      final ConsistencyChecksSettings consistencyChecksSettings) {
    this.db = db;
    this.defaultHandle = defaultHandle;
    this.consistencyChecksSettings = consistencyChecksSettings;
    prefixReadOptions =
        new ReadOptions().setPrefixSameAsStart(true).setTotalOrderSeek(false).setReadaheadSize(0);
    defaultReadOptions = new ReadOptions();
    defaultWriteOptions = new WriteOptions();
    openTransactions = Collections.synchronizedList(new ArrayList<>());
  }

  static long getNativeHandle(final RocksObject object) {
    try {
      return RocksDbInternal.nativeHandle.getLong(object);
    } catch (final IllegalAccessException e) {
      throw new RuntimeException(
          "Unexpected error occurred trying to access private nativeHandle_ field", e);
    }
  }

  ReadOptions getPrefixReadOptions() {
    return prefixReadOptions;
  }

  long getReadOptionsNativeHandle() {
    return getNativeHandle(defaultReadOptions);
  }

  @Override
  public <KeyType extends DbKey, ValueType extends DbValue>
      ColumnFamily<KeyType, ValueType> createColumnFamily(
          final ColumnFamilyType columnFamily,
          final TransactionContext context,
          final KeyType keyInstance,
          final ValueType valueInstance) {
    return new TransactionalColumnFamily<>(
        this,
        consistencyChecksSettings,
        columnFamily,
        context,
        keyInstance,
        valueInstance,
        defaultHandle,
        getNativeHandle(defaultHandle));
  }

  @Override
  public void createSnapshot(final File snapshotDir) {
    try (final Checkpoint checkpoint = Checkpoint.create(db)) {
      checkpoint.createCheckpoint(snapshotDir.getAbsolutePath());
    } catch (final RocksDBException e) {
      throw new DbException("Failed to take snapshot in path " + snapshotDir, e);
    }
  }

  @Override
  public Optional<String> getProperty(final String propertyName) {
    try {
      return Optional.ofNullable(db.getProperty(defaultHandle, propertyName));
    } catch (final RocksDBException e) {
      return Optional.empty();
    }
  }

  @Override
  public TransactionContext createContext() {
    final Transaction transaction = db.beginTransaction(defaultWriteOptions);
    final TaskiteTransaction taskiteTransaction = new TaskiteTransaction(transaction, this);
    openTransactions.add(taskiteTransaction);
    return new DefaultTransactionContext(taskiteTransaction);
  }

  @Override
  public boolean isEmpty(final ColumnFamilyType column, final TransactionContext context) {
    return createColumnFamily(column, context, DbNullKey.INSTANCE, DbNil.INSTANCE).isEmpty();
  }

  @Override
  public void close() {
    synchronized (openTransactions) {
      openTransactions.forEach(TaskiteTransaction::close);
      openTransactions.clear();
    }
    prefixReadOptions.close();
    defaultReadOptions.close();
    defaultWriteOptions.close();
  }

  ColumnFamilyHandle defaultHandle() {
    return defaultHandle;
  }

  @Override
  public Transaction renewTransaction(final Transaction oldTransaction) {
    return db.beginTransaction(defaultWriteOptions, oldTransaction);
  }

  /** The underlying RocksDB, for physical maintenance (checkpoint import, clear) by the owner. */
  public OptimisticTransactionDB rocksDb() {
    return db;
  }
}
