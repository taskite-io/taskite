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
package io.taskite.state.processing;

import io.taskite.db.ColumnFamily;
import io.taskite.db.DbKey;
import io.taskite.db.DbTransaction;
import io.taskite.db.DbValue;
import io.taskite.db.TaskiteDb;
import io.taskite.db.TransactionContext;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.state.PartitionStorage;
import java.util.Objects;

/**
 * A {@link ProcessingStateStore} over a partition's transactional RocksDB. It owns the
 * transactional database and hands out typed column families to {@link
 * DbLastProcessedPositionState} (which models the last processed position) and to the engine's
 * {@code DbJobState}, exactly as those state models are built on this store. Business state, the
 * generated-key high-water and the last processed position all live in logical column families of
 * one physical database, so a command's whole effect commits atomically through a single optimistic
 * transaction owned by the caller (the stream processor).
 */
public final class StateStoreProcessingStore implements ProcessingStateStore {

  private final TaskiteDb<ColumnFamilies> db;
  private final TransactionContext transactionContext;
  private final DbLastProcessedPositionState lastProcessedPositionState;

  public StateStoreProcessingStore(final PartitionStorage storage) {
    Objects.requireNonNull(storage, "storage");
    db = storage.transactionalDb();
    transactionContext = db.createContext();
    lastProcessedPositionState = new DbLastProcessedPositionState(this);
  }

  @Override
  public <KeyType extends DbKey, ValueType extends DbValue>
      ColumnFamily<KeyType, ValueType> createColumnFamily(
          final ColumnFamilies columnFamily,
          final KeyType keyInstance,
          final ValueType valueInstance) {
    return db.createColumnFamily(columnFamily, transactionContext, keyInstance, valueInstance);
  }

  @Override
  public DbTransaction getCurrentTransaction() {
    return transactionContext.getCurrentTransaction();
  }

  @Override
  public long getLastProcessedPosition() {
    return lastProcessedPositionState.getLastProcessedPosition();
  }

  @Override
  public void markAsProcessed(final long position) {
    lastProcessedPositionState.markAsProcessed(position);
  }

  @Override
  public void close() {}
}
