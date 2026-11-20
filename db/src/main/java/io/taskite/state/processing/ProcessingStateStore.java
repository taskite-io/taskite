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
import io.taskite.protocol.ColumnFamilies;

/**
 * The processing state facade consumed by the engine: typed column families over the business
 * state, plus the last processed source position. This is <em>not</em> the replicated log; it is
 * the derived RocksDB state the {@code StreamProcessor} reads and mutates while applying records.
 * The replicated append/read log is owned separately by Stream Platform.
 *
 * <p>The stream processor owns transaction boundaries. It opens a transaction through {@link
 * #getCurrentTransaction()}, applies the complete command effect (business state, generated-key
 * high-water and, through {@link #markAsProcessed(long)}, the processed position), and then commits
 * or rolls back. Reads inside the transaction see its own uncommitted writes (read-your-writes) and
 * the whole apply commits atomically.
 */
public interface ProcessingStateStore extends AutoCloseable {

  /** Creates a typed column family bound to the store's transaction context. */
  <KeyType extends DbKey, ValueType extends DbValue>
      ColumnFamily<KeyType, ValueType> createColumnFamily(
          ColumnFamilies columnFamily, KeyType keyInstance, ValueType valueInstance);

  /**
   * Returns the store's current transaction, opening (renewing) one if none is in progress. The
   * caller owns committing or rolling it back.
   */
  DbTransaction getCurrentTransaction();

  long getLastProcessedPosition();

  /**
   * Records {@code position} as the last processed source position, as a plain write into the
   * currently open transaction. It commits together with state changes when the caller commits.
   */
  void markAsProcessed(long position);

  @Override
  void close();
}
