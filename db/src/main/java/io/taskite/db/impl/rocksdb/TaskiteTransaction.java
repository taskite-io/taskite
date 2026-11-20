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

import static io.taskite.db.impl.rocksdb.RocksDbInternal.isRocksDbExceptionRecoverable;

import io.taskite.db.DbException;
import io.taskite.db.DbTransaction;
import io.taskite.db.TransactionOperation;
import org.agrona.LangUtil;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;

/**
 * Wraps a RocksDB {@link Transaction}, exposing put/get/delete against an arbitrary column-family
 * native handle through {@link RocksDbInternal}, plus commit and rollback with recoverable-error
 * mapping.
 */
public final class TaskiteTransaction implements DbTransaction, AutoCloseable {

  private final long nativeHandle;
  private final TransactionRenovator transactionRenovator;

  private volatile boolean inCurrentTransaction;
  private Transaction transaction;
  private boolean hasWrites;

  TaskiteTransaction(
      final Transaction transaction, final TransactionRenovator transactionRenovator) {
    this.transactionRenovator = transactionRenovator;
    this.transaction = transaction;
    try {
      nativeHandle = RocksDbInternal.nativeHandle.getLong(transaction);
    } catch (final Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public void put(
      final long columnFamilyHandle,
      final byte[] key,
      final int keyLength,
      final byte[] value,
      final int valueLength)
      throws Exception {
    try {
      RocksDbInternal.putWithHandle.invokeExact(
          nativeHandle, key, 0, keyLength, value, 0, valueLength, columnFamilyHandle, false);
      hasWrites = true;
    } catch (final Throwable e) {
      LangUtil.rethrowUnchecked(e);
    }
  }

  public byte[] get(
      final long columnFamilyHandle,
      final long readOptionsHandle,
      final byte[] key,
      final int keyLength)
      throws Exception {
    try {
      return (byte[])
          RocksDbInternal.getWithHandle.invokeExact(
              nativeHandle, readOptionsHandle, key, 0, keyLength, columnFamilyHandle);
    } catch (final Throwable e) {
      LangUtil.rethrowUnchecked(e);
      return null; // unreachable
    }
  }

  public void delete(final long columnFamilyHandle, final byte[] key, final int keyLength)
      throws Exception {
    try {
      RocksDbInternal.removeWithHandle.invokeExact(
          nativeHandle, key, keyLength, columnFamilyHandle, false);
      hasWrites = true;
    } catch (final Throwable e) {
      LangUtil.rethrowUnchecked(e);
    }
  }

  public RocksIterator newIterator(final ReadOptions options, final ColumnFamilyHandle handle) {
    return transaction.getIterator(options, handle);
  }

  void resetTransaction() {
    transaction = transactionRenovator.renewTransaction(transaction);
    hasWrites = false;
    inCurrentTransaction = true;
  }

  boolean isInCurrentTransaction() {
    return inCurrentTransaction;
  }

  @Override
  public void run(final TransactionOperation operations) throws Exception {
    try {
      operations.run();
    } catch (final RocksDBException rdbex) {
      final String errorMessage = "Unexpected error occurred during RocksDB transaction.";
      if (isRocksDbExceptionRecoverable(rdbex)) {
        throw new DbException(errorMessage, rdbex);
      }
      throw rdbex;
    }
  }

  @Override
  public void commit() throws RocksDBException {
    try {
      commitInternal();
    } catch (final RocksDBException rdbex) {
      final String errorMessage = "Unexpected error occurred during RocksDB transaction commit.";
      if (isRocksDbExceptionRecoverable(rdbex)) {
        throw new DbException(errorMessage, rdbex);
      }
      throw rdbex;
    }
  }

  @Override
  public void rollback() throws RocksDBException {
    try {
      rollbackInternal();
    } catch (final RocksDBException rdbex) {
      final String errorMessage = "Unexpected error occurred during RocksDB transaction rollback.";
      if (isRocksDbExceptionRecoverable(rdbex)) {
        throw new DbException(errorMessage, rdbex);
      }
      throw rdbex;
    }
  }

  void commitInternal() throws RocksDBException {
    try {
      if (hasWrites) {
        transaction.commit();
      } else {
        // Even an empty native commit enters the write queue and writes a WAL record.
        // Ordinary get/iterator reads do not track conflicts or need a write commit.
        transaction.rollback();
      }
    } finally {
      inCurrentTransaction = false;
    }
  }

  void rollbackInternal() throws RocksDBException {
    try {
      transaction.rollback();
    } finally {
      inCurrentTransaction = false;
    }
  }

  @Override
  public void close() {
    transaction.close();
  }
}
