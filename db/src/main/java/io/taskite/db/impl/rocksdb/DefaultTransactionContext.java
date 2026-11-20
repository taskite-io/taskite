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

import static io.taskite.db.impl.rocksdb.RocksDbInternal.RECOVERABLE_ERROR_CODES;

import io.taskite.db.DbException;
import io.taskite.db.DbTransaction;
import io.taskite.db.TransactionContext;
import io.taskite.db.TransactionOperation;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;

/** The default transaction context: reuses one {@link TaskiteTransaction}, nesting-aware. */
public final class DefaultTransactionContext implements TransactionContext {

  private final TaskiteTransaction transaction;
  private Thread ownerThread;

  DefaultTransactionContext(final TaskiteTransaction transaction) {
    this.transaction = transaction;
  }

  @Override
  public synchronized void runInTransaction(final TransactionOperation operations) {
    try {
      if (transaction.isInCurrentTransaction()) {
        ensureOwnedByCurrentThread();
        operations.run();
      } else {
        runInNewTransaction(operations);
      }
    } catch (final RuntimeException e) {
      throw e;
    } catch (final RocksDBException rdbex) {
      final String errorMessage = "Unexpected error occurred during RocksDB transaction.";
      if (isRocksDbExceptionRecoverable(rdbex)) {
        throw new DbException(errorMessage, rdbex);
      } else {
        throw new RuntimeException(errorMessage, rdbex);
      }
    } catch (final Exception ex) {
      throw new RuntimeException("Unexpected error occurred during db transaction operation.", ex);
    }
  }

  @Override
  public synchronized DbTransaction getCurrentTransaction() {
    if (!transaction.isInCurrentTransaction()) {
      beginTransaction();
    } else {
      ensureOwnedByCurrentThread();
    }
    return transaction;
  }

  private void runInNewTransaction(final TransactionOperation operations) throws Exception {
    beginTransaction();
    try {
      operations.run();
      transaction.commitInternal();
    } finally {
      try {
        transaction.rollbackInternal();
      } finally {
        ownerThread = null;
      }
    }
  }

  private void beginTransaction() {
    ownerThread = Thread.currentThread();
    try {
      transaction.resetTransaction();
    } catch (final RuntimeException | Error error) {
      ownerThread = null;
      throw error;
    }
  }

  private void ensureOwnedByCurrentThread() {
    final Thread currentThread = Thread.currentThread();
    if (ownerThread != currentThread) {
      throw new IllegalStateException(
          "Transaction context is owned by thread '%s' and cannot be accessed from thread '%s'"
              .formatted(threadName(ownerThread), threadName(currentThread)));
    }
  }

  private static String threadName(final Thread thread) {
    return thread == null ? "<unknown>" : thread.getName();
  }

  private boolean isRocksDbExceptionRecoverable(final RocksDBException rdbex) {
    final Status status = rdbex.getStatus();
    return RECOVERABLE_ERROR_CODES.contains(status.getCode());
  }
}
