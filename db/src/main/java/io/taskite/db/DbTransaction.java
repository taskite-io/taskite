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
package io.taskite.db;

/**
 * A database transaction that can be committed or, on error, rolled back. Reads inside the
 * transaction reflect uncommitted writes made within it (read-your-writes).
 */
public interface DbTransaction {

  /**
   * Runs the given operations in the current transaction. Reads via get or iteration reflect
   * changes made earlier in the same transaction.
   */
  void run(TransactionOperation operations) throws Exception;

  /** Commits the transaction, writing all buffered changes into the database. */
  void commit() throws Exception;

  /** Rolls the transaction back to the latest commit, discarding all changes in between. */
  void rollback() throws Exception;
}
