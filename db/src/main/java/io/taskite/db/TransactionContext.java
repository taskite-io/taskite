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
 * The context used to interact with the database inside a transaction. Access to different column
 * families within one transaction is possible, and reads reflect changes made during the
 * transaction.
 */
public interface TransactionContext {

  /**
   * Runs the given operations in a transaction. Nested calls reuse the same transaction: the
   * outermost call commits on success and rolls back on error.
   */
  void runInTransaction(TransactionOperation operations);

  /**
   * Returns the current transaction, creating (renewing) one if none is open. The caller decides
   * when to commit or roll back.
   */
  DbTransaction getCurrentTransaction();
}
