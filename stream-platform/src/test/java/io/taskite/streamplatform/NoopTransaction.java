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
package io.taskite.streamplatform;

import io.taskite.db.DbTransaction;
import io.taskite.db.TransactionOperation;

/**
 * A no-op {@link DbTransaction} for in-memory test stores that back their state with plain maps:
 * the {@code StreamProcessor}'s transaction ownership is exercised against the real RocksDB-backed
 * store elsewhere, so these unit tests only need a transaction object that runs operations inline
 * and treats commit/rollback as no-ops.
 */
public final class NoopTransaction implements DbTransaction {

  public static final NoopTransaction INSTANCE = new NoopTransaction();

  private NoopTransaction() {}

  @Override
  public void run(final TransactionOperation operations) throws Exception {
    operations.run();
  }

  @Override
  public void commit() {}

  @Override
  public void rollback() {}
}
