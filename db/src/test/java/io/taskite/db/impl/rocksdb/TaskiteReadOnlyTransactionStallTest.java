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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

final class TaskiteReadOnlyTransactionStallTest {
  @Test
  void readsAndScansCompleteWhileNativeWriteCommitIsStalled(@TempDir final Path tempDir)
      throws Exception {
    RocksDB.loadLibrary();
    try (Options options =
            new Options()
                .setCreateIfMissing(true)
                .setWriteBufferSize(65_536)
                .setMaxWriteBufferNumber(2);
        OptimisticTransactionDB db = OptimisticTransactionDB.open(options, tempDir.toString());
        // Fail promptly on a write stall so a regression cannot hang the test.
        WriteOptions writes = new WriteOptions().setNoSlowdown(true);
        ReadOptions reads = new ReadOptions();
        TaskiteTransaction transaction =
            new TaskiteTransaction(
                db.beginTransaction(writes), old -> db.beginTransaction(writes, old))) {
      final byte[] seed = new byte[] {1};
      db.put(writes, seed, new byte[] {2});
      db.pauseBackgroundWork();
      try {
        for (int i = 0; i < 2048 && !"1".equals(db.getProperty("rocksdb.is-write-stopped")); i++) {
          try {
            db.put(writes, new byte[] {3, (byte) i, (byte) (i >>> 8)}, new byte[4096]);
          } catch (final RocksDBException stalled) {
            assertEquals("1", db.getProperty("rocksdb.is-write-stopped"));
          }
        }
        assertEquals("1", db.getProperty("rocksdb.is-write-stopped"));
        try (Transaction nativeTransaction = db.beginTransaction(writes)) {
          assertThrows(RocksDBException.class, nativeTransaction::commit);
        }

        final var context = new DefaultTransactionContext(transaction);
        context.runInTransaction(
            () ->
                assertEquals(
                    2,
                    transaction
                        .get(
                            TaskiteTransactionDb.getNativeHandle(db.getDefaultColumnFamily()),
                            TaskiteTransactionDb.getNativeHandle(reads),
                            seed,
                            seed.length)[0]));
        context.runInTransaction(
            () -> {
              try (var iterator = transaction.newIterator(reads, db.getDefaultColumnFamily())) {
                iterator.seek(seed);
                assertEquals(2, iterator.value()[0]);
              }
            });
        context.getCurrentTransaction().commit();
        assertEquals("1", db.getProperty("rocksdb.is-write-stopped"));
      } finally {
        db.continueBackgroundWork();
      }
    }
  }
}
