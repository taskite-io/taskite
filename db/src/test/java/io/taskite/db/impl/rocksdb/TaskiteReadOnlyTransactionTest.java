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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.db.ColumnFamily;
import io.taskite.db.DbException;
import io.taskite.db.TransactionContext;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbString;
import io.taskite.protocol.EnumValue;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;

final class TaskiteReadOnlyTransactionTest {
  @TempDir Path tempDir;

  private Statistics statistics;
  private Options options;
  private OptimisticTransactionDB rocksDb;
  private TaskiteTransactionDb<TestColumnFamilies> db;
  private TransactionContext context;
  private ColumnFamily<DbLong, DbString> firstColumnFamilies;
  private ColumnFamily<DbLong, DbString> secondColumnFamilies;

  @BeforeEach
  void openDatabase() throws Exception {
    RocksDB.loadLibrary();
    statistics = new Statistics();
    options = new Options().setCreateIfMissing(true).setStatistics(statistics);
    rocksDb = OptimisticTransactionDB.open(options, tempDir.resolve("runtime").toString());
    db = new TaskiteTransactionDb<>(rocksDb, rocksDb.getDefaultColumnFamily());
    context = db.createContext();
    firstColumnFamilies = column(TestColumnFamilies.FIRST, context);
    secondColumnFamilies = column(TestColumnFamilies.SECOND, context);
    firstColumnFamilies.upsert(key(1), value("one"));
    statistics.reset();
  }

  @AfterEach
  void closeDatabase() {
    if (db != null) {
      db.close();
    }
    if (rocksDb != null) {
      rocksDb.close();
    }
    if (options != null) {
      options.close();
    }
    if (statistics != null) {
      statistics.close();
    }
  }

  @ParameterizedTest
  @EnumSource(ReadOperation.class)
  void readOnlyOperationsDoNotEnterWriteQueueOrWriteWal(final ReadOperation operation)
      throws Exception {
    switch (operation) {
      case EMPTY -> context.runInTransaction(() -> {});
      case GET -> assertEquals("one", firstColumnFamilies.get(key(1)).toString());
      case MISSING_GET -> assertNull(firstColumnFamilies.get(key(2)));
      case EXISTS -> assertTrue(firstColumnFamilies.exists(key(1)));
      case SCAN -> {
        final List<String> values = new ArrayList<>();
        firstColumnFamilies.forEach((key, value) -> values.add(value.toString()));
        assertEquals(List.of("one"), values);
      }
      case COUNT -> assertEquals(1, firstColumnFamilies.count());
      case EMPTY_COLUMN -> assertTrue(secondColumnFamilies.isEmpty());
      case NESTED ->
          context.runInTransaction(
              () ->
                  context.runInTransaction(
                      () -> assertEquals("one", firstColumnFamilies.get(key(1)).toString())));
      case EXPLICIT -> {
        final var transaction = context.getCurrentTransaction();
        assertEquals("one", firstColumnFamilies.get(key(1)).toString());
        transaction.commit();
      }
    }
    assertNoWrites();

    // A later mutation must still commit when the same native transaction is reused.
    secondColumnFamilies.upsert(key(2), value("two"));
    assertEquals(1, writes());
    assertEquals(
        "two", column(TestColumnFamilies.SECOND, db.createContext()).get(key(2)).toString());
    assertEquals(1, writes());
  }

  @Test
  void commitsMixedReadsPutsAndDeletesAcrossColumnsAsOneWrite() {
    context.runInTransaction(
        () -> {
          assertEquals("one", firstColumnFamilies.get(key(1)).toString());
          firstColumnFamilies.deleteExisting(key(1));
          secondColumnFamilies.upsert(key(2), value("two"));
          context.runInTransaction(
              () -> assertEquals("two", secondColumnFamilies.get(key(2)).toString()));
        });

    assertEquals(1, writes());
    final var reader = db.createContext();
    assertNull(column(TestColumnFamilies.FIRST, reader).get(key(1)));
    assertEquals("two", column(TestColumnFamilies.SECOND, reader).get(key(2)).toString());
    assertEquals(1, writes());
  }

  @Test
  void deleteOnlyTransactionStillCommits() {
    firstColumnFamilies.deleteExisting(key(1));
    assertEquals(1, writes());
    assertNull(column(TestColumnFamilies.FIRST, db.createContext()).get(key(1)));
    assertEquals(1, writes());
  }

  @Test
  void failedWriteRollsBackAndDoesNotMakeNextReadWrite() {
    assertThrows(
        IllegalStateException.class,
        () ->
            context.runInTransaction(
                () -> {
                  firstColumnFamilies.deleteExisting(key(1));
                  secondColumnFamilies.upsert(key(2), value("discarded"));
                  throw new IllegalStateException("abort");
                }));

    assertEquals("one", firstColumnFamilies.get(key(1)).toString());
    assertNull(secondColumnFamilies.get(key(2)));
    assertNoWrites();
    secondColumnFamilies.upsert(key(3), value("committed"));
    assertEquals(1, writes());
  }

  @Test
  void explicitRollbackResetsWriteTrackingBeforeEmptyCommit() throws Exception {
    final var transaction = context.getCurrentTransaction();
    firstColumnFamilies.upsert(key(2), value("discarded"));
    transaction.rollback();
    context.getCurrentTransaction().commit();
    assertNull(firstColumnFamilies.get(key(2)));
    assertNoWrites();
  }

  @Test
  void conflictingWritesStillFailAtomicallyAndContextCanBeReused() throws Exception {
    final var conflictingContext = db.createContext();
    final var competing = column(TestColumnFamilies.FIRST, conflictingContext);
    final var transaction = context.getCurrentTransaction();
    firstColumnFamilies.upsert(key(1), value("loser"));
    secondColumnFamilies.upsert(key(2), value("must-not-leak"));
    competing.upsert(key(1), value("winner"));

    assertThrows(DbException.class, transaction::commit);
    transaction.rollback();
    statistics.reset();
    assertEquals("winner", firstColumnFamilies.get(key(1)).toString());
    assertFalse(secondColumnFamilies.exists(key(2)));
    assertNoWrites();
    secondColumnFamilies.upsert(key(3), value("after-conflict"));
    assertEquals(1, writes());
  }

  private long writes() {
    return statistics.getTickerCount(TickerType.WRITE_DONE_BY_SELF)
        + statistics.getTickerCount(TickerType.WRITE_DONE_BY_OTHER);
  }

  private void assertNoWrites() {
    assertEquals(0, writes(), "Read-only transactions must not join the RocksDB write queue");
    assertEquals(0, statistics.getTickerCount(TickerType.WAL_FILE_BYTES));
  }

  private ColumnFamily<DbLong, DbString> column(
      final TestColumnFamilies family, final TransactionContext transactionContext) {
    return db.createColumnFamily(family, transactionContext, new DbLong(), new DbString());
  }

  private static DbLong key(final long number) {
    final var key = new DbLong();
    key.wrapLong(number);
    return key;
  }

  private static DbString value(final String text) {
    final var value = new DbString();
    value.wrapString(text);
    return value;
  }

  private enum ReadOperation {
    EMPTY,
    GET,
    MISSING_GET,
    EXISTS,
    SCAN,
    COUNT,
    EMPTY_COLUMN,
    NESTED,
    EXPLICIT
  }

  private enum TestColumnFamilies implements EnumValue {
    FIRST,
    SECOND;

    @Override
    public int getValue() {
      return ordinal();
    }
  }
}
