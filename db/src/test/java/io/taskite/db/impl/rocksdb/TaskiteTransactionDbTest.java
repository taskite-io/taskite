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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.db.ColumnFamily;
import io.taskite.db.DbInconsistentException;
import io.taskite.db.DbKey;
import io.taskite.db.impl.DbConstants;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbString;
import io.taskite.protocol.EnumValue;
import io.taskite.state.PartitionStorage;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.OptimisticTransactionDB;

final class TaskiteTransactionDbTest {

  @TempDir Path tempDir;

  @Test
  void readsOwnWritesAndCommitsAcrossLogicalColumnFamilies() throws Exception {
    try (PartitionStorage storage = newStorage();
        TaskiteTransactionDb<TestColumnFamilies> db = newDb(storage)) {
      final var context = db.createContext();
      final var firstCf =
          db.createColumnFamily(TestColumnFamilies.DEFAULT, context, new DbLong(), new DbString());
      final var secondCf =
          db.createColumnFamily(
              TestColumnFamilies.DEFAULT_TWO, context, new DbString(), new DbLong());

      context.getCurrentTransaction();
      firstCf.upsert(dbLong(1), dbString("job-1"));
      secondCf.upsert(dbString("applied"), dbLong(42));

      assertEquals("job-1", firstCf.get(dbLong(1)).toString());
      assertEquals(42, secondCf.get(dbString("applied")).getValue());

      context.getCurrentTransaction().commit();

      final var readContext = db.createContext();
      final var reopenedFirst =
          db.createColumnFamily(
              TestColumnFamilies.DEFAULT, readContext, new DbLong(), new DbString());
      final var reopenedSecond =
          db.createColumnFamily(
              TestColumnFamilies.DEFAULT_TWO, readContext, new DbString(), new DbLong());

      assertEquals("job-1", reopenedFirst.get(dbLong(1)).toString());
      assertEquals(42, reopenedSecond.get(dbString("applied")).getValue());
    }
  }

  @Test
  void rollsBackOpenTransactionAcrossLogicalColumnFamilies() throws Exception {
    try (PartitionStorage storage = newStorage();
        TaskiteTransactionDb<TestColumnFamilies> db = newDb(storage)) {
      final var context = db.createContext();
      final var firstCf =
          db.createColumnFamily(TestColumnFamilies.DEFAULT, context, new DbLong(), new DbString());
      final var secondCf =
          db.createColumnFamily(
              TestColumnFamilies.DEFAULT_TWO, context, new DbString(), new DbLong());

      context.getCurrentTransaction();
      firstCf.upsert(dbLong(1), dbString("job-1"));
      secondCf.upsert(dbString("applied"), dbLong(42));
      assertTrue(firstCf.exists(dbLong(1)));
      assertTrue(secondCf.exists(dbString("applied")));

      context.getCurrentTransaction().rollback();

      assertFalse(firstCf.exists(dbLong(1)));
      assertFalse(secondCf.exists(dbString("applied")));
    }
  }

  @Test
  void enforcesInsertUpdateAndDeleteExistingPreconditions() {
    try (PartitionStorage storage = newStorage();
        TaskiteTransactionDb<TestColumnFamilies> db = newDb(storage)) {
      final var context = db.createContext();
      final var cf =
          db.createColumnFamily(TestColumnFamilies.DEFAULT, context, new DbLong(), new DbString());

      cf.insert(dbLong(1), dbString("one"));

      assertThrows(DbInconsistentException.class, () -> cf.insert(dbLong(1), dbString("again")));
      assertThrows(DbInconsistentException.class, () -> cf.update(dbLong(2), dbString("two")));
      assertThrows(DbInconsistentException.class, () -> cf.deleteExisting(dbLong(2)));

      cf.update(dbLong(1), dbString("updated"));
      assertEquals("updated", cf.get(dbLong(1)).toString());
      cf.deleteExisting(dbLong(1));
      assertNull(cf.get(dbLong(1)));
    }
  }

  @Test
  void iteratesAndCountsEntriesByPrefixFromStartKey() throws Exception {
    try (PartitionStorage storage = newStorage();
        TaskiteTransactionDb<TestColumnFamilies> db = newDb(storage)) {
      final var context = db.createContext();
      final ColumnFamily<TenantScopedKey, DbString> cf =
          db.createColumnFamily(
              TestColumnFamilies.DEFAULT, context, new TenantScopedKey(), new DbString());
      cf.upsert(new TenantScopedKey(1, 1), dbString("a"));
      cf.upsert(new TenantScopedKey(1, 2), dbString("b"));
      cf.upsert(new TenantScopedKey(1, 3), dbString("c"));
      cf.upsert(new TenantScopedKey(2, 1), dbString("d"));

      assertEquals(3, cf.countEqualPrefix(new TenantPrefixKey(1)));

      final List<Long> visited = new ArrayList<>();
      cf.whileEqualPrefix(
          new TenantPrefixKey(1),
          new TenantScopedKey(1, 2),
          (key, value) -> {
            visited.add(key.id());
            return true;
          });

      assertEquals(List.of(2L, 3L), visited);
      context.getCurrentTransaction().commit();
    }
  }

  @Test
  void commitsAfterIteratingAndThenWritingAnEmptyLogicalColumnFamily() throws Exception {
    try (PartitionStorage storage = newStorage();
        TaskiteTransactionDb<TestColumnFamilies> db = newDb(storage)) {
      final var context = db.createContext();
      final var written =
          db.createColumnFamily(TestColumnFamilies.DEFAULT, context, new DbLong(), new DbString());
      final var iterated =
          db.createColumnFamily(
              TestColumnFamilies.DEFAULT_TWO, context, new DbLong(), new DbString());

      context.getCurrentTransaction();
      written.upsert(dbLong(1), dbString("value"));
      iterated.forEach((key, value) -> {});
      iterated.upsert(dbLong(1), dbString("materialized"));

      context.getCurrentTransaction().commit();
    }
  }

  @Test
  void rejectsConcurrentAccessUntilExplicitTransactionCompletes() throws Exception {
    try (PartitionStorage storage = newStorage();
        TaskiteTransactionDb<TestColumnFamilies> db = newDb(storage)) {
      final var context = db.createContext();
      context.getCurrentTransaction();
      final AtomicReference<Throwable> concurrentFailure = new AtomicReference<>();

      final Thread concurrent =
          Thread.ofPlatform()
              .name("concurrent-db-reader")
              .start(
                  () -> {
                    try {
                      context.getCurrentTransaction();
                    } catch (final Throwable error) {
                      concurrentFailure.set(error);
                    }
                  });
      concurrent.join();

      final IllegalStateException error =
          assertInstanceOf(IllegalStateException.class, concurrentFailure.get());
      assertTrue(error.getMessage().contains("concurrent-db-reader"));

      context.getCurrentTransaction().rollback();
    }
  }

  @Test
  void allowsSequentialTransactionContextHandoffBetweenThreads() throws Exception {
    try (PartitionStorage storage = newStorage();
        TaskiteTransactionDb<TestColumnFamilies> db = newDb(storage)) {
      final var context = db.createContext();
      context.getCurrentTransaction().rollback();
      final AtomicReference<Throwable> handoffFailure = new AtomicReference<>();

      final Thread nextOwner =
          Thread.ofPlatform()
              .name("next-db-owner")
              .start(
                  () -> {
                    try {
                      context.getCurrentTransaction().rollback();
                    } catch (final Throwable error) {
                      handoffFailure.set(error);
                    }
                  });
      nextOwner.join();

      assertNull(handoffFailure.get());
    }
  }

  private PartitionStorage newStorage() {
    return new PartitionStorage(tempDir.resolve("runtime"));
  }

  private static TaskiteTransactionDb<TestColumnFamilies> newDb(final PartitionStorage storage) {
    return new TaskiteTransactionDb<>(
        (OptimisticTransactionDB) storage.runtimeDb(), storage.defaultColumnFamily());
  }

  private static DbLong dbLong(final long value) {
    final var key = new DbLong();
    key.wrapLong(value);
    return key;
  }

  private static DbString dbString(final String value) {
    final var string = new DbString();
    string.wrapString(value);
    return string;
  }

  private enum TestColumnFamilies implements EnumValue {
    DEFAULT(10),
    DEFAULT_TWO(20);

    private final int value;

    TestColumnFamilies(final int value) {
      this.value = value;
    }

    @Override
    public int getValue() {
      return value;
    }
  }

  private static final class TenantPrefixKey implements DbKey {
    private int tenant;

    TenantPrefixKey(final int tenant) {
      this.tenant = tenant;
    }

    @Override
    public void wrap(final DirectBuffer buffer, final int offset, final int length) {
      tenant = buffer.getInt(offset, DbConstants.DB_BYTE_ORDER);
    }

    @Override
    public int getLength() {
      return Integer.BYTES;
    }

    @Override
    public void write(final MutableDirectBuffer buffer, final int offset) {
      buffer.putInt(offset, tenant, DbConstants.DB_BYTE_ORDER);
    }
  }

  private static final class TenantScopedKey implements DbKey {
    private int tenant;
    private long id;

    TenantScopedKey() {}

    TenantScopedKey(final int tenant, final long id) {
      this.tenant = tenant;
      this.id = id;
    }

    long id() {
      return id;
    }

    @Override
    public void wrap(final DirectBuffer buffer, final int offset, final int length) {
      tenant = buffer.getInt(offset, DbConstants.DB_BYTE_ORDER);
      id = buffer.getLong(offset + Integer.BYTES, DbConstants.DB_BYTE_ORDER);
    }

    @Override
    public int getLength() {
      return Integer.BYTES + Long.BYTES;
    }

    @Override
    public void write(final MutableDirectBuffer buffer, final int offset) {
      buffer.putInt(offset, tenant, DbConstants.DB_BYTE_ORDER);
      buffer.putLong(offset + Integer.BYTES, id, DbConstants.DB_BYTE_ORDER);
    }

    @Override
    public String toString() {
      return "TenantScopedKey{" + "tenant=" + tenant + ", id=" + id + '}';
    }
  }
}
