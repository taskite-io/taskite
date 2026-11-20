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

import io.taskite.db.ConsistencyChecksSettings;
import io.taskite.db.ContainsForeignKeys;
import io.taskite.db.DbInconsistentException;
import io.taskite.db.DbKey;
import io.taskite.db.impl.DbConstants;
import io.taskite.db.impl.DbForeignKey;
import io.taskite.util.BufferUtil;
import java.nio.ByteBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.rocksdb.ColumnFamilyHandle;

/**
 * Verifies that a referenced foreign key exists before a write. Unlike a bound {@link
 * TransactionalColumnFamily}, it can inspect any logical column family by prefixing the referenced
 * key with the explicit column-family value used by {@link ColumnFamilyContext}.
 */
final class ForeignKeyChecker {
  private final TaskiteTransactionDb<?> transactionDb;
  private final ExpandableArrayBuffer keyBuffer = new ExpandableArrayBuffer();
  private final boolean enabled;

  ForeignKeyChecker(
      final TaskiteTransactionDb<?> transactionDb, final ConsistencyChecksSettings settings) {
    this.transactionDb = transactionDb;
    enabled = settings.enableForeignKeyChecks();
  }

  void assertExists(
      final TaskiteTransaction transaction, final ContainsForeignKeys containsForeignKey)
      throws Exception {
    if (!enabled) {
      return;
    }
    for (final var fk : containsForeignKey.containedForeignKeys()) {
      assertForeignKeyExists(transaction, fk);
    }
  }

  private void assertForeignKeyExists(
      final TaskiteTransaction transaction, final DbForeignKey<DbKey> foreignKey) throws Exception {
    if (foreignKey.shouldSkipCheck()) {
      return;
    }

    final ColumnFamilyHandle handle = transactionDb.defaultHandle();
    keyBuffer.putLong(0, foreignKey.columnFamily().getValue(), DbConstants.DB_BYTE_ORDER);
    foreignKey.write(keyBuffer, Long.BYTES);
    final var keyLength = Long.BYTES + foreignKey.getLength();

    switch (foreignKey.match()) {
      case Full ->
          assertKeyExists(transaction, handle, foreignKey, keyBuffer.byteArray(), keyLength);
      case Prefix -> assertPrefixExists(handle, foreignKey, keyBuffer.byteArray(), keyLength);
      default ->
          throw new IllegalStateException("Unknown foreign key match type: " + foreignKey.match());
    }
  }

  private void assertKeyExists(
      final TaskiteTransaction transaction,
      final ColumnFamilyHandle handle,
      final DbForeignKey<? extends DbKey> foreignKey,
      final byte[] key,
      final int keyLength)
      throws Exception {
    final var exists =
        transaction.get(
                TaskiteTransactionDb.getNativeHandle(handle),
                transactionDb.getReadOptionsNativeHandle(),
                key,
                keyLength)
            != null;
    if (!exists) {
      throw new DbInconsistentException(
          "Foreign key " + foreignKey.inner() + " does not exist in " + foreignKey.columnFamily());
    }
  }

  private void assertPrefixExists(
      final ColumnFamilyHandle handle,
      final DbForeignKey<? extends DbKey> foreignKey,
      final byte[] prefix,
      final int prefixLength) {
    try (final var iterator =
        transactionDb.rocksDb().newIterator(handle, transactionDb.getPrefixReadOptions())) {
      final ByteBuffer bufferView = ByteBuffer.wrap(prefix, 0, prefixLength);
      iterator.seek(bufferView);
      boolean exists = false;
      if (iterator.isValid()) {
        final byte[] keyBytes = iterator.key();
        exists = BufferUtil.startsWith(prefix, 0, prefixLength, keyBytes, 0, keyBytes.length);
      }
      if (!exists) {
        throw new DbInconsistentException(
            "Foreign key "
                + foreignKey.inner()
                + " does not exist as prefix in "
                + foreignKey.columnFamily());
      }
    }
  }
}
