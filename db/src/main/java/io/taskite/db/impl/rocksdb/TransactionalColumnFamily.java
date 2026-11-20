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

import static io.taskite.util.BufferUtil.startsWith;

import io.taskite.db.ColumnFamily;
import io.taskite.db.ConsistencyChecksSettings;
import io.taskite.db.ContainsForeignKeys;
import io.taskite.db.DbInconsistentException;
import io.taskite.db.DbKey;
import io.taskite.db.DbValue;
import io.taskite.db.KeyValuePairVisitor;
import io.taskite.db.TransactionContext;
import io.taskite.db.impl.DbNullKey;
import io.taskite.protocol.EnumValue;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.agrona.DirectBuffer;
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.UnsafeBuffer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksIterator;

/**
 * A typed column family backed by an optimistic RocksDB transaction. All access runs through the
 * owning {@link TransactionContext}, so reads reflect uncommitted writes made earlier in the same
 * transaction. Keys are namespaced by the logical column-family prefix within a single physical
 * RocksDB column family. Physical-family routing, write preconditions, and foreign-key checks are
 * enforced before data reaches the native transaction.
 */
final class TransactionalColumnFamily<
        ColumnFamilyNames extends Enum<ColumnFamilyNames> & EnumValue,
        KeyType extends DbKey,
        ValueType extends DbValue>
    implements ColumnFamily<KeyType, ValueType> {

  private final TaskiteTransactionDb<ColumnFamilyNames> transactionDb;
  private final ConsistencyChecksSettings consistencyChecksSettings;
  private final ColumnFamilyNames columnFamily;
  private final TransactionContext context;
  private final ValueType valueInstance;
  private final KeyType keyInstance;
  private final ColumnFamilyContext columnFamilyContext;
  private final ForeignKeyChecker foreignKeyChecker;
  private final ColumnFamilyHandle physicalHandle;
  private final long physicalNativeHandle;

  TransactionalColumnFamily(
      final TaskiteTransactionDb<ColumnFamilyNames> transactionDb,
      final ConsistencyChecksSettings consistencyChecksSettings,
      final ColumnFamilyNames columnFamily,
      final TransactionContext context,
      final KeyType keyInstance,
      final ValueType valueInstance,
      final ColumnFamilyHandle physicalHandle,
      final long physicalNativeHandle) {
    this.transactionDb = transactionDb;
    this.consistencyChecksSettings = consistencyChecksSettings;
    this.columnFamily = columnFamily;
    this.context = context;
    this.keyInstance = keyInstance;
    this.valueInstance = valueInstance;
    this.physicalHandle = physicalHandle;
    this.physicalNativeHandle = physicalNativeHandle;
    columnFamilyContext = new ColumnFamilyContext(columnFamily.getValue());
    foreignKeyChecker = new ForeignKeyChecker(transactionDb, consistencyChecksSettings);
  }

  @Override
  public void insert(final KeyType key, final ValueType value) {
    ensureInOpenTransaction(
        transaction -> {
          columnFamilyContext.writeKey(key);
          columnFamilyContext.writeValue(value);
          assertKeyDoesNotExist(transaction, key);
          assertForeignKeysExist(transaction, key, value);
          put(transaction, value);
        });
  }

  @Override
  public void update(final KeyType key, final ValueType value) {
    ensureInOpenTransaction(
        transaction -> {
          columnFamilyContext.writeKey(key);
          columnFamilyContext.writeValue(value);
          assertKeyExists(transaction, key);
          assertForeignKeysExist(transaction, key, value);
          put(transaction, value);
        });
  }

  @Override
  public void upsert(final KeyType key, final ValueType value) {
    ensureInOpenTransaction(
        transaction -> {
          columnFamilyContext.writeKey(key);
          columnFamilyContext.writeValue(value);
          assertForeignKeysExist(transaction, key, value);
          put(transaction, value);
        });
  }

  @Override
  public ValueType get(final KeyType key) {
    ensureInOpenTransaction(
        transaction -> {
          columnFamilyContext.writeKey(key);
          final byte[] value = getValue(transaction);
          columnFamilyContext.wrapValueView(value);
        });
    final var valueBuffer = columnFamilyContext.getValueView();
    if (valueBuffer != null) {
      valueInstance.wrap(valueBuffer, 0, valueBuffer.capacity());
      return valueInstance;
    }
    return null;
  }

  @Override
  public ValueType get(final KeyType key, final Supplier<ValueType> valueSupplier) {
    final var result = new MutableReference<ValueType>();
    ensureInOpenTransaction(
        transaction -> {
          columnFamilyContext.writeKey(key);
          final byte[] valueBytes = getValue(transaction);
          if (valueBytes != null) {
            final var newValue = valueSupplier.get();
            newValue.wrap(new UnsafeBuffer(valueBytes), 0, valueBytes.length);
            result.set(newValue);
          }
        });
    return result.get();
  }

  @Override
  public void forEach(final Consumer<ValueType> consumer) {
    ensureInOpenTransaction(
        transaction ->
            forEachInPrefix(
                DbNullKey.INSTANCE,
                (k, v) -> {
                  consumer.accept(v);
                  return true;
                }));
  }

  @Override
  public void forEach(final BiConsumer<KeyType, ValueType> consumer) {
    ensureInOpenTransaction(
        transaction ->
            forEachInPrefix(
                DbNullKey.INSTANCE,
                (k, v) -> {
                  consumer.accept(k, v);
                  return true;
                }));
  }

  @Override
  public void whileTrue(final KeyValuePairVisitor<KeyType, ValueType> visitor) {
    ensureInOpenTransaction(transaction -> forEachInPrefix(DbNullKey.INSTANCE, visitor));
  }

  @Override
  public void whileTrue(
      final KeyType startAtKey, final KeyValuePairVisitor<KeyType, ValueType> visitor) {
    ensureInOpenTransaction(
        transaction -> forEachInPrefix(startAtKey, DbNullKey.INSTANCE, visitor));
  }

  @Override
  public void whileEqualPrefix(
      final DbKey keyPrefix, final BiConsumer<KeyType, ValueType> visitor) {
    ensureInOpenTransaction(
        transaction ->
            forEachInPrefix(
                keyPrefix,
                (k, v) -> {
                  visitor.accept(k, v);
                  return true;
                }));
  }

  @Override
  public void whileEqualPrefix(
      final DbKey keyPrefix, final KeyValuePairVisitor<KeyType, ValueType> visitor) {
    ensureInOpenTransaction(transaction -> forEachInPrefix(keyPrefix, visitor));
  }

  @Override
  public void whileEqualPrefix(
      final DbKey keyPrefix,
      final KeyType startAtKey,
      final KeyValuePairVisitor<KeyType, ValueType> visitor) {
    ensureInOpenTransaction(transaction -> forEachInPrefix(startAtKey, keyPrefix, visitor));
  }

  @Override
  public void deleteExisting(final KeyType key) {
    ensureInOpenTransaction(
        transaction -> {
          columnFamilyContext.writeKey(key);
          assertKeyExists(transaction, key);
          transaction.delete(
              physicalNativeHandle,
              columnFamilyContext.getKeyBufferArray(),
              columnFamilyContext.getKeyLength());
        });
  }

  @Override
  public void deleteIfExists(final KeyType key) {
    ensureInOpenTransaction(
        transaction -> {
          columnFamilyContext.writeKey(key);
          transaction.delete(
              physicalNativeHandle,
              columnFamilyContext.getKeyBufferArray(),
              columnFamilyContext.getKeyLength());
        });
  }

  @Override
  public boolean exists(final KeyType key) {
    ensureInOpenTransaction(
        transaction -> {
          columnFamilyContext.writeKey(key);
          columnFamilyContext.wrapValueView(getValue(transaction));
        });
    return !columnFamilyContext.isValueViewEmpty();
  }

  @Override
  public boolean isEmpty() {
    final AtomicBoolean isEmpty = new AtomicBoolean(true);
    ensureInOpenTransaction(
        transaction ->
            forEachInPrefix(
                DbNullKey.INSTANCE,
                (key, value) -> {
                  isEmpty.set(false);
                  return false;
                }));
    return isEmpty.get();
  }

  @Override
  public long count() {
    return countEachInPrefix(DbNullKey.INSTANCE);
  }

  @Override
  public long countEqualPrefix(final DbKey prefix) {
    return countEachInPrefix(prefix);
  }

  private void put(final TaskiteTransaction transaction, final ValueType value) throws Exception {
    transaction.put(
        physicalNativeHandle,
        columnFamilyContext.getKeyBufferArray(),
        columnFamilyContext.getKeyLength(),
        columnFamilyContext.getValueBufferArray(),
        value.getLength());
  }

  private byte[] getValue(final TaskiteTransaction transaction) throws Exception {
    return transaction.get(
        physicalNativeHandle,
        transactionDb.getReadOptionsNativeHandle(),
        columnFamilyContext.getKeyBufferArray(),
        columnFamilyContext.getKeyLength());
  }

  private void assertForeignKeysExist(final TaskiteTransaction transaction, final Object... keys)
      throws Exception {
    if (!consistencyChecksSettings.enableForeignKeyChecks()) {
      return;
    }
    for (final var key : keys) {
      if (key instanceof final ContainsForeignKeys containsForeignKey) {
        foreignKeyChecker.assertExists(transaction, containsForeignKey);
      }
    }
  }

  private void assertKeyDoesNotExist(final TaskiteTransaction transaction, final KeyType key)
      throws Exception {
    if (!consistencyChecksSettings.enablePreconditions()) {
      return;
    }
    if (getValue(transaction) != null) {
      throw new DbInconsistentException(
          "Key " + key + " in ColumnFamily " + columnFamily + " already exists");
    }
  }

  private void assertKeyExists(final TaskiteTransaction transaction, final KeyType key)
      throws Exception {
    if (!consistencyChecksSettings.enablePreconditions()) {
      return;
    }
    if (getValue(transaction) == null) {
      throw new DbInconsistentException(
          "Key " + key + " in ColumnFamily " + columnFamily + " does not exist");
    }
  }

  private void ensureInOpenTransaction(final TransactionConsumer operation) {
    context.runInTransaction(
        () -> operation.run((TaskiteTransaction) context.getCurrentTransaction()));
  }

  private RocksIterator newIterator() {
    final var currentTransaction = (TaskiteTransaction) context.getCurrentTransaction();
    return currentTransaction.newIterator(transactionDb.getPrefixReadOptions(), physicalHandle);
  }

  private void forEachInPrefix(
      final DbKey prefix, final KeyValuePairVisitor<KeyType, ValueType> visitor) {
    forEachInPrefix(prefix, prefix, visitor);
  }

  private void forEachInPrefix(
      final DbKey startAt,
      final DbKey prefix,
      final KeyValuePairVisitor<KeyType, ValueType> visitor) {
    final var seekTarget = Objects.requireNonNullElse(startAt, prefix);
    Objects.requireNonNull(prefix);
    Objects.requireNonNull(visitor);

    columnFamilyContext.withPrefixKey(
        prefix,
        (prefixKey, prefixLength) -> {
          try (final RocksIterator iterator = newIterator()) {
            boolean shouldVisitNext = true;
            for (iterator.seek(columnFamilyContext.keyWithColumnFamily(seekTarget));
                iterator.isValid() && shouldVisitNext;
                iterator.next()) {
              final byte[] keyBytes = iterator.key();
              if (!startsWith(prefixKey, 0, prefixLength, keyBytes, 0, keyBytes.length)) {
                break;
              }
              shouldVisitNext = visit(visitor, iterator);
            }
          }
        });
  }

  private long countEachInPrefix(final DbKey prefix) {
    final var seekTarget = Objects.requireNonNull(prefix);
    final var count = new AtomicLong(0);
    ensureInOpenTransaction(
        transaction ->
            columnFamilyContext.withPrefixKey(
                prefix,
                (prefixKey, prefixLength) -> {
                  try (final RocksIterator iterator = newIterator()) {
                    for (iterator.seek(columnFamilyContext.keyWithColumnFamily(seekTarget));
                        iterator.isValid();
                        iterator.next()) {
                      final byte[] keyBytes = iterator.key();
                      if (!startsWith(prefixKey, 0, prefixLength, keyBytes, 0, keyBytes.length)) {
                        break;
                      }
                      count.getAndIncrement();
                    }
                  }
                }));
    return count.get();
  }

  private boolean visit(
      final KeyValuePairVisitor<KeyType, ValueType> visitor, final RocksIterator iterator) {
    columnFamilyContext.wrapKeyView(iterator.key());
    columnFamilyContext.wrapValueView(iterator.value());

    final DirectBuffer keyViewBuffer = columnFamilyContext.getKeyView();
    keyInstance.wrap(keyViewBuffer, 0, keyViewBuffer.capacity());
    final DirectBuffer valueViewBuffer = columnFamilyContext.getValueView();
    valueInstance.wrap(valueViewBuffer, 0, valueViewBuffer.capacity());

    return visitor.visit(keyInstance, valueInstance);
  }
}
