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

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A typed column family: stores keys of type {@link KeyType} with values of type {@link ValueType}.
 * A column family is a logical key space within a single RocksDB column family, distinguished by a
 * numeric prefix; all reads and writes run inside the owning {@link TransactionContext}, so they
 * reflect uncommitted changes made earlier in the same transaction.
 */
public interface ColumnFamily<KeyType extends DbKey, ValueType extends DbValue> {

  /**
   * Inserts a new key/value pair.
   *
   * @throws DbInconsistentException if the key already exists
   */
  void insert(KeyType key, ValueType value);

  /**
   * Updates the value of an existing key.
   *
   * @throws DbInconsistentException if the key does not exist
   */
  void update(KeyType key, ValueType value);

  /** Inserts or updates a key/value pair. */
  void upsert(KeyType key, ValueType value);

  /**
   * Returns the value stored for {@code key}, or {@code null} if absent. The returned instance is
   * the column family's shared mutable value; copy it before the next access.
   */
  ValueType get(KeyType key);

  /**
   * Returns the value stored for {@code key}, or {@code null} if absent, materialized into a fresh
   * instance from {@code valueSupplier} (safe to retain).
   */
  ValueType get(KeyType key, Supplier<ValueType> valueSupplier);

  /** Visits every value in key order. */
  void forEach(Consumer<ValueType> consumer);

  /** Visits every key/value pair in key order. */
  void forEach(BiConsumer<KeyType, ValueType> consumer);

  /** Visits key/value pairs in key order until the visitor returns {@code false}. */
  void whileTrue(KeyValuePairVisitor<KeyType, ValueType> visitor);

  /** As {@link #whileTrue(KeyValuePairVisitor)} but starting at {@code startAtKey}. */
  void whileTrue(KeyType startAtKey, KeyValuePairVisitor<KeyType, ValueType> visitor);

  /** Visits every key/value pair whose key shares the given prefix, in key order. */
  void whileEqualPrefix(DbKey keyPrefix, BiConsumer<KeyType, ValueType> visitor);

  /** As above, stopping when the visitor returns {@code false}. */
  void whileEqualPrefix(DbKey keyPrefix, KeyValuePairVisitor<KeyType, ValueType> visitor);

  /** As above, starting at {@code startAtKey}. */
  void whileEqualPrefix(
      DbKey keyPrefix, KeyType startAtKey, KeyValuePairVisitor<KeyType, ValueType> visitor);

  /**
   * Deletes an existing key/value pair.
   *
   * @throws DbInconsistentException if the key does not exist
   */
  void deleteExisting(KeyType key);

  /** Deletes the key/value pair if the key exists; no-op otherwise. */
  void deleteIfExists(KeyType key);

  /** Returns whether the key exists in this column family. */
  boolean exists(KeyType key);

  /** Returns whether the column family has no entries. */
  boolean isEmpty();

  /** Counts entries by iterating over all of them (expensive). */
  long count();

  /** Counts entries with the given prefix by iterating over them (expensive). */
  long countEqualPrefix(DbKey prefix);
}
