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

import io.taskite.protocol.EnumValue;
import java.io.File;
import java.util.Optional;

/**
 * A transactional key/value database over RocksDB that stores typed pairs in logical column
 * families. Logical families share a physical RocksDB column family and are distinguished by a
 * stable, explicitly assigned numeric prefix. One optimistic transaction can therefore span and
 * atomically commit changes across logical families.
 *
 * @param <ColumnFamilyType> the enum whose constants name the logical column families
 */
public interface TaskiteDb<ColumnFamilyType extends Enum<ColumnFamilyType> & EnumValue>
    extends AutoCloseable {

  /**
   * Creates a typed handle for a logical column family. The key and value instances fix the column
   * family's types and are reused as scratch buffers, so a column family is not thread-safe.
   */
  <KeyType extends DbKey, ValueType extends DbValue>
      ColumnFamily<KeyType, ValueType> createColumnFamily(
          ColumnFamilyType columnFamily,
          TransactionContext context,
          KeyType keyInstance,
          ValueType valueInstance);

  /** Creates a physical checkpoint of the whole database in the given directory. */
  void createSnapshot(File snapshotDir);

  /** Returns a RocksDB property value, if present. */
  Optional<String> getProperty(String propertyName);

  /** Opens a new transaction context for reading and writing. */
  TransactionContext createContext();

  /** Returns whether the given logical column family has no entries. */
  boolean isEmpty(ColumnFamilyType column, TransactionContext context);

  @Override
  void close();
}
