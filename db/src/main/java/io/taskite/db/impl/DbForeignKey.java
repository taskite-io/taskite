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
package io.taskite.db.impl;

import io.taskite.db.ContainsForeignKeys;
import io.taskite.db.DbKey;
import io.taskite.db.DbValue;
import io.taskite.protocol.EnumValue;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Predicate;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Wraps a key from another column family, marking it as a reference. When serialized it behaves
 * exactly as the inner key (the {@code columnFamily} is metadata for the foreign-key check only,
 * not written to the buffer). The referenced column family supplies the explicit numeric key prefix
 * used for the existence check.
 *
 * @param inner the referenced key
 * @param columnFamily the column family the referenced key lives in
 * @param match whether the reference must match a full key or only a prefix
 * @param skip predicate over the inner key that, when true, skips the existence check
 */
public record DbForeignKey<K extends DbKey>(
    K inner, EnumValue columnFamily, MatchType match, Predicate<K> skip)
    implements DbKey, DbValue, ContainsForeignKeys {

  public DbForeignKey(final K inner, final EnumValue columnFamily) {
    this(inner, columnFamily, MatchType.Full);
  }

  public DbForeignKey(final K inner, final EnumValue columnFamily, final MatchType match) {
    this(inner, columnFamily, match, k -> false);
  }

  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    inner.wrap(buffer, offset, length);
  }

  @Override
  public int getLength() {
    return inner.getLength();
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    inner.write(buffer, offset);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<DbForeignKey<DbKey>> containedForeignKeys() {
    return Collections.singletonList((DbForeignKey<DbKey>) this);
  }

  public boolean shouldSkipCheck() {
    return skip.test(inner);
  }

  public enum MatchType {
    Full,
    Prefix
  }
}
