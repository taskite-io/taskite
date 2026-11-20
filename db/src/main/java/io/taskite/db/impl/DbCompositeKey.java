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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/** A composite key made of two ordered key parts, written and read back to back. */
public final class DbCompositeKey<FirstKeyType extends DbKey, SecondKeyType extends DbKey>
    implements DbKey, ContainsForeignKeys {
  private final FirstKeyType first;
  private final SecondKeyType second;
  private final Collection<DbForeignKey<DbKey>> containedForeignKeys;

  public DbCompositeKey(final FirstKeyType first, final SecondKeyType second) {
    this.first = first;
    this.second = second;
    containedForeignKeys = collectContainedForeignKeys(first, second);
  }

  public FirstKeyType first() {
    return first;
  }

  public SecondKeyType second() {
    return second;
  }

  @Override
  public Collection<DbForeignKey<DbKey>> containedForeignKeys() {
    return containedForeignKeys;
  }

  @Override
  public void wrap(final DirectBuffer directBuffer, final int offset, final int length) {
    first.wrap(directBuffer, offset, length);
    final int firstKeyLength = first.getLength();
    second.wrap(directBuffer, offset + firstKeyLength, length - firstKeyLength);
  }

  @Override
  public int getLength() {
    return first.getLength() + second.getLength();
  }

  @Override
  public void write(final MutableDirectBuffer mutableDirectBuffer, final int offset) {
    first.write(mutableDirectBuffer, offset);
    final int firstKeyPartLength = first.getLength();
    second.write(mutableDirectBuffer, offset + firstKeyPartLength);
  }

  private static Collection<DbForeignKey<DbKey>> collectContainedForeignKeys(
      final DbKey first, final DbKey second) {
    final var result = new ArrayList<DbForeignKey<DbKey>>();
    if (first instanceof ContainsForeignKeys firstForeignKeyProvider) {
      result.addAll(firstForeignKeyProvider.containedForeignKeys());
    }
    if (second instanceof ContainsForeignKeys secondForeignKeyProvider) {
      result.addAll(secondForeignKeyProvider.containedForeignKeys());
    }
    return Collections.unmodifiableList(result);
  }

  @Override
  public String toString() {
    return "DbCompositeKey{first=" + first + ", second=" + second + '}';
  }
}
