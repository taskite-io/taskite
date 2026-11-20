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
package io.taskite.streamplatform.state;

import io.taskite.db.ColumnFamily;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbString;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.Protocol;
import io.taskite.state.processing.ProcessingStateStore;
import io.taskite.streamplatform.api.state.KeyGenerator;
import java.util.Objects;

/** Transactional key high-water for a cluster and partition. */
public final class PersistedKeyGenerator implements KeyGenerator {
  private static final String LATEST_KEY = "keygen/latestKey";
  private static final String EXHAUSTED = "Key space exhausted for clusterKey %d, partition %d";
  private static final String INVALID_HIGH_WATER =
      "Key %d does not belong to clusterKey %d, partition %d";
  private final long clusterKey;
  private final int partitionId;
  private final long keyStartValue;
  private final ColumnFamily<DbString, DbLong> keyColumnFamilies;
  private final DbString latestKey = new DbString();
  private final DbLong nextValue = new DbLong();

  public PersistedKeyGenerator(
      final long clusterKey, final int partitionId, final ProcessingStateStore storage) {
    Objects.requireNonNull(storage, "storage");
    this.clusterKey = clusterKey;
    this.partitionId = partitionId;
    keyStartValue = Protocol.encodeKey(clusterKey, partitionId, 0);
    keyColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.KEY, new DbString(), new DbLong());
    latestKey.wrapString(LATEST_KEY);
  }

  @Override
  public long nextKey() {
    final long current = currentHighWater();
    if (Protocol.decodeKeyInPartition(current) == Protocol.MAXIMUM_SEQUENCE) {
      throw new IllegalStateException(EXHAUSTED.formatted(clusterKey, partitionId));
    }
    final long next = current + 1;
    persistHighWater(next);
    return next;
  }

  /** Foreign cluster or partition keys do not advance the local high-water. */
  @Override
  public void setKeyIfHigher(final long key) {
    if (Protocol.decodeClusterKey(key) != clusterKey
        || Protocol.decodePartitionId(key) != partitionId) {
      return;
    }
    if (key > currentHighWater()) {
      persistHighWater(key);
    }
  }

  private long currentHighWater() {
    final DbLong current = keyColumnFamilies.get(latestKey);
    final long highWater = current == null ? keyStartValue : current.getValue();
    validateIdentity(highWater);
    return highWater;
  }

  private void validateIdentity(final long key) {
    final boolean valid =
        key >= 0
            && Protocol.decodeClusterKey(key) == clusterKey
            && Protocol.decodePartitionId(key) == partitionId;
    if (!valid) {
      throw new IllegalStateException(INVALID_HIGH_WATER.formatted(key, clusterKey, partitionId));
    }
  }

  private void persistHighWater(final long key) {
    nextValue.wrapLong(key);
    keyColumnFamilies.upsert(latestKey, nextValue);
  }
}
