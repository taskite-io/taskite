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
package io.taskite.broker.state;

import io.taskite.db.impl.DbConstants;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.state.PartitionStorage;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.rocksdb.RocksDBException;

/**
 * Durable per-partition last-applied fenced-recovery epoch (Q6). A fenced {@code RESET_PEERS} is
 * only accepted when its target recovery epoch is strictly greater than the value stored here, so a
 * stale or replayed reset can never lower the epoch or re-run against an already-recovered
 * partition. The value lives in the partition's own runtime store so it survives restart and is
 * captured by the partition snapshot alongside the raft log.
 */
public final class BrokerPartitionRecoveryEpochStore {
  private static final String EPOCH_KEY = "__recovery_epoch__";

  private final PartitionStorage storage;

  public BrokerPartitionRecoveryEpochStore(final PartitionStorage storage) {
    this.storage = storage;
  }

  /** The last-applied recovery epoch, or {@code 0} when none has been recorded. */
  public synchronized long current() {
    synchronized (storage) {
      if (storage.runtimeDb().isClosed()) {
        return 0L;
      }
      try {
        final byte[] stored = storage.runtimeDb().get(storage.defaultColumnFamily(), key());
        if (stored == null) {
          return 0L;
        }
        return ByteBuffer.wrap(stored).order(DbConstants.DB_BYTE_ORDER).getLong();
      } catch (final RocksDBException error) {
        throw new IllegalStateException("failed to read recovery epoch", error);
      }
    }
  }

  /** Persists {@code epoch} as the new last-applied recovery epoch. */
  public synchronized void put(final long epoch) {
    synchronized (storage) {
      if (storage.runtimeDb().isClosed()) {
        throw new IllegalStateException("partition storage is closed");
      }
      try {
        final byte[] value =
            ByteBuffer.allocate(Long.BYTES).order(DbConstants.DB_BYTE_ORDER).putLong(epoch).array();
        storage.runtimeDb().put(storage.defaultColumnFamily(), key(), value);
      } catch (final RocksDBException error) {
        throw new IllegalStateException("failed to persist recovery epoch " + epoch, error);
      }
    }
  }

  private static byte[] key() {
    final byte[] stringBytes = EPOCH_KEY.getBytes(StandardCharsets.UTF_8);
    return ByteBuffer.allocate(Long.BYTES + Integer.BYTES + stringBytes.length)
        .order(DbConstants.DB_BYTE_ORDER)
        .putLong(ColumnFamilies.BROKER_RECOVERY_EPOCHS.getValue())
        .putInt(stringBytes.length)
        .put(stringBytes)
        .array();
  }
}
