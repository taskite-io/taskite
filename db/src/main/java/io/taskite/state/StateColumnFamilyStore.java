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
package io.taskite.state;

import java.util.Map;
import org.rocksdb.ColumnFamilyHandle;

/**
 * Business-state store backed by the {@code default} column family. Exposes the in-memory {@link
 * StateStore} map contract (live capture/restore) on top of the generic key/value mechanics from
 * {@link AbstractColumnFamilyStore}. Durable snapshots are physical RocksDB checkpoints taken by
 * {@code PartitionSnapshotStore} over the whole runtime database, not per-store payloads.
 */
public final class StateColumnFamilyStore extends AbstractColumnFamilyStore implements StateStore {

  public StateColumnFamilyStore(PartitionStorage storage) {
    this(storage, false, "");
  }

  public StateColumnFamilyStore(PartitionStorage storage, String keyPrefix) {
    this(storage, false, keyPrefix);
  }

  private StateColumnFamilyStore(PartitionStorage storage, boolean ownsStorage, String keyPrefix) {
    super(storage, ownsStorage, defaultColumnFamily(storage), keyPrefix);
  }

  @Override
  public Map<String, String> snapshot() {
    return captureState();
  }

  @Override
  public void restore(Map<String, String> snapshot) {
    replaceState(snapshot);
  }

  private static ColumnFamilyHandle defaultColumnFamily(PartitionStorage storage) {
    return storage.defaultColumnFamily();
  }
}
