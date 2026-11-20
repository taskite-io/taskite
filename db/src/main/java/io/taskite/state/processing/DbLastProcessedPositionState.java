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
package io.taskite.state.processing;

import io.taskite.db.ColumnFamily;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbString;
import io.taskite.protocol.ColumnFamilies;

/**
 * The last processed source position modelled as a typed column family, structurally mirroring the
 * engine's {@code DbJobState}: the position lives in {@link ColumnFamilies#PROCESSING_META} keyed
 * by a fixed scratch key. It is built on a {@link StateStoreProcessingStore} that owns the
 * transactional database and hands out the typed column family, exactly as {@code DbJobState} is
 * built on the same processing-state store.
 *
 * <p>Each access is a single column-family operation, which already runs inside the store's
 * transaction (the column family opens one via {@code ensureInOpenTransaction}). During a command
 * apply the stream processor has opened the transaction, so {@link #markAsProcessed(long)} joins it
 * as a plain write and commits together with the command's state changes; standalone reads
 * self-commit.
 */
final class DbLastProcessedPositionState implements MutableLastProcessedPositionState {

  private static final String LAST_PROCESSED_POSITION = "logstream/meta/lastProcessedPosition";

  // ["logstream/meta/lastProcessedPosition"]: fixed key for this partition's source position.
  private final DbString lastProcessedPositionKey;
  // A read value bound to the column family at creation, and a separate write value so a get during
  // a put does not clobber the value being written.
  private final DbLong positionToRead;
  private final DbLong positionToWrite;
  // PROCESSING_META: ["logstream/meta/lastProcessedPosition"] -> last processed position (long).
  private final ColumnFamily<DbString, DbLong> processingMetaColumnFamilies;

  DbLastProcessedPositionState(final StateStoreProcessingStore store) {
    lastProcessedPositionKey = new DbString();
    positionToRead = new DbLong();
    positionToWrite = new DbLong();

    lastProcessedPositionKey.wrapString(LAST_PROCESSED_POSITION);
    processingMetaColumnFamilies =
        store.createColumnFamily(
            ColumnFamilies.PROCESSING_META, lastProcessedPositionKey, positionToRead);
  }

  @Override
  public long getLastProcessedPosition() {
    final DbLong stored = processingMetaColumnFamilies.get(lastProcessedPositionKey);
    return stored == null ? 0L : stored.getValue();
  }

  @Override
  public void markAsProcessed(final long position) {
    final DbLong stored = processingMetaColumnFamilies.get(lastProcessedPositionKey);
    if (stored != null && stored.getValue() >= position) {
      return;
    }
    positionToWrite.wrapLong(position);
    processingMetaColumnFamilies.upsert(lastProcessedPositionKey, positionToWrite);
  }
}
