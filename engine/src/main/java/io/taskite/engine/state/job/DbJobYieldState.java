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
package io.taskite.engine.state.job;

import io.taskite.db.ColumnFamily;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbNil;
import io.taskite.db.impl.DbString;
import io.taskite.engine.state.immutable.JobYieldState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.record.value.broker.JobYieldStatus;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.ArrayList;
import java.util.List;

/** Session fences survive completed operations, restarts, and snapshots. */
public final class DbJobYieldState implements JobYieldState {
  // ["operation:" + yieldId], length-prefixed session key, or the fixed ["sync:cursor"] key.
  private final DbString key;
  // [yieldId]: pending operation lookup and pagination cursor.
  private final DbLong pendingKey;
  private final JobYieldRecordValue value;
  private final JobYieldCursorRecordValue cursorValue;
  // JOB_YIELDS: operation key -> JobYieldRecord MessagePack; session key -> MessagePack yieldId;
  // ["sync:cursor"] -> MessagePack synchronized cursor. Session fences outlive completion.
  private final ColumnFamily<DbString, JobYieldRecordValue> recordsColumnFamilies;
  private final ColumnFamily<DbString, JobYieldCursorRecordValue> cursorsColumnFamilies;
  // PENDING_JOB_YIELDS: [yieldId] -> nil; contains only pending operations, ordered by yieldId.
  private final ColumnFamily<DbLong, DbNil> pendingColumnFamilies;
  private boolean sessionsReady;

  public DbJobYieldState(final ProcessingStateStore storage) {
    key = new DbString();
    pendingKey = new DbLong();
    value = new JobYieldRecordValue();
    cursorValue = new JobYieldCursorRecordValue();

    recordsColumnFamilies = storage.createColumnFamily(ColumnFamilies.JOB_YIELDS, key, value);
    cursorsColumnFamilies = storage.createColumnFamily(ColumnFamilies.JOB_YIELDS, key, cursorValue);
    pendingColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.PENDING_JOB_YIELDS, pendingKey, DbNil.INSTANCE);
  }

  @Override
  public JobYieldRecord get(final long yieldId) {
    key.wrapString("operation:" + yieldId);
    final var stored = recordsColumnFamilies.get(key, JobYieldRecordValue::new);
    return stored == null ? null : stored.getRecord();
  }

  @Override
  public boolean isRevoked(final String tenantId, final String workerId, final long token) {
    key.wrapString(sessionKey(tenantId, workerId, token));
    return cursorsColumnFamilies.exists(key);
  }

  @Override
  public List<JobYieldRecord> pending(final long afterId, final int limit) {
    if (limit < 1) {
      throw new IllegalArgumentException("Pending plan limit must be positive");
    }
    final var result = new ArrayList<JobYieldRecord>();
    pendingKey.wrapLong(afterId);
    pendingColumnFamilies.whileTrue(
        pendingKey,
        (storedKey, ignored) -> {
          if (storedKey.getValue() > afterId) {
            result.add(get(storedKey.getValue()));
          }
          return result.size() < limit;
        });
    return result;
  }

  @Override
  public long synchronizedCursor() {
    key.wrapString("sync:cursor");
    final var stored = cursorsColumnFamilies.get(key);
    return stored == null ? 0 : stored.getRecord();
  }

  @Override
  public boolean sessionsReady() {
    return sessionsReady;
  }

  public void resetSessionsReady() {
    sessionsReady = false;
  }

  public void synchronize(final JobYieldRecord record) {
    for (final var session : record.getSessions()) {
      if (get(session.getYieldId()) == null) {
        fence(session);
      }
    }
    key.wrapString("sync:cursor");
    cursorValue.setRecord(record.getSyncCursor());
    cursorsColumnFamilies.upsert(key, cursorValue);
    if (record.isSyncComplete()) {
      sessionsReady = true;
    }
  }

  public void fence(final JobYieldRecord record) {
    put(record);
    key.wrapString(
        sessionKey(record.getTenantId(), record.getWorkerId(), record.getWorkerLeaseToken()));
    cursorValue.setRecord(record.getYieldId());
    cursorsColumnFamilies.upsert(key, cursorValue);
  }

  /** Associates operation progress with the yielded job's persisted worker session fence. */
  public void released(final String tenantId, final String workerId, final long workerLeaseToken) {
    if (workerLeaseToken <= 0 || workerId.isBlank()) {
      return;
    }
    key.wrapString(sessionKey(tenantId, workerId, workerLeaseToken));
    final var session = cursorsColumnFamilies.get(key);
    if (session == null) {
      return;
    }
    final long yieldId = session.getRecord();
    final var record = get(yieldId);
    if (record == null || record.getStatus() != JobYieldStatus.PENDING) {
      return;
    }
    put(record.setReleasedJobCount(Math.addExact(record.getReleasedJobCount(), 1)));
  }

  public void complete(final JobYieldRecord record) {
    put(record);
  }

  private void put(final JobYieldRecord record) {
    key.wrapString("operation:" + record.getYieldId());
    value.setRecord(record);
    recordsColumnFamilies.upsert(key, value);
    // [yieldId]
    pendingKey.wrapLong(record.getYieldId());
    if (record.getStatus() == JobYieldStatus.PENDING) {
      pendingColumnFamilies.upsert(pendingKey, DbNil.INSTANCE);
    } else {
      pendingColumnFamilies.deleteIfExists(pendingKey);
    }
  }

  // ["session:" + tenantLength + ":" + tenantId + workerLength + ":" + workerId + ":" + token]
  private static String sessionKey(final String tenantId, final String workerId, final long token) {
    // Length prefixes make identifiers containing separators unambiguous.
    return "session:"
        + tenantId.length()
        + ":"
        + tenantId
        + workerId.length()
        + ":"
        + workerId
        + ":"
        + token;
  }
}
