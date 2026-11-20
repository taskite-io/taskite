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
import io.taskite.db.impl.DbCompositeKey;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbNil;
import io.taskite.db.impl.DbString;
import io.taskite.engine.state.mutable.MutableJobSubscriptionState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.state.processing.ProcessingStateStore;

/** Partition-local subscription templates; deletion removes the stored record. */
public final class DbJobSubscriptionState implements MutableJobSubscriptionState {
  private final DbLong key = new DbLong();
  private final JobSubscriptionRecordValue value = new JobSubscriptionRecordValue();
  private final ColumnFamily<DbLong, JobSubscriptionRecordValue> subscriptionsColumnFamilies;
  private final DbCompositeKey<DbString, DbString> typeTenantKey =
      new DbCompositeKey<>(new DbString(), new DbString());
  private final DbCompositeKey<DbCompositeKey<DbString, DbString>, DbLong>
      typeTenantSubscriptionKey = new DbCompositeKey<>(typeTenantKey, new DbLong());
  private final ColumnFamily<DbCompositeKey<DbCompositeKey<DbString, DbString>, DbLong>, DbNil>
      subscriptionsByTypeColumnFamilies;

  public DbJobSubscriptionState(final ProcessingStateStore storage) {
    subscriptionsColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.BROKER_JOB_SUBSCRIPTIONS, key, value);
    subscriptionsByTypeColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.BROKER_JOB_SUBSCRIPTIONS_BY_TYPE,
            typeTenantSubscriptionKey,
            DbNil.INSTANCE);
  }

  @Override
  public JobSubscriptionRecord get(final long id) {
    key.wrapLong(id);
    final var found = subscriptionsColumnFamilies.get(key, JobSubscriptionRecordValue::new);
    return found == null ? null : found.getRecord();
  }

  @Override
  public long findSubscriptionKey(final String jobType, final String tenantId) {
    wrapTypeTenantKey(jobType, tenantId);
    final long[] found = {0};
    subscriptionsByTypeColumnFamilies.whileEqualPrefix(
        typeTenantKey,
        (indexKey, ignored) -> {
          found[0] = indexKey.second().getValue();
          return false;
        });
    return found[0];
  }

  @Override
  public void create(final long id, final JobSubscriptionRecord record) {
    final var previous = get(id);
    if (previous != null) {
      subscriptionsByTypeColumnFamilies.deleteIfExists(wrapIndexKey(id, previous));
    }
    key.wrapLong(id);
    value.setRecord(record);
    subscriptionsColumnFamilies.upsert(key, value);
    subscriptionsByTypeColumnFamilies.upsert(wrapIndexKey(id, record), DbNil.INSTANCE);
  }

  @Override
  public void delete(final long id) {
    final var record = get(id);
    if (record == null) {
      return;
    }
    subscriptionsByTypeColumnFamilies.deleteIfExists(wrapIndexKey(id, record));
    key.wrapLong(id);
    subscriptionsColumnFamilies.deleteIfExists(key);
  }

  private void wrapTypeTenantKey(final String jobType, final String tenantId) {
    typeTenantKey.first().wrapString(jobType);
    typeTenantKey.second().wrapString(tenantId);
  }

  private DbCompositeKey<DbCompositeKey<DbString, DbString>, DbLong> wrapIndexKey(
      final long id, final JobSubscriptionRecord record) {
    wrapTypeTenantKey(record.getJobType(), record.getTenantId());
    typeTenantSubscriptionKey.second().wrapLong(id);
    return typeTenantSubscriptionKey;
  }
}
