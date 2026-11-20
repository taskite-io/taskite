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
package io.taskite.placementdriver.state.job;

import io.taskite.db.ColumnFamily;
import io.taskite.db.impl.DbLong;
import io.taskite.placementdriver.state.mutable.MutableJobSubscriptionState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.ArrayList;
import java.util.Optional;

public final class DbJobSubscriptionState implements MutableJobSubscriptionState {
  private final DbLong key = new DbLong();
  private final JobSubscriptionRecordValue value = new JobSubscriptionRecordValue();
  private final ColumnFamily<DbLong, JobSubscriptionRecordValue> subscriptionsColumnFamilies;

  public DbJobSubscriptionState(final ProcessingStateStore storage) {
    subscriptionsColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.PD_JOB_SUBSCRIPTIONS, key, value);
  }

  @Override
  public Optional<JobSubscriptionRecord> get(final long subscriptionKey) {
    key.wrapLong(subscriptionKey);
    final var found = subscriptionsColumnFamilies.get(key, JobSubscriptionRecordValue::new);
    return found == null ? Optional.empty() : Optional.of(found.getRecord());
  }

  @Override
  public void put(final long subscriptionKey, final JobSubscriptionRecord subscription) {
    key.wrapLong(subscriptionKey);
    value.setRecord(subscription);
    subscriptionsColumnFamilies.upsert(key, value);
  }

  @Override
  public void remove(final long subscriptionKey) {
    key.wrapLong(subscriptionKey);
    subscriptionsColumnFamilies.deleteIfExists(key);
  }

  @Override
  public void deleteCluster(final String clusterId) {
    final var keys = new ArrayList<Long>();
    subscriptionsColumnFamilies.forEach(
        (key, value) -> {
          if (value.getRecord().getClusterId().equals(clusterId)) {
            keys.add(key.getValue());
          }
        });
    keys.forEach(this::remove);
  }
}
