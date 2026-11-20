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
package io.taskite.placementdriver.state.timer;

import io.taskite.db.ColumnFamily;
import io.taskite.db.impl.DbCompositeKey;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbNil;
import io.taskite.placementdriver.state.mutable.MutableTimerSubscriptionState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;

/**
 * Transactional persistence for PD timer subscriptions. {@link ColumnFamilies#PD_TIMERS} stores
 * subscriptions by their globally generated timer keys; {@link ColumnFamilies#PD_TIMER_DUE_DATES}
 * orders them by {@code [dueDate, timerKey]} for deadline scans. Both column families are updated
 * in the same transaction.
 */
public final class DbTimerSubscriptionState implements MutableTimerSubscriptionState {
  // [timerKey]
  private final DbLong timerKey;
  // TimerSubscriptionRecord encoded as MessagePack, including cluster and tenant identifiers.
  private final TimerSubscriptionRecordValue timerSubscriptionValue;

  // PD_TIMERS: [timerKey] -> TimerSubscriptionRecord MessagePack.
  private final ColumnFamily<DbLong, TimerSubscriptionRecordValue> timerSubscriptionColumnFamilies;
  // [dueDate]: deadline prefix is ordered before timerKey in the due-date index.
  private final DbLong dueDate;
  // [dueDate, timerKey]: due date first, with timer key breaking ties.
  private final DbCompositeKey<DbLong, DbLong> dueDateCompositeKey;
  // PD_TIMER_DUE_DATES: [dueDate, timerKey] -> nil; ordered scans break deadline ties by timer key.
  private final ColumnFamily<DbCompositeKey<DbLong, DbLong>, DbNil> dueDateColumnFamilies;
  // Earliest unconsumed due date of the current scan, or -1 when none remain.
  private long nextDueDate;

  public DbTimerSubscriptionState(final ProcessingStateStore storage) {
    timerKey = new DbLong();
    timerSubscriptionValue = new TimerSubscriptionRecordValue();
    dueDate = new DbLong();
    dueDateCompositeKey = new DbCompositeKey<>(dueDate, timerKey);

    timerSubscriptionColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.PD_TIMERS, timerKey, timerSubscriptionValue);
    dueDateColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_TIMER_DUE_DATES, dueDateCompositeKey, DbNil.INSTANCE);
  }

  @Override
  public Map<Long, TimerSubscriptionRecord> subscriptions() {
    final Map<Long, TimerSubscriptionRecord> result = new LinkedHashMap<>();
    timerSubscriptionColumnFamilies.forEach(
        (key, value) -> result.put(key.getValue(), value.getRecord()));
    return result;
  }

  @Override
  public Optional<TimerSubscriptionRecord> get(final long timerKey) {
    this.timerKey.wrapLong(timerKey);
    final var value =
        timerSubscriptionColumnFamilies.get(this.timerKey, TimerSubscriptionRecordValue::new);
    return value == null ? Optional.empty() : Optional.of(value.getRecord());
  }

  @Override
  public long processTimersWithDueDateBefore(
      final long timestamp, final BiPredicate<Long, TimerSubscriptionRecord> visitor) {
    nextDueDate = -1L;
    dueDateColumnFamilies.whileTrue(
        (key, ignored) -> {
          final long dueDate = key.first().getValue();
          final DbLong timerKey = key.second();
          boolean consumed = false;
          if (dueDate <= timestamp) {
            final var subscription =
                timerSubscriptionColumnFamilies.get(timerKey, TimerSubscriptionRecordValue::new);
            if (subscription == null) {
              return true;
            }
            consumed = visitor.test(timerKey.getValue(), subscription.getRecord());
          }
          if (!consumed) {
            nextDueDate = dueDate;
          }
          return consumed;
        });
    return nextDueDate;
  }

  @Override
  public void put(final long timerKey, final TimerSubscriptionRecord subscription) {
    get(timerKey).ifPresent(previous -> remove(timerKey, previous));
    this.timerKey.wrapLong(timerKey);
    timerSubscriptionValue.setRecord(subscription);
    timerSubscriptionColumnFamilies.upsert(this.timerKey, timerSubscriptionValue);
    dueDate.wrapLong(subscription.getDueDate());
    dueDateColumnFamilies.upsert(dueDateCompositeKey, DbNil.INSTANCE);
  }

  @Override
  public void remove(final long timerKey, final TimerSubscriptionRecord subscription) {
    this.timerKey.wrapLong(timerKey);
    timerSubscriptionColumnFamilies.deleteIfExists(this.timerKey);
    dueDate.wrapLong(subscription.getDueDate());
    dueDateColumnFamilies.deleteIfExists(dueDateCompositeKey);
  }

  @Override
  public void deleteCluster(final String clusterId) {
    subscriptions()
        .forEach(
            (key, subscription) -> {
              if (subscription.getClusterId().equals(clusterId)) {
                remove(key, subscription);
              }
            });
  }
}
