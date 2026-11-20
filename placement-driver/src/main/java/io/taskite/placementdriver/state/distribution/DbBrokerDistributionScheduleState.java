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
package io.taskite.placementdriver.state.distribution;

import io.taskite.db.ColumnFamily;
import io.taskite.db.DbValue;
import io.taskite.db.impl.DbCompositeKey;
import io.taskite.db.impl.DbForeignKey;
import io.taskite.db.impl.DbInt;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbNil;
import io.taskite.db.impl.DbString;
import io.taskite.placementdriver.state.immutable.CommandDistributionState.PendingTarget;
import io.taskite.placementdriver.state.mutable.MutableBrokerDistributionScheduleState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * PD_BROKER_DISTRIBUTION_SCHEDULES (234): [distribution FK -> PD_COMMAND_DISTRIBUTIONS, target
 * cluster, partition] -> [generation, attempt id, dueAt].
 *
 * <p>PD_BROKER_DISTRIBUTION_DUE_DATES (235): [dueAt, schedule FK -> 234] -> nil. Both tables change
 * in the same Applier transaction. Attempts are retained across leadership changes;
 * ACK/reset/delete remove both entries.
 */
public final class DbBrokerDistributionScheduleState
    implements MutableBrokerDistributionScheduleState {
  // [distributionKey FK -> PD_COMMAND_DISTRIBUTIONS, clusterId, partitionId]: operation key.
  private final DbCompositeKey<DbForeignKey<DbLong>, DbCompositeKey<DbString, DbInt>> targetKey;
  // [dueAt, schedule FK -> PD_BROKER_DISTRIBUTION_SCHEDULES]: operation key.
  private final DbCompositeKey<
          DbLong,
          DbForeignKey<DbCompositeKey<DbForeignKey<DbLong>, DbCompositeKey<DbString, DbInt>>>>
      deadlineKey;
  // PD_BROKER_DISTRIBUTION_SCHEDULES:
  // [distributionKey FK -> PD_COMMAND_DISTRIBUTIONS, clusterId, partitionId]
  // -> ScheduleValue [generation, attemptId, dueAt] (three longs).
  private final ColumnFamily<
          DbCompositeKey<DbForeignKey<DbLong>, DbCompositeKey<DbString, DbInt>>, ScheduleValue>
      schedulesColumnFamilies;
  // PD_BROKER_DISTRIBUTION_DUE_DATES:
  // [dueAt, schedule FK -> PD_BROKER_DISTRIBUTION_SCHEDULES] -> nil.
  // Orders target dispatch/observation deadlines and supports counting reserved targets.
  private final ColumnFamily<
          DbCompositeKey<
              DbLong,
              DbForeignKey<DbCompositeKey<DbForeignKey<DbLong>, DbCompositeKey<DbString, DbInt>>>>,
          DbNil>
      deadlinesColumnFamilies;

  public DbBrokerDistributionScheduleState(final ProcessingStateStore storage) {
    targetKey =
        new DbCompositeKey<>(
            new DbForeignKey<>(new DbLong(), ColumnFamilies.PD_COMMAND_DISTRIBUTIONS),
            new DbCompositeKey<>(new DbString(), new DbInt()));
    deadlineKey =
        new DbCompositeKey<>(
            new DbLong(),
            new DbForeignKey<>(
                new DbCompositeKey<>(
                    new DbForeignKey<>(new DbLong(), ColumnFamilies.PD_COMMAND_DISTRIBUTIONS),
                    new DbCompositeKey<>(new DbString(), new DbInt())),
                ColumnFamilies.PD_BROKER_DISTRIBUTION_SCHEDULES));

    schedulesColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_BROKER_DISTRIBUTION_SCHEDULES, targetKey, new ScheduleValue());
    deadlinesColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_BROKER_DISTRIBUTION_DUE_DATES, deadlineKey, DbNil.INSTANCE);
  }

  @Override
  public Optional<Entry> get(final PendingTarget target) {
    final var value = schedulesColumnFamilies.get(wrapTargetKey(target));
    return value == null
        ? Optional.empty()
        : Optional.of(new Entry(target, value.generation, value.attemptId, value.dueAt));
  }

  @Override
  public void put(final Entry entry) {
    remove(entry.target());
    final var value = new ScheduleValue();
    value.generation = entry.generation();
    value.attemptId = entry.attemptId();
    value.dueAt = entry.dueAt();
    schedulesColumnFamilies.upsert(wrapTargetKey(entry.target()), value);
    deadlinesColumnFamilies.upsert(wrapDeadlineKey(entry.dueAt(), entry.target()), DbNil.INSTANCE);
  }

  @Override
  public void remove(final PendingTarget target) {
    get(target)
        .ifPresent(
            entry ->
                deadlinesColumnFamilies.deleteIfExists(wrapDeadlineKey(entry.dueAt(), target)));
    schedulesColumnFamilies.deleteIfExists(wrapTargetKey(target));
  }

  @Override
  public List<Entry> dueBefore(final long now, final int limit) {
    if (limit <= 0) {
      return List.of();
    }
    final List<Entry> result = new ArrayList<>();
    deadlinesColumnFamilies.whileTrue(
        (key, value) -> {
          if (key.first().getValue() > now) {
            return false;
          }
          get(target(key.second().inner())).ifPresent(result::add);
          return result.size() < limit;
        });
    return result;
  }

  @Override
  public long nextDueAt() {
    final long[] next = {-1};
    deadlinesColumnFamilies.whileTrue(
        (key, value) -> {
          next[0] = key.first().getValue();
          return false;
        });
    return next[0];
  }

  @Override
  public int reservedTargets(final long now, final int limit) {
    final int[] count = {0};
    deadlinesColumnFamilies.whileTrue(
        wrapDeadlineKey(now + 1, PendingTarget.START), (key, value) -> ++count[0] < limit);
    return count[0];
  }

  // [distributionKey FK -> PD_COMMAND_DISTRIBUTIONS, clusterId, partitionId]
  private DbCompositeKey<DbForeignKey<DbLong>, DbCompositeKey<DbString, DbInt>> wrapTargetKey(
      final PendingTarget target) {
    targetKey.first().inner().wrapLong(target.distributionKey());
    targetKey.second().first().wrapString(target.clusterId());
    targetKey.second().second().wrapInt(target.partitionId());
    return targetKey;
  }

  private static PendingTarget target(
      final DbCompositeKey<DbForeignKey<DbLong>, DbCompositeKey<DbString, DbInt>> key) {
    return new PendingTarget(
        key.first().inner().getValue(),
        key.second().first().toString(),
        key.second().second().getValue());
  }

  // [dueAt, FK(distributionKey, clusterId, partitionId) -> PD_BROKER_DISTRIBUTION_SCHEDULES]
  private DbCompositeKey<
          DbLong,
          DbForeignKey<DbCompositeKey<DbForeignKey<DbLong>, DbCompositeKey<DbString, DbInt>>>>
      wrapDeadlineKey(final long dueAt, final PendingTarget target) {
    deadlineKey.first().wrapLong(dueAt);
    final var schedule = deadlineKey.second().inner();
    schedule.first().inner().wrapLong(target.distributionKey());
    schedule.second().first().wrapString(target.clusterId());
    schedule.second().second().wrapInt(target.partitionId());
    return deadlineKey;
  }

  private static final class ScheduleValue implements DbValue {
    private long generation;
    private long attemptId;
    private long dueAt;

    @Override
    public int getLength() {
      return Long.BYTES * 3;
    }

    @Override
    public void write(final MutableDirectBuffer buffer, final int offset) {
      buffer.putLong(offset, generation);
      buffer.putLong(offset + Long.BYTES, attemptId);
      buffer.putLong(offset + Long.BYTES * 2, dueAt);
    }

    @Override
    public void wrap(final DirectBuffer buffer, final int offset, final int length) {
      generation = buffer.getLong(offset);
      attemptId = buffer.getLong(offset + Long.BYTES);
      dueAt = buffer.getLong(offset + Long.BYTES * 2);
    }
  }
}
