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
import io.taskite.db.impl.DbForeignKey;
import io.taskite.db.impl.DbInt;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbNil;
import io.taskite.db.impl.DbString;
import io.taskite.db.impl.DbTenantAwareKey;
import io.taskite.db.impl.DbTenantAwareKey.PlacementType;
import io.taskite.engine.state.mutable.MutableJobState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import org.agrona.DirectBuffer;

/**
 * Transactional persistence for job records and indexes. The job record lives in {@link
 * ColumnFamilies#JOBS} keyed by job key, its lifecycle state in {@link ColumnFamilies#JOB_STATES}
 * keyed by a foreign key into JOBS, and an activatable index in {@link
 * ColumnFamilies#JOB_ACTIVATABLE} keyed by {@code [type, invertedPriority, jobKey, tenant]}.
 * Timeout and retry backoff indexes are ordered by {@code [dueDate, jobKey]} and maintained in the
 * same transaction. The subscription index is keyed by {@code [subscriptionKey, jobKey]} and covers
 * all persisted lifecycle states.
 */
public final class DbJobState implements MutableJobState {

  private static final String DEFAULT_TENANT = "<default>";

  // Two separate wrappers so a get during a put does not clobber the value being written.
  private final JobRecordValue jobRecordToRead;
  private final JobRecordValue jobRecordToWrite;

  // [jobKey]
  private final DbLong jobKey;
  // [jobKey] -> JOBS
  private final DbForeignKey<DbLong> fkJob;
  // JOBS: [jobKey] -> JobRecord.
  private final ColumnFamily<DbLong, JobRecordValue> jobsColumnFamilies;

  private final JobStateValue jobState;
  // JOB_STATES: [jobKey FK -> JOBS] -> lifecycle State.
  private final ColumnFamily<DbForeignKey<DbLong>, JobStateValue> statesJobColumnFamilies;

  // [jobType]
  private final DbString jobTypeKey;
  // [tenantId]
  private final DbString tenantIdKey;
  // [invertedPriority]: Integer.MAX_VALUE - priority.
  // Orders higher priorities first under the unsigned byte comparator.
  private final DbInt invertedPriorityKey;
  // [invertedPriority, jobKey FK -> JOBS]
  private final DbCompositeKey<DbInt, DbForeignKey<DbLong>> invertedPriorityJobKey;
  // [jobType, invertedPriority, jobKey FK -> JOBS]
  private final DbCompositeKey<DbString, DbCompositeKey<DbInt, DbForeignKey<DbLong>>>
      typePriorityJobKey;
  // [jobType, invertedPriority, jobKey, tenantId]
  private final DbTenantAwareKey<
          DbCompositeKey<DbString, DbCompositeKey<DbInt, DbForeignKey<DbLong>>>>
      tenantAwarePriorityKey;
  // JOB_ACTIVATABLE: [jobType, invertedPriority, jobKey FK -> JOBS, tenantId] -> nil.
  // Type-prefix scans visit higher priorities first, then order equal priorities by job key.
  private final ColumnFamily<
          DbTenantAwareKey<DbCompositeKey<DbString, DbCompositeKey<DbInt, DbForeignKey<DbLong>>>>,
          DbNil>
      priorityActivatableColumnFamilies;
  // [dueDate, jobKey]
  private final DbCompositeKey<DbLong, DbLong> timeKey;
  // JOB_DEADLINES: [activationDeadline, jobKey] -> nil; ordered by timeout deadline.
  private final ColumnFamily<DbCompositeKey<DbLong, DbLong>, DbNil> deadlineColumnFamilies;
  // JOB_BACKOFF: [retryDueDate, jobKey] -> nil; ordered by retry availability.
  private final ColumnFamily<DbCompositeKey<DbLong, DbLong>, DbNil> backoffColumnFamilies;

  private final DbLong subscriptionKey;
  // JOBS_BY_SUBSCRIPTION: [subscriptionKey, jobKey] -> nil.
  private final DbCompositeKey<DbLong, DbLong> subscriptionJobKey;
  private final ColumnFamily<DbCompositeKey<DbLong, DbLong>, DbNil>
      jobsBySubscriptionColumnFamilies;

  // [tenantId, workerId, workerLeaseToken]
  private final DbCompositeKey<DbString, DbCompositeKey<DbString, DbLong>> workerSessionKey;
  // [tenantId, workerId, workerLeaseToken, jobKey]
  private final DbCompositeKey<DbCompositeKey<DbString, DbCompositeKey<DbString, DbLong>>, DbLong>
      activatedWorkerKey;
  // JOB_ACTIVATED_BY_WORKER: [tenantId, workerId, workerLeaseToken, jobKey] -> nil.
  // Worker-session prefix scans enumerate jobs belonging to that fenced lease.
  private final ColumnFamily<
          DbCompositeKey<DbCompositeKey<DbString, DbCompositeKey<DbString, DbLong>>, DbLong>, DbNil>
      activatedByWorkerColumnFamilies;
  // [metadataId]: 0 is the worker-index cursor; 1 is the ready marker.
  private final DbLong metadataKey;
  private final DbLong metadataValue;
  // JOB_INDEX_METADATA: [0] -> worker-index migration cursor; [1] -> ready marker (1).
  private final ColumnFamily<DbLong, DbLong> indexMetadataColumnFamilies;

  public DbJobState(final ProcessingStateStore storage) {
    jobRecordToRead = new JobRecordValue();
    jobRecordToWrite = new JobRecordValue();
    jobKey = new DbLong();
    jobState = new JobStateValue();
    jobTypeKey = new DbString();
    tenantIdKey = new DbString();
    invertedPriorityKey = new DbInt();
    timeKey = new DbCompositeKey<>(new DbLong(), new DbLong());
    subscriptionKey = new DbLong();
    subscriptionJobKey = new DbCompositeKey<>(subscriptionKey, new DbLong());
    workerSessionKey =
        new DbCompositeKey<>(new DbString(), new DbCompositeKey<>(new DbString(), new DbLong()));
    activatedWorkerKey = new DbCompositeKey<>(workerSessionKey, new DbLong());
    metadataKey = new DbLong();
    metadataValue = new DbLong();

    Objects.requireNonNull(storage, "storage");

    fkJob = new DbForeignKey<>(jobKey, ColumnFamilies.JOBS);
    jobsColumnFamilies = storage.createColumnFamily(ColumnFamilies.JOBS, jobKey, jobRecordToRead);

    statesJobColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.JOB_STATES, fkJob, jobState);

    invertedPriorityJobKey = new DbCompositeKey<>(invertedPriorityKey, fkJob);
    typePriorityJobKey = new DbCompositeKey<>(jobTypeKey, invertedPriorityJobKey);
    tenantAwarePriorityKey =
        new DbTenantAwareKey<>(tenantIdKey, typePriorityJobKey, PlacementType.SUFFIX);
    priorityActivatableColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.JOB_ACTIVATABLE, tenantAwarePriorityKey, DbNil.INSTANCE);
    deadlineColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.JOB_DEADLINES, timeKey, DbNil.INSTANCE);
    backoffColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.JOB_BACKOFF, timeKey, DbNil.INSTANCE);
    jobsBySubscriptionColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.JOBS_BY_SUBSCRIPTION, subscriptionJobKey, DbNil.INSTANCE);
    activatedByWorkerColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.JOB_ACTIVATED_BY_WORKER, activatedWorkerKey, DbNil.INSTANCE);
    indexMetadataColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.JOB_INDEX_METADATA, metadataKey, metadataValue);
  }

  // [metadataId]
  private DbLong wrapMetadataKey(final long value) {
    metadataKey.wrapLong(value);
    return metadataKey;
  }

  @Override
  public void create(final long key, final JobRecord record) {
    update(key, record, State.ACTIVATABLE);
  }

  @Override
  public void activate(final long key, final JobRecord record) {
    update(key, record, State.ACTIVATED);
  }

  @Override
  public void complete(final long key, final JobRecord record) {
    removeIndexes(key);
    jobKey.wrapLong(key);
    jobsColumnFamilies.deleteIfExists(jobKey);
    statesJobColumnFamilies.deleteIfExists(fkJob);
  }

  @Override
  public void update(final long key, JobRecord record, final State state) {
    // Callers may have passed the shared read value; capture it before reading old indexes.
    record = record.copy();
    removeIndexes(key);
    if (state == State.ACTIVATABLE || (state == State.FAILED && record.getRetries() > 0)) {
      // A retry has no owner until a new ACTIVATED event binds its Worker session.
      record.setWorker("").setWorkerId("").setWorkerLeaseToken(0);
    }

    jobKey.wrapLong(key);
    jobRecordToWrite.setRecord(record);
    jobsColumnFamilies.upsert(jobKey, jobRecordToWrite);
    jobState.setState(state);
    statesJobColumnFamilies.upsert(fkJob, jobState);
    if (record.getSubscriptionKey() > 0) {
      jobsBySubscriptionColumnFamilies.upsert(
          subscriptionJobKey(record.getSubscriptionKey(), key), DbNil.INSTANCE);
    }

    if (state == State.ACTIVATABLE) {
      makeJobActivatable(record.getType(), key, record.getTenantId(), record.getPriority());
    }

    addTimeIndex(key, record, state);
    if (state == State.ACTIVATED && hasWorkerSession(record)) {
      activatedByWorkerColumnFamilies.upsert(workerKey(record, key), DbNil.INSTANCE);
    }
  }

  /** An applied migration event advances the cursor and its indexes in the same transaction. */
  @Override
  public void migrateWorkerIndex(final int limit) {
    if (workerIndexReady()) {
      return;
    }
    final DbLong saved = indexMetadataColumnFamilies.get(wrapMetadataKey(0));
    final long cursor = saved == null ? 0 : saved.getValue();
    final List<Long> keys = new ArrayList<>();
    jobKey.wrapLong(cursor);
    jobsColumnFamilies.whileTrue(
        jobKey,
        (key, ignored) -> {
          if (key.getValue() > cursor) {
            keys.add(key.getValue());
          }
          return keys.size() < limit;
        });
    for (final long key : keys) {
      final JobRecord record = getJob(key).copy();
      if (getState(key) == State.ACTIVATED && hasWorkerSession(record)) {
        activatedByWorkerColumnFamilies.upsert(workerKey(record, key), DbNil.INSTANCE);
      }
    }
    metadataValue.wrapLong(keys.isEmpty() ? cursor : keys.getLast());
    indexMetadataColumnFamilies.upsert(wrapMetadataKey(0), metadataValue);
    if (keys.size() < limit) {
      metadataValue.wrapLong(1);
      indexMetadataColumnFamilies.upsert(wrapMetadataKey(1), metadataValue);
    }
  }

  private void removeIndexes(final long key) {
    final JobRecord previous = getJob(key);
    if (previous == null) {
      return;
    }
    if (previous.getSubscriptionKey() > 0) {
      jobsBySubscriptionColumnFamilies.deleteIfExists(
          subscriptionJobKey(previous.getSubscriptionKey(), key));
    }
    makeJobNotActivatable(previous.getType(), key, previous.getTenantId(), previous.getPriority());
    if (hasWorkerSession(previous)) {
      activatedByWorkerColumnFamilies.deleteIfExists(workerKey(previous, key));
    }
    if (previous.getDeadline() > 0) {
      deadlineColumnFamilies.deleteIfExists(timeKey(previous.getDeadline(), key));
    }
    if (previous.getRecurringTime() > 0) {
      backoffColumnFamilies.deleteIfExists(timeKey(previous.getRecurringTime(), key));
    }
  }

  private void addTimeIndex(final long key, final JobRecord record, final State state) {
    if (state == State.ACTIVATED && record.getDeadline() > 0) {
      deadlineColumnFamilies.upsert(timeKey(record.getDeadline(), key), DbNil.INSTANCE);
    } else if (state == State.FAILED && record.getRetries() > 0 && record.getRecurringTime() > 0) {
      backoffColumnFamilies.upsert(timeKey(record.getRecurringTime(), key), DbNil.INSTANCE);
    }
  }

  private static boolean hasWorkerSession(final JobRecord record) {
    return record.getWorkerLeaseToken() > 0 && !record.getWorkerId().isBlank();
  }

  // [tenantId, workerId, workerLeaseToken, jobKey]
  private DbCompositeKey<DbCompositeKey<DbString, DbCompositeKey<DbString, DbLong>>, DbLong>
      workerKey(final JobRecord record, final long key) {
    workerSessionKey.first().wrapString(tenantOrDefault(record.getTenantId()));
    workerSessionKey.second().first().wrapString(record.getWorkerId());
    workerSessionKey.second().second().wrapLong(record.getWorkerLeaseToken());
    activatedWorkerKey.second().wrapLong(key);
    return activatedWorkerKey;
  }

  // [dueDate, jobKey]: shared scratch key for activation deadlines and retry backoffs.
  private DbCompositeKey<DbLong, DbLong> timeKey(final long date, final long key) {
    timeKey.first().wrapLong(date);
    timeKey.second().wrapLong(key);
    return timeKey;
  }

  private DbCompositeKey<DbLong, DbLong> subscriptionJobKey(
      final long subscription, final long key) {
    subscriptionKey.wrapLong(subscription);
    subscriptionJobKey.second().wrapLong(key);
    return subscriptionJobKey;
  }

  private List<DueJob> findDueJobs(
      final ColumnFamily<DbCompositeKey<DbLong, DbLong>, DbNil> index,
      final long timestamp,
      final int limit) {
    if (limit <= 0) {
      throw new IllegalArgumentException("Due job limit must be positive");
    }
    final List<DueJob> result = new ArrayList<>();
    index.whileTrue(
        (key, ignored) -> {
          final long dueDate = key.first().getValue();
          if (dueDate > timestamp || result.size() >= limit) {
            return false;
          }
          final long id = key.second().getValue();
          final JobRecord job = getJob(id);
          if (job != null) {
            result.add(
                new DueJob(
                    id, dueDate, job.getTenantId(), job.getWorkerId(), job.getWorkerLeaseToken()));
          }
          return result.size() < limit;
        });
    return result;
  }

  private long firstDate(final ColumnFamily<DbCompositeKey<DbLong, DbLong>, DbNil> index) {
    final long[] date = {Long.MAX_VALUE};
    index.whileTrue(
        (key, ignored) -> {
          date[0] = key.first().getValue();
          return false;
        });
    return date[0];
  }

  @Override
  public JobRecord getJob(final long key) {
    jobKey.wrapLong(key);
    final JobRecordValue value = jobsColumnFamilies.get(jobKey);
    return value == null ? null : value.getRecord();
  }

  @Override
  public State getState(final long key) {
    jobKey.wrapLong(key);
    final JobStateValue storedState = statesJobColumnFamilies.get(fkJob);
    return storedState == null ? State.NOT_FOUND : storedState.getState();
  }

  @Override
  public boolean exists(final long key) {
    jobKey.wrapLong(key);
    return jobsColumnFamilies.exists(jobKey);
  }

  @Override
  public List<DueJob> findTimedOutJobs(final long timestamp, final int limit) {
    return findDueJobs(deadlineColumnFamilies, timestamp, limit);
  }

  @Override
  public List<DueJob> findBackedOffJobs(final long timestamp, final int limit) {
    return findDueJobs(backoffColumnFamilies, timestamp, limit);
  }

  @Override
  public long getNextTimeoutDueDate() {
    return firstDate(deadlineColumnFamilies);
  }

  @Override
  public long getNextBackoffDueDate() {
    return firstDate(backoffColumnFamilies);
  }

  @Override
  public List<Long> findActivatedByWorker(
      final String tenantId, final String workerId, final long token, final int limit) {
    if (limit <= 0) {
      throw new IllegalArgumentException("Worker job limit must be positive");
    }
    workerSessionKey.first().wrapString(tenantOrDefault(tenantId));
    workerSessionKey.second().first().wrapString(workerId);
    workerSessionKey.second().second().wrapLong(token);
    final var result = new ArrayList<Long>();
    activatedByWorkerColumnFamilies.whileEqualPrefix(
        workerSessionKey,
        (key, ignored) -> {
          result.add(key.second().getValue());
          return result.size() < limit;
        });
    return result;
  }

  @Override
  public boolean workerIndexReady() {
    return indexMetadataColumnFamilies.exists(wrapMetadataKey(1));
  }

  @Override
  public void forEachJobs(
      final long subscription, final BiFunction<Long, JobRecord, Boolean> callback) {
    if (subscription <= 0) {
      return;
    }
    subscriptionKey.wrapLong(subscription);
    jobsBySubscriptionColumnFamilies.whileEqualPrefix(
        subscriptionKey,
        (key, ignored) -> {
          final long id = key.second().getValue();
          final JobRecord record = getJob(id);
          return record == null || callback.apply(id, record.copy());
        });
  }

  @Override
  public void forEachActivatableJobs(
      final DirectBuffer type,
      final List<String> tenantIds,
      final BiFunction<Long, JobRecord, Boolean> callback) {
    jobTypeKey.wrapBuffer(type);

    priorityActivatableColumnFamilies.whileEqualPrefix(
        jobTypeKey,
        (compositeKey, nil) -> {
          final long visitedKey = compositeKey.wrappedKey().second().second().inner().getValue();
          final String tenantId = compositeKey.tenantKey().toString();
          if (tenantIds.contains(tenantId)) {
            return visitJob(visitedKey, callback);
          }
          return true;
        });
  }

  private boolean visitJob(final long key, final BiFunction<Long, JobRecord, Boolean> callback) {
    final JobRecord job = getJob(key);
    if (job == null) {
      return true;
    }
    return callback.apply(key, job);
  }

  private void makeJobActivatable(
      final String type, final long key, final String tenantId, final int priority) {
    wrapActivatableKey(type, key, tenantId, priority);
    priorityActivatableColumnFamilies.upsert(tenantAwarePriorityKey, DbNil.INSTANCE);
  }

  private void makeJobNotActivatable(
      final String type, final long key, final String tenantId, final int priority) {
    wrapActivatableKey(type, key, tenantId, priority);
    priorityActivatableColumnFamilies.deleteIfExists(tenantAwarePriorityKey);
  }

  // [jobType, invertedPriority, jobKey FK -> JOBS, tenantId]: tenant is the final suffix.
  private void wrapActivatableKey(
      final String type, final long key, final String tenantId, final int priority) {
    jobTypeKey.wrapString(type);
    invertedPriorityKey.wrapInt(Integer.MAX_VALUE - priority);
    jobKey.wrapLong(key);
    tenantIdKey.wrapString(tenantOrDefault(tenantId));
  }

  /** Keeps missing-tenant index keys consistent for both insert and delete. */
  private static String tenantOrDefault(final String tenantId) {
    return tenantId == null || tenantId.isBlank() ? DEFAULT_TENANT : tenantId;
  }
}
