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
package io.taskite.protocol.impl.record.value.broker;

import io.taskite.protocol.impl.encoding.MsgPackConverter;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.broker.JobYieldReason;
import io.taskite.protocol.record.value.broker.JobYieldStatus;
import java.util.List;
import java.util.Objects;

/** Deterministic control identity and committed per-partition progress. */
public final class JobYieldRecord extends UnifiedRecordValue {
  private long yieldId;

  private String clusterId = "";
  private int partitionId;
  private String tenantId = "";
  private String workerId = "";
  private long workerLeaseToken;
  private JobYieldReason reason = JobYieldReason.WORKER_LEASE_EXPIRED;
  private JobYieldStatus status = JobYieldStatus.PENDING;
  private long releasedJobCount;
  private long timestampEpochMillis;
  private String lastError = "";
  private String pauseReason = "";
  private int attempts;
  private long dispatchAttemptId;
  private long nextAttemptAtEpochMillis;
  private long previousSyncCursor;
  private long syncCursor;
  private boolean syncComplete;
  private List<JobYieldRecord> sessions = List.of();

  public long getDispatchAttemptId() {
    return dispatchAttemptId;
  }

  public JobYieldRecord setDispatchAttemptId(long value) {
    dispatchAttemptId = value;
    return this;
  }

  public long getPreviousSyncCursor() {
    return previousSyncCursor;
  }

  public JobYieldRecord setPreviousSyncCursor(long value) {
    previousSyncCursor = value;
    return this;
  }

  public long getSyncCursor() {
    return syncCursor;
  }

  public JobYieldRecord setSyncCursor(long value) {
    syncCursor = value;
    return this;
  }

  public boolean isSyncComplete() {
    return syncComplete;
  }

  public JobYieldRecord setSyncComplete(boolean value) {
    syncComplete = value;
    return this;
  }

  public List<JobYieldRecord> getSessions() {
    return sessions;
  }

  public JobYieldRecord setSessions(List<JobYieldRecord> value) {
    sessions = List.copyOf(value);
    return this;
  }

  public int getAttempts() {
    return attempts;
  }

  public JobYieldRecord setAttempts(int value) {
    attempts = value;
    return this;
  }

  public long getNextAttemptAtEpochMillis() {
    return nextAttemptAtEpochMillis;
  }

  public JobYieldRecord setNextAttemptAtEpochMillis(long value) {
    nextAttemptAtEpochMillis = value;
    return this;
  }

  public long getYieldId() {
    return yieldId;
  }

  public JobYieldRecord setYieldId(long value) {
    yieldId = value;
    return this;
  }

  public String getClusterId() {
    return clusterId;
  }

  public JobYieldRecord setClusterId(String value) {
    clusterId = value;
    return this;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public JobYieldRecord setPartitionId(int value) {
    partitionId = value;
    return this;
  }

  public String getTenantId() {
    return tenantId;
  }

  public JobYieldRecord setTenantId(String value) {
    tenantId = value;
    return this;
  }

  public String getWorkerId() {
    return workerId;
  }

  public JobYieldRecord setWorkerId(String value) {
    workerId = value;
    return this;
  }

  public long getWorkerLeaseToken() {
    return workerLeaseToken;
  }

  public JobYieldRecord setWorkerLeaseToken(long value) {
    workerLeaseToken = value;
    return this;
  }

  public JobYieldReason getReason() {
    return reason;
  }

  public JobYieldRecord setReason(JobYieldReason value) {
    reason = value;
    return this;
  }

  public JobYieldStatus getStatus() {
    return status;
  }

  public JobYieldRecord setStatus(JobYieldStatus value) {
    status = value;
    return this;
  }

  public long getReleasedJobCount() {
    return releasedJobCount;
  }

  public JobYieldRecord setReleasedJobCount(long value) {
    releasedJobCount = value;
    return this;
  }

  public long getTimestampEpochMillis() {
    return timestampEpochMillis;
  }

  public JobYieldRecord setTimestampEpochMillis(long value) {
    timestampEpochMillis = value;
    return this;
  }

  public String getLastError() {
    return lastError;
  }

  public JobYieldRecord setLastError(String value) {
    lastError = value;
    return this;
  }

  public String getPauseReason() {
    return pauseReason;
  }

  public JobYieldRecord setPauseReason(String value) {
    pauseReason = value;
    return this;
  }

  public JobYieldRecord copy() {
    return MsgPackConverter.copy(this);
  }

  public boolean sameIdentity(JobYieldRecord other) {
    return yieldId == other.yieldId
        && partitionId == other.partitionId
        && workerLeaseToken == other.workerLeaseToken
        && Objects.equals(clusterId, other.clusterId)
        && Objects.equals(tenantId, other.tenantId)
        && Objects.equals(workerId, other.workerId)
        && reason == other.reason;
  }
}
