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

import io.taskite.protocol.record.UnifiedRecordValue;
import java.util.ArrayList;
import java.util.List;

/** A tenant-scoped activation command and its committed job batch. */
public final class JobBatchRecord extends UnifiedRecordValue {
  private String type = "";
  private String worker = "";
  private String workerId = "";
  private long workerLeaseToken;

  public String getWorkerId() {
    return workerId;
  }

  public JobBatchRecord setWorkerId(String value) {
    workerId = value == null ? "" : value;
    return this;
  }

  public long getWorkerLeaseToken() {
    return workerLeaseToken;
  }

  public JobBatchRecord setWorkerLeaseToken(long value) {
    workerLeaseToken = value;
    return this;
  }

  private String tenantId = "";
  private long timeout;
  private int maxJobsToActivate;
  private List<String> fetchVariables = List.of();
  private List<ActivatedJob> jobs = new ArrayList<>();

  public String getType() {
    return type;
  }

  public JobBatchRecord setType(String value) {
    type = value;
    return this;
  }

  public String getWorker() {
    return worker;
  }

  public JobBatchRecord setWorker(String value) {
    worker = value;
    return this;
  }

  public String getTenantId() {
    return tenantId;
  }

  public JobBatchRecord setTenantId(String value) {
    tenantId = value;
    return this;
  }

  public long getTimeout() {
    return timeout;
  }

  public JobBatchRecord setTimeout(long value) {
    timeout = value;
    return this;
  }

  public int getMaxJobsToActivate() {
    return maxJobsToActivate;
  }

  public JobBatchRecord setMaxJobsToActivate(int value) {
    maxJobsToActivate = value;
    return this;
  }

  public List<String> getFetchVariables() {
    return fetchVariables;
  }

  public JobBatchRecord setFetchVariables(List<String> value) {
    fetchVariables = List.copyOf(value);
    return this;
  }

  public List<ActivatedJob> getJobs() {
    return jobs;
  }

  public JobBatchRecord setJobs(List<ActivatedJob> value) {
    jobs = new ArrayList<>(value);
    return this;
  }

  public record ActivatedJob(long key, JobRecord record) {}
}
