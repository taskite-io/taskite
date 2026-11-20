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
package io.taskite.protocol.impl.record.value.placementdriver.worker;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.taskite.protocol.impl.record.value.placementdriver.job.JobYieldPlan;
import io.taskite.protocol.record.UnifiedRecordValue;
import java.util.ArrayList;
import java.util.List;

/** Command and event payload for a concrete SDK job-worker instance. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class WorkerRegistrationRecord extends UnifiedRecordValue {
  public static final long DEFAULT_RENEWAL_TIMEOUT_MILLIS = 30_000L;

  private String clusterId = "";
  private List<String> tenantIds = new ArrayList<>();
  private String workerId = "";
  private String name = "";
  private String jobType = "";
  private int maxJobsActive;
  private int activeJobs;
  private int concurrency;
  private long timeoutMillis;
  private long requestTimeoutMillis;
  private long pollIntervalMillis;
  private List<String> fetchVariables = new ArrayList<>();
  private String clientVersion = "";
  private long renewalTimeoutMillis = DEFAULT_RENEWAL_TIMEOUT_MILLIS;
  private long expectedLastRenewalEpochMillis;
  private long expectedStatusChangedAtEpochMillis;
  private long eventTimestampEpochMillis;
  private long workerLeaseToken;
  private long expectedWorkerLeaseToken;
  private long leaseDeadlineEpochMillis;

  private List<JobYieldPlan> yieldPlans = new ArrayList<>();

  private long registeredAtEpochMillis;

  public List<JobYieldPlan> getYieldPlans() {
    return yieldPlans;
  }

  public WorkerRegistrationRecord setYieldPlans(List<JobYieldPlan> value) {
    yieldPlans = new ArrayList<>(value);
    return this;
  }

  public long getRegisteredAtEpochMillis() {
    return registeredAtEpochMillis;
  }

  public WorkerRegistrationRecord setRegisteredAtEpochMillis(long value) {
    registeredAtEpochMillis = value;
    return this;
  }

  public long getWorkerLeaseToken() {
    return workerLeaseToken;
  }

  public WorkerRegistrationRecord setWorkerLeaseToken(long value) {
    workerLeaseToken = value;
    return this;
  }

  public long getExpectedWorkerLeaseToken() {
    return expectedWorkerLeaseToken;
  }

  public WorkerRegistrationRecord setExpectedWorkerLeaseToken(long value) {
    expectedWorkerLeaseToken = value;
    return this;
  }

  public long getLeaseDeadlineEpochMillis() {
    return leaseDeadlineEpochMillis;
  }

  public WorkerRegistrationRecord setLeaseDeadlineEpochMillis(long value) {
    leaseDeadlineEpochMillis = value;
    return this;
  }

  public String getClusterId() {
    return clusterId;
  }

  public WorkerRegistrationRecord setClusterId(final String clusterId) {
    this.clusterId = value(clusterId);
    return this;
  }

  public List<String> getTenantIds() {
    return tenantIds;
  }

  public WorkerRegistrationRecord setTenantIds(final List<String> tenantIds) {
    this.tenantIds = copy(tenantIds);
    return this;
  }

  public String getWorkerId() {
    return workerId;
  }

  public WorkerRegistrationRecord setWorkerId(final String workerId) {
    this.workerId = value(workerId);
    return this;
  }

  public String getName() {
    return name;
  }

  public WorkerRegistrationRecord setName(final String name) {
    this.name = value(name);
    return this;
  }

  public String getJobType() {
    return jobType;
  }

  public WorkerRegistrationRecord setJobType(final String jobType) {
    this.jobType = value(jobType);
    return this;
  }

  public int getMaxJobsActive() {
    return maxJobsActive;
  }

  public WorkerRegistrationRecord setMaxJobsActive(final int maxJobsActive) {
    this.maxJobsActive = maxJobsActive;
    return this;
  }

  public int getActiveJobs() {
    return activeJobs;
  }

  public WorkerRegistrationRecord setActiveJobs(final int activeJobs) {
    this.activeJobs = activeJobs;
    return this;
  }

  public int getConcurrency() {
    return concurrency;
  }

  public WorkerRegistrationRecord setConcurrency(final int concurrency) {
    this.concurrency = concurrency;
    return this;
  }

  public long getTimeoutMillis() {
    return timeoutMillis;
  }

  public WorkerRegistrationRecord setTimeoutMillis(final long timeoutMillis) {
    this.timeoutMillis = timeoutMillis;
    return this;
  }

  public long getRequestTimeoutMillis() {
    return requestTimeoutMillis;
  }

  public WorkerRegistrationRecord setRequestTimeoutMillis(final long requestTimeoutMillis) {
    this.requestTimeoutMillis = requestTimeoutMillis;
    return this;
  }

  public long getPollIntervalMillis() {
    return pollIntervalMillis;
  }

  public WorkerRegistrationRecord setPollIntervalMillis(final long pollIntervalMillis) {
    this.pollIntervalMillis = pollIntervalMillis;
    return this;
  }

  public List<String> getFetchVariables() {
    return fetchVariables;
  }

  public WorkerRegistrationRecord setFetchVariables(final List<String> fetchVariables) {
    this.fetchVariables = copy(fetchVariables);
    return this;
  }

  public String getClientVersion() {
    return clientVersion;
  }

  public WorkerRegistrationRecord setClientVersion(final String clientVersion) {
    this.clientVersion = value(clientVersion);
    return this;
  }

  public long getRenewalTimeoutMillis() {
    return renewalTimeoutMillis;
  }

  public WorkerRegistrationRecord setRenewalTimeoutMillis(final long renewalTimeoutMillis) {
    this.renewalTimeoutMillis = renewalTimeoutMillis;
    return this;
  }

  public long getExpectedLastRenewalEpochMillis() {
    return expectedLastRenewalEpochMillis;
  }

  public WorkerRegistrationRecord setExpectedLastRenewalEpochMillis(
      final long expectedLastRenewalEpochMillis) {
    this.expectedLastRenewalEpochMillis = Math.max(0L, expectedLastRenewalEpochMillis);
    return this;
  }

  public long getExpectedStatusChangedAtEpochMillis() {
    return expectedStatusChangedAtEpochMillis;
  }

  public WorkerRegistrationRecord setExpectedStatusChangedAtEpochMillis(
      final long expectedStatusChangedAtEpochMillis) {
    this.expectedStatusChangedAtEpochMillis = Math.max(0L, expectedStatusChangedAtEpochMillis);
    return this;
  }

  public long getEventTimestampEpochMillis() {
    return eventTimestampEpochMillis;
  }

  public WorkerRegistrationRecord setEventTimestampEpochMillis(
      final long eventTimestampEpochMillis) {
    this.eventTimestampEpochMillis = Math.max(0L, eventTimestampEpochMillis);
    return this;
  }

  private static String value(final String value) {
    return value == null ? "" : value;
  }

  private static List<String> copy(final List<String> values) {
    return values == null ? new ArrayList<>() : new ArrayList<>(values);
  }
}
