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
package io.taskite.engine.util.client;

import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.Record;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.protocol.record.value.broker.TenantOwned;
import io.taskite.util.BufferUtil;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import org.agrona.DirectBuffer;

public final class JobClient {
  private final CommandWriter writer;
  private long key = -1;
  private String type;
  private final JobRecord value =
      new JobRecord().setTenantId(TenantOwned.DEFAULT_TENANT_IDENTIFIER);

  public JobClient(final CommandWriter writer) {
    this.writer = Objects.requireNonNull(writer, "writer");
  }

  public JobClient withKey(final long jobKey) {
    key = jobKey;
    return this;
  }

  public JobClient withType(final String jobType) {
    value.setJobType(jobType);
    return this;
  }

  public JobClient withTenantId(final String tenant) {
    value.setTenantId(tenant);
    return this;
  }

  public JobClient withWorker(final String workerId, final long leaseToken) {
    value.setWorkerId(workerId).setWorkerLeaseToken(leaseToken);
    return this;
  }

  public JobClient withVariables(final Map<String, Object> variables) {
    value.setVariablesMap(Variables.copy(variables));
    return this;
  }

  /** Passes JSON to the native processor, allowing tests of malformed variable rejections. */
  public JobClient withVariables(final String json) {
    value.setVariablesJson(Objects.requireNonNull(json, "json"));
    return this;
  }

  public JobClient withVariables(final DirectBuffer json) {
    return withVariables(BufferUtil.bufferAsString(json));
  }

  public JobClient withVariable(final String name, final Object variable) {
    return withVariables(Variables.add(value.getVariables(), name, variable));
  }

  public JobClient withRetries(final int retries) {
    value.setRetries(retries);
    return this;
  }

  public JobClient withPriority(final int priority) {
    value.setPriority(priority);
    return this;
  }

  public JobClient withTimeout(final long timeoutMillis) {
    value.setTimeout(timeoutMillis);
    return this;
  }

  public JobClient withTimeout(final Duration timeout) {
    return withTimeout(timeout.toMillis());
  }

  public JobClient withErrorMessage(final String message) {
    value.setErrorMessage(message);
    return this;
  }

  public JobClient withRetryBackoff(final Duration backoff) {
    return withRetryBackoff(backoff.toMillis());
  }

  public JobClient withRetryBackoff(final long backoffMillis) {
    value.setRetryBackoff(backoffMillis);
    return this;
  }

  public JobClient withSubscriptionKey(final long subscriptionKey) {
    value.setSubscriptionKey(subscriptionKey);
    return this;
  }

  public Record<JobRecord> create() {
    return writer.send(
        -1, ValueType.JOB, JobIntent.CREATE, value, JobIntent.CREATED, null, JobRecord.class);
  }

  public Record<JobRecord> cancel() {
    return execute(JobIntent.CANCEL, JobIntent.CANCELED);
  }

  public Record<JobRecord> complete() {
    return execute(JobIntent.COMPLETE, JobIntent.COMPLETED);
  }

  public Record<JobRecord> fail() {
    return execute(JobIntent.FAIL, JobIntent.FAILED);
  }

  public Record<JobRecord> updateRetries() {
    return execute(JobIntent.UPDATE_RETRIES, JobIntent.RETRIES_UPDATED);
  }

  public Record<JobRecord> updateTimeout() {
    return execute(JobIntent.UPDATE_TIMEOUT, JobIntent.TIMEOUT_UPDATED);
  }

  public Record<JobRecord> updatePriority() {
    return execute(JobIntent.UPDATE_PRIORITY, JobIntent.PRIORITY_UPDATED);
  }

  public Record<JobRecord> query() {
    return execute(JobIntent.QUERY, JobIntent.QUERIED);
  }

  private long resolveKey() {
    return key;
  }

  private Record<JobRecord> execute(final JobIntent command, final JobIntent event) {
    final long jobKey = resolveKey();
    return writer.send(jobKey, ValueType.JOB, command, value, event, jobKey, JobRecord.class);
  }

  public RejectionClient expectRejection() {
    return new RejectionClient();
  }

  public final class RejectionClient {
    private RejectionClient() {}

    public Record<?> create() {
      return writer.reject(-1, ValueType.JOB, JobIntent.CREATE, value, JobIntent.CREATED, null);
    }

    public Record<?> cancel() {
      return execute(JobIntent.CANCEL, JobIntent.CANCELED);
    }

    public Record<?> complete() {
      return execute(JobIntent.COMPLETE, JobIntent.COMPLETED);
    }

    public Record<?> fail() {
      return execute(JobIntent.FAIL, JobIntent.FAILED);
    }

    public Record<?> updateRetries() {
      return execute(JobIntent.UPDATE_RETRIES, JobIntent.RETRIES_UPDATED);
    }

    public Record<?> updateTimeout() {
      return execute(JobIntent.UPDATE_TIMEOUT, JobIntent.TIMEOUT_UPDATED);
    }

    public Record<?> updatePriority() {
      return execute(JobIntent.UPDATE_PRIORITY, JobIntent.PRIORITY_UPDATED);
    }

    public Record<?> query() {
      return execute(JobIntent.QUERY, JobIntent.QUERIED);
    }

    private Record<?> execute(final JobIntent command, final JobIntent event) {
      final long jobKey = resolveKey();
      return writer.reject(jobKey, ValueType.JOB, command, value, event, jobKey);
    }
  }
}
