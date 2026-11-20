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

import io.taskite.protocol.impl.record.value.broker.JobBatchRecord;
import io.taskite.protocol.record.Record;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobBatchIntent;
import io.taskite.protocol.record.value.broker.TenantOwned;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

public final class JobActivationClient {
  private final CommandWriter writer;
  private final JobBatchRecord value =
      new JobBatchRecord()
          .setTenantId(TenantOwned.DEFAULT_TENANT_IDENTIFIER)
          .setWorker("taskite-engine-test")
          .setMaxJobsToActivate(1)
          .setTimeout(Duration.ofSeconds(30).toMillis());

  public JobActivationClient(final CommandWriter writer) {
    this.writer = Objects.requireNonNull(writer, "writer");
  }

  public JobActivationClient withType(final String type) {
    value.setType(type);
    return this;
  }

  public JobActivationClient withTenantId(final String tenant) {
    value.setTenantId(tenant);
    return this;
  }

  public JobActivationClient withMaxJobsToActivate(final int maximum) {
    value.setMaxJobsToActivate(maximum);
    return this;
  }

  public JobActivationClient withTimeout(final Duration timeout) {
    return withTimeout(timeout.toMillis());
  }

  public JobActivationClient withTimeout(final long timeoutMillis) {
    value.setTimeout(timeoutMillis);
    return this;
  }

  public JobActivationClient withWorker(final String worker) {
    value.setWorker(worker);
    return this;
  }

  public JobActivationClient withWorker(final String workerId, final long leaseToken) {
    value.setWorkerId(workerId).setWorkerLeaseToken(leaseToken);
    return this;
  }

  public JobActivationClient withFetchVariables(final String... names) {
    return withFetchVariables(List.of(names));
  }

  public JobActivationClient withFetchVariables(final List<String> names) {
    value.setFetchVariables(List.copyOf(names));
    return this;
  }

  public Record<JobBatchRecord> activate() {
    return writer.send(
        -1,
        ValueType.JOB_BATCH,
        JobBatchIntent.ACTIVATE,
        value,
        JobBatchIntent.ACTIVATED,
        null,
        JobBatchRecord.class);
  }

  public RejectionClient expectRejection() {
    return new RejectionClient();
  }

  public final class RejectionClient {
    private RejectionClient() {}

    public Record<?> activate() {
      return writer.reject(
          -1, ValueType.JOB_BATCH, JobBatchIntent.ACTIVATE, value, JobBatchIntent.ACTIVATED, null);
    }
  }
}
