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
package io.taskite.engine.processing.job;

import io.taskite.engine.processing.common.JobActivationBehavior;
import io.taskite.engine.processing.common.WorkerJobLeaseCheck;
import io.taskite.engine.state.immutable.JobState;
import io.taskite.engine.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.broker.JobBatchRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobBatchIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.Writers;
import io.taskite.util.BufferUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Selects a bounded batch without mutating its index while it is being traversed. */
public final class JobBatchActivateProcessor implements TypedRecordProcessor<JobBatchRecord> {
  // Leave room for the command envelope under the transport's default 4 MiB message limit.
  private static final int MAX_BATCH_BYTES = 3 * 1024 * 1024;
  private static final String PROCESSING_TIME_REQUIRED =
      "Expected to construct job batch activation processor, but the processing time supplier was null";
  private static final String INVALID_WORKER_SESSION =
      "Expected to activate jobs of type '%s' for tenant '%s' using Worker '%s' with lease token"
          + " '%d', but the Worker identity is incomplete or its session is revoked";
  private static final String WORKER_SESSION_NOT_READY =
      "Expected to activate jobs of type '%s' for tenant '%s' using Worker '%s' with lease token"
          + " '%d', but Worker index migration or session synchronization is not complete";
  private static final String INVALID_ACTIVATION_REQUEST =
      "Expected to activate jobs with non-blank type and tenant and positive timeout and batch size,"
          + " but received type '%s', tenant '%s', timeout '%d', and maxJobsToActivate '%d'";
  private static final String DEADLINE_OUT_OF_RANGE =
      "Expected to activate jobs of type '%s' for tenant '%s' with timeout '%d', but adding the"
          + " timeout to the processing time exceeds the signed 64-bit deadline range";
  private static final String JOB_EXCEEDS_BATCH_SIZE =
      "Expected to activate job with key '%d' of type '%s' for tenant '%s', but its activation batch"
          + " requires '%d' bytes and exceeds the limit of '%d' bytes; reduce fetchVariables";

  private final JobState jobs;
  private final JobActivationBehavior activation;
  private final Writers writers;
  private final KeyGenerator keys;
  private final LongSupplier processingTime;
  private final WorkerJobLeaseCheck leaseCheck;

  public JobBatchActivateProcessor(
      final ProcessingState state,
      final Writers writers,
      final KeyGenerator keys,
      final LongSupplier processingTime) {
    jobs = state.getJobState();
    leaseCheck = new WorkerJobLeaseCheck(state);
    activation = new JobActivationBehavior(state);
    this.writers = writers;
    this.keys = keys;
    this.processingTime = Objects.requireNonNull(processingTime, PROCESSING_TIME_REQUIRED);
  }

  @Override
  public void processRecord(final TypedRecord<JobBatchRecord> record) {
    final var request = record.getValue();
    if (!leaseCheck.canActivate(
        request.getTenantId(), request.getWorkerId(), request.getWorkerLeaseToken())) {
      reject(
          record,
          leaseCheck.ready()
              ? RejectionType.STALE_WORKER_SESSION
              : RejectionType.WORKER_SESSION_NOT_READY,
          String.format(
              leaseCheck.ready() ? INVALID_WORKER_SESSION : WORKER_SESSION_NOT_READY,
              request.getType(),
              request.getTenantId(),
              request.getWorkerId(),
              request.getWorkerLeaseToken()));
      return;
    }
    if (request.getType() == null
        || request.getType().isBlank()
        || request.getTenantId() == null
        || request.getTenantId().isBlank()
        || request.getTimeout() <= 0
        || request.getMaxJobsToActivate() <= 0) {
      reject(
          record,
          RejectionType.INVALID_ARGUMENT,
          String.format(
              INVALID_ACTIVATION_REQUEST,
              request.getType(),
              request.getTenantId(),
              request.getTimeout(),
              request.getMaxJobsToActivate()));
      return;
    }
    final long deadline;
    try {
      deadline = Math.addExact(processingTime.getAsLong(), request.getTimeout());
    } catch (final ArithmeticException error) {
      reject(
          record,
          RejectionType.INVALID_ARGUMENT,
          String.format(
              DEADLINE_OUT_OF_RANGE,
              request.getType(),
              request.getTenantId(),
              request.getTimeout()));
      return;
    }
    final var batch =
        new JobBatchRecord()
            .setType(request.getType())
            .setWorker(request.getWorker())
            .setWorkerId(request.getWorkerId())
            .setWorkerLeaseToken(request.getWorkerLeaseToken())
            .setTenantId(request.getTenantId())
            .setTimeout(request.getTimeout())
            .setMaxJobsToActivate(request.getMaxJobsToActivate())
            .setFetchVariables(request.getFetchVariables());
    final long[] bytes = {batch.getLength()};
    final boolean[] oversized = {false};
    final var selected = new ArrayList<JobBatchRecord.ActivatedJob>();
    jobs.forEachActivatableJobs(
        BufferUtil.wrapString(request.getType()),
        List.of(request.getTenantId()),
        (key, job) -> {
          if (!activation.canActivate(key, job)) {
            return true;
          }
          selected.add(new JobBatchRecord.ActivatedJob(key, job.copy()));
          return selected.size() < request.getMaxJobsToActivate();
        });

    for (final var selectedJob : selected) {
      final var activated =
          activation
              .withVariables(selectedJob.key(), selectedJob.record(), request.getFetchVariables())
              .setWorker(request.getWorker())
              .setWorkerId(request.getWorkerId())
              .setWorkerLeaseToken(request.getWorkerLeaseToken())
              .setTimeout(request.getTimeout())
              .setDeadline(deadline);
      bytes[0] += activated.getLength() + 64L;
      if (bytes[0] > MAX_BATCH_BYTES) {
        oversized[0] = true;
        break;
      }
      batch.getJobs().add(new JobBatchRecord.ActivatedJob(selectedJob.key(), activated));
    }
    if (oversized[0] && batch.getJobs().isEmpty()) {
      reject(
          record,
          RejectionType.EXCEEDED_BATCH_RECORD_SIZE,
          String.format(
              JOB_EXCEEDS_BATCH_SIZE,
              selected.getFirst().key(),
              request.getType(),
              request.getTenantId(),
              bytes[0],
              MAX_BATCH_BYTES));
      return;
    }
    final long key = keys.nextKey();
    writers.state().appendFollowUpEvent(key, ValueType.JOB_BATCH, JobBatchIntent.ACTIVATED, batch);
    writers.response().writeEventOnCommand(key, JobBatchIntent.ACTIVATED, batch, record);
  }

  private void reject(
      final TypedRecord<JobBatchRecord> command, final RejectionType type, final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(command, type, reason);
  }
}
