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
package io.taskite.placementdriver.processing.job;

import io.grpc.Status;
import io.taskite.broker.client.response.BrokerErrorException;
import io.taskite.broker.client.response.BrokerRejectionException;
import io.taskite.placementdriver.broker.distribution.BrokerDistributionRouteResolver;
import io.taskite.placementdriver.job.JobYieldTransport;
import io.taskite.placementdriver.state.immutable.JobYieldState;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.impl.record.value.placementdriver.job.JobYieldPlan;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.protocol.record.value.broker.JobYieldStatus;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.StreamProcessorLifecycleAware;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;
import java.time.Duration;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shares transport lifecycle and fencing between the dedicated Yield command processors. */
public final class JobYieldCommandSender implements StreamProcessorLifecycleAware {
  private static final Logger LOG = LoggerFactory.getLogger(JobYieldCommandSender.class);
  private static final String LEADER_UNAVAILABLE = "PARTITION_LEADER_UNAVAILABLE";
  private static final String INVALID_RESPONSE = "INVALID_YIELD_JOBS_RESPONSE";
  private final ProcessingState state;
  private final JobYieldState yields;
  private final Writers writers;
  private final BooleanSupplier leader;
  private final BrokerDistributionRouteResolver routes;
  private final Supplier<JobYieldTransport> transports;
  private final long observationTimeout;
  private volatile ProcessingScheduleService scheduling;
  private volatile long lifecycleGeneration;
  private JobYieldTransport transport;

  public JobYieldCommandSender(
      final TypedRecordProcessorContext context,
      final BooleanSupplier leader,
      final BrokerDistributionRouteResolver routes,
      final Supplier<JobYieldTransport> transports,
      final Duration observationTimeout) {
    state = context.getProcessingState(ProcessingState.class);
    yields = state.getJobYieldState();
    writers = context.getWriters();
    this.leader = leader;
    this.routes = routes;
    this.transports = transports;
    this.observationTimeout = observationTimeout.toMillis();
  }

  private int reserved(final long now) {
    return yields.reservedTargets(now, JobYieldScheduler.MAX_RESERVED)
        + yields.synchronization().reservedTargets(now, JobYieldScheduler.MAX_SYNC_RESERVED);
  }

  public final class JobYieldDispatchProcessor implements TypedRecordProcessor<JobYieldRecord> {
    @Override
    public void processRecord(final TypedRecord<JobYieldRecord> record) {
      if (!active(lifecycleGeneration)) {
        return;
      }
      final var command = record.getValue();
      final var plan = yields.plan(command.getClusterId(), command.getYieldId()).orElse(null);
      if (plan == null || record.getPosition() <= 0) {
        return;
      }
      final var target =
          plan.partitions().stream()
              .filter(partition -> partition.partitionId() == command.getPartitionId())
              .findFirst()
              .orElse(null);
      if (target == null
          || target.status() != JobYieldStatus.PENDING
          || target.nextAttemptAtEpochMillis() != command.getNextAttemptAtEpochMillis()
          || target.nextAttemptAtEpochMillis() > record.getTimestamp()
          || reserved(record.getTimestamp()) >= JobYieldScheduler.MAX_RESERVED) {
        return;
      }
      final var request =
          identity(plan, target.partitionId())
              .setDispatchAttemptId(record.getPosition())
              .setAttempts(target.attempts() + 1)
              .setReleasedJobCount(target.releasedJobCount())
              .setLastError(target.lastError())
              .setPauseReason(target.pauseReason())
              .setTimestampEpochMillis(record.getTimestamp())
              .setNextAttemptAtEpochMillis(record.getTimestamp() + observationTimeout);
      writers.state().appendFollowUpEvent(plan.yieldId(), JobYieldIntent.PROGRESS_UPDATED, request);
      final long expected = lifecycleGeneration;
      writers.postCommit().append(() -> send(request, expected, false));
    }
  }

  public final class JobYieldSyncPartitionProcessor
      implements TypedRecordProcessor<JobYieldRecord> {
    @Override
    public void processRecord(final TypedRecord<JobYieldRecord> record) {
      if (!active(lifecycleGeneration)) {
        return;
      }
      final var command = record.getValue();
      final var entry =
          yields
              .synchronization()
              .get(command.getClusterId(), command.getPartitionId())
              .orElse(null);
      if (entry == null
          || record.getPosition() <= 0
          || entry.attemptId() != command.getDispatchAttemptId()
          || entry.cursor() != command.getPreviousSyncCursor()
          || entry.dueAt() != command.getNextAttemptAtEpochMillis()
          || entry.dueAt() > record.getTimestamp()
          || yields
                  .synchronization()
                  .reservedTargets(record.getTimestamp(), JobYieldScheduler.MAX_SYNC_RESERVED)
              >= JobYieldScheduler.MAX_SYNC_RESERVED
          || reserved(record.getTimestamp()) >= JobYieldScheduler.MAX_RESERVED
          || state
              .getBrokerPartitionTopologyState()
              .partitionTopology(command.getClusterId(), command.getPartitionId())
              .isEmpty()) {
        return;
      }
      final var page = yields.plansAfter(entry.clusterId(), entry.cursor(), 100);
      final var request =
          new JobYieldRecord()
              .setClusterId(entry.clusterId())
              .setPartitionId(entry.partitionId())
              .setDispatchAttemptId(record.getPosition())
              .setPreviousSyncCursor(entry.cursor())
              .setSyncCursor(page.isEmpty() ? entry.cursor() : page.getLast().yieldId())
              .setSyncComplete(page.size() < 100)
              .setSessions(page.stream().map(plan -> identity(plan, entry.partitionId())).toList())
              .setTimestampEpochMillis(record.getTimestamp())
              .setNextAttemptAtEpochMillis(record.getTimestamp() + observationTimeout);
      // Persist the last acknowledged cursor, not the page-end cursor being sent.
      final var prepared = request.copy().setSyncCursor(entry.cursor()).setSessions(List.of());
      writers.state().appendFollowUpEvent(0, JobYieldIntent.PARTITION_SYNC_UPDATED, prepared);
      final long expected = lifecycleGeneration;
      writers.postCommit().append(() -> send(request, expected, true));
    }
  }

  public final class JobYieldUpdatePartitionSyncProcessor
      implements TypedRecordProcessor<JobYieldRecord> {
    @Override
    public void processRecord(final TypedRecord<JobYieldRecord> record) {
      if (!active(lifecycleGeneration)) {
        return;
      }
      final var result = record.getValue();
      final var entry =
          yields.synchronization().get(result.getClusterId(), result.getPartitionId()).orElse(null);
      if (entry == null
          || !entry.inFlight()
          || entry.attemptId() != result.getDispatchAttemptId()
          || entry.cursor() != result.getPreviousSyncCursor()
          || entry.dueAt() != result.getNextAttemptAtEpochMillis()) {
        return;
      }
      final boolean success = result.getLastError().isEmpty() && result.getSyncCursor() >= 0;
      final var updated =
          result
              .copy()
              .setStatus(JobYieldStatus.COMPLETED)
              .setSyncCursor(success ? result.getSyncCursor() : entry.cursor())
              .setTimestampEpochMillis(record.getTimestamp())
              .setNextAttemptAtEpochMillis(
                  record.getTimestamp() + (success ? result.isSyncComplete() ? 5000 : 0 : 1000));
      writers.state().appendFollowUpEvent(0, JobYieldIntent.PARTITION_SYNC_UPDATED, updated);
    }
  }

  private void send(final JobYieldRecord request, final long expected, final boolean sync) {
    if (!active(expected)) {
      return;
    }
    try {
      final var endpoint =
          routes
              .resolveBrokerDistributionRoute(request.getClusterId())
              .leaderEndpoint(request.getPartitionId());
      if (endpoint.isEmpty()) {
        result(request, null, new IllegalStateException(LEADER_UNAVAILABLE), expected, sync);
        return;
      }
      final var future =
          sync
              ? transport.syncWorkerSessions(endpoint.get(), request.copy())
              : transport.yieldJobs(endpoint.get(), request.copy());
      future.onComplete((response, error) -> result(request, response, error, expected, sync));
    } catch (final RuntimeException error) {
      result(request, null, error, expected, sync);
    }
  }

  private void result(
      final JobYieldRecord request,
      final JobYieldRecord response,
      final Throwable error,
      final long expected,
      final boolean sync) {
    final var service = scheduling;
    if (service == null || !active(expected)) {
      return;
    }
    final var result = request.copy().setSessions(List.of());
    if (error == null
        && response != null
        && (sync
            ? request.getClusterId().equals(response.getClusterId())
                && request.getPartitionId() == response.getPartitionId()
                && response.getSyncCursor() >= 0
            : request.sameIdentity(response)
                && response.getReleasedJobCount() >= 0
                && (response.getStatus() == JobYieldStatus.COMPLETED
                    || response.getStatus() == JobYieldStatus.PENDING))) {
      result.setLastError("").setPauseReason("");
      if (sync) {
        result.setSyncCursor(response.getSyncCursor()).setSyncComplete(response.isSyncComplete());
      } else {
        result.setReleasedJobCount(response.getReleasedJobCount()).setStatus(response.getStatus());
      }
    } else {
      recordFailure(result, error);
    }
    service.schedule(
        Duration.ZERO,
        commands -> {
          if (active(expected)) {
            commands.tryAppend(
                request.getYieldId(),
                ValueType.JOB_YIELD,
                sync
                    ? JobYieldIntent.UPDATE_PARTITION_SYNC
                    : result.getStatus() == JobYieldStatus.COMPLETED
                        ? JobYieldIntent.ACKNOWLEDGE
                        : JobYieldIntent.UPDATE_PROGRESS,
                result);
          }
        });
  }

  public static long retryDelay(final int attempts) {
    return Math.min(30_000, 1_000L << Math.min(5, Math.max(0, attempts - 1)));
  }

  private static void recordFailure(final JobYieldRecord result, final Throwable error) {
    result.setPauseReason("");
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      if (cause instanceof final BrokerRejectionException rejection) {
        final var type = rejection.getRejection().type();
        result.setLastError(type.name());
        if (type == RejectionType.FEATURE_DISABLED) {
          result.setPauseReason(type.name());
        }
        return;
      }
      if (cause instanceof final BrokerErrorException brokerError) {
        result.setLastError(brokerError.getError().code().name());
        return;
      }
    }
    result.setLastError(
        error == null ? INVALID_RESPONSE : Status.fromThrowable(error).getCode().name());
  }

  private static JobYieldRecord identity(final JobYieldPlan plan, final int partition) {
    return new JobYieldRecord()
        .setYieldId(plan.yieldId())
        .setClusterId(plan.clusterId())
        .setPartitionId(partition)
        .setTenantId(plan.tenantId())
        .setWorkerId(plan.workerId())
        .setWorkerLeaseToken(plan.workerLeaseToken())
        .setReason(plan.reason());
  }

  private boolean active(final long expected) {
    return scheduling != null && expected == lifecycleGeneration && leader.getAsBoolean();
  }

  @Override
  public void onRecovered(final ProcessingScheduleService scheduling) {
    onClose();
    transport = transports.get();
    this.scheduling = scheduling;
  }

  @Override
  public void onClose() {
    lifecycleGeneration++;
    scheduling = null;
    if (transport != null) {
      try {
        transport.close();
      } catch (final Exception error) {
        LOG.warn("Failed to close Job Yield transport", error);
      }
      transport = null;
    }
  }
}
