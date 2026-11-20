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
package io.taskite.engine.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.taskite.protocol.impl.record.value.broker.JobBatchRecord;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.intent.broker.IncidentIntent;
import io.taskite.protocol.record.intent.broker.JobBatchIntent;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.streamplatform.api.metrics.ProcessingMetrics;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/** Micrometer counters and gauge for committed task-engine events. */
public final class MicrometerEngineMetrics implements ProcessingMetrics {

  public static final String JOB_EVENTS = "taskite_job_events";
  public static final String JOB_EVENTS_TOTAL = JOB_EVENTS + "_total";
  public static final String INCIDENT_EVENTS = "taskite_incident_events";
  public static final String INCIDENT_EVENTS_TOTAL = INCIDENT_EVENTS + "_total";
  public static final String PENDING_INCIDENTS = "taskite_pending_incidents";

  private final MeterRegistry registry;
  private final String cluster;
  private final String partition;
  private final LongSupplier initialPendingIncidents;
  private final Map<CounterKey, Counter> counters = new LinkedHashMap<>();
  private final AtomicLong pendingIncidents = new AtomicLong();
  private final Gauge pendingIncidentsGauge;

  public MicrometerEngineMetrics(
      final MeterRegistry registry,
      final String clusterId,
      final int partitionId,
      final LongSupplier initialPendingIncidents) {
    this.registry = Objects.requireNonNull(registry, "registry");
    cluster = Objects.requireNonNull(clusterId, "clusterId");
    partition = Integer.toString(partitionId);
    this.initialPendingIncidents =
        Objects.requireNonNull(initialPendingIncidents, "initialPendingIncidents");
    pendingIncidentsGauge =
        Gauge.builder(PENDING_INCIDENTS, pendingIncidents, AtomicLong::get)
            .description("The number of currently pending incidents")
            .tag("cluster", cluster)
            .tag("partition", partition)
            .register(registry);
  }

  @Override
  public void onRecovered() {
    pendingIncidents.set(initialPendingIncidents.getAsLong());
  }

  @Override
  public void onProcessed(final List<LoggedEvent> writtenRecords) {
    long pendingIncidentDelta = 0;
    for (final LoggedEvent record : writtenRecords) {
      if (record.metadata().getRecordType() != RecordType.EVENT) {
        continue;
      }
      pendingIncidentDelta += recordEvent(record);
    }
    if (pendingIncidentDelta != 0) {
      pendingIncidents.addAndGet(pendingIncidentDelta);
    }
  }

  @Override
  public void close() {
    counters.values().forEach(registry::remove);
    counters.clear();
    registry.remove(pendingIncidentsGauge);
  }

  private long recordEvent(final LoggedEvent record) {
    return switch (record.metadata().getValueType()) {
      case JOB -> {
        recordJobEvent(record);
        yield 0;
      }
      case JOB_BATCH -> {
        recordJobBatchEvent(record);
        yield 0;
      }
      case INCIDENT -> recordIncidentEvent(record);
      default -> 0;
    };
  }

  private void recordJobEvent(final LoggedEvent record) {
    if (!(record.metadata().getIntent() instanceof final JobIntent intent)
        || !(record.value() instanceof final JobRecord value)) {
      return;
    }
    final String action =
        switch (intent) {
          case CREATED -> "created";
          case ACTIVATED -> "activated";
          case TIMED_OUT -> "timed out";
          case YIELDED -> "yielded";
          case COMPLETED -> "completed";
          case FAILED -> "failed";
          case CANCELED -> "canceled";
          default -> null;
        };
    if (action != null) {
      counter(
              JOB_EVENTS,
              "The number of job events",
              action,
              "job_type",
              normalizeJobType(value.getType()))
          .increment();
    }
  }

  private void recordJobBatchEvent(final LoggedEvent record) {
    if (record.metadata().getIntent() == JobBatchIntent.ACTIVATED
        && record.value() instanceof final JobBatchRecord batch
        && !batch.getJobs().isEmpty()) {
      final Map<String, Long> jobsByType = new LinkedHashMap<>();
      for (final JobBatchRecord.ActivatedJob job : batch.getJobs()) {
        final String jobType =
            job.record().getType() == null || job.record().getType().isBlank()
                ? normalizeJobType(batch.getType())
                : job.record().getType();
        jobsByType.merge(jobType, 1L, Long::sum);
      }
      jobsByType.forEach(
          (jobType, count) ->
              counter(JOB_EVENTS, "The number of job events", "activated", "job_type", jobType)
                  .increment(count));
    }
  }

  private long recordIncidentEvent(final LoggedEvent record) {
    if (!(record.metadata().getIntent() instanceof final IncidentIntent intent)) {
      return 0;
    }
    return switch (intent) {
      case CREATED -> {
        counter(INCIDENT_EVENTS, "The number of incident events", "created").increment();
        yield 1;
      }
      case RESOLVED -> {
        counter(INCIDENT_EVENTS, "The number of incident events", "resolved").increment();
        yield -1;
      }
      default -> 0;
    };
  }

  private Counter counter(final String name, final String description, final String action) {
    return counter(name, description, action, null, null);
  }

  private Counter counter(
      final String name,
      final String description,
      final String action,
      final String extraTag,
      final String extraTagValue) {
    final CounterKey key = new CounterKey(name, action, extraTag, extraTagValue);
    return counters.computeIfAbsent(
        key,
        ignored -> {
          final Counter.Builder builder =
              Counter.builder(name)
                  .description(description)
                  .tag("cluster", cluster)
                  .tag("partition", partition)
                  .tag("action", action);
          if (extraTag != null) {
            builder.tag(extraTag, extraTagValue);
          }
          return builder.register(registry);
        });
  }

  private static String normalizeJobType(final String jobType) {
    return jobType == null ? "" : jobType;
  }

  private record CounterKey(String name, String action, String extraTag, String extraTagValue) {}
}
