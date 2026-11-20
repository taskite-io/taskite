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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.taskite.protocol.impl.record.value.broker.IncidentRecord;
import io.taskite.protocol.impl.record.value.broker.JobBatchRecord;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.broker.IncidentIntent;
import io.taskite.protocol.record.intent.broker.JobBatchIntent;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.util.List;
import org.junit.jupiter.api.Test;

final class MicrometerEngineMetricsTest {

  private static final int PARTITION_ID = 2;

  @Test
  void recordsJobAndIncidentEventsAfterCommit() {
    final var registry = new SimpleMeterRegistry();
    final var metrics = new MicrometerEngineMetrics(registry, "cluster-a", PARTITION_ID, () -> 4);
    metrics.onRecovered();

    metrics.onProcessed(
        List.of(
            event(ValueType.JOB, JobIntent.CREATED, new JobRecord().setJobType("payment")),
            event(ValueType.JOB, JobIntent.ACTIVATED, new JobRecord().setJobType("payment")),
            event(
                ValueType.JOB_BATCH,
                JobBatchIntent.ACTIVATED,
                new JobBatchRecord()
                    .setJobs(
                        List.of(
                            new JobBatchRecord.ActivatedJob(
                                1, new JobRecord().setJobType("payment")),
                            new JobBatchRecord.ActivatedJob(
                                2, new JobRecord().setJobType("shipping"))))),
            event(ValueType.JOB, JobIntent.TIMED_OUT, new JobRecord().setJobType("payment")),
            event(ValueType.JOB, JobIntent.COMPLETED, new JobRecord().setJobType("payment")),
            event(ValueType.JOB, JobIntent.FAILED, new JobRecord().setJobType("payment")),
            event(ValueType.JOB, JobIntent.CANCELED, new JobRecord().setJobType("payment")),
            event(ValueType.INCIDENT, IncidentIntent.CREATED, new IncidentRecord())));
    assertEquals(1, jobCount(registry, "created", "payment"));
    assertEquals(2, jobCount(registry, "activated", "payment"));
    assertEquals(1, jobCount(registry, "activated", "shipping"));
    assertEquals(1, jobCount(registry, "timed out", "payment"));
    assertEquals(1, jobCount(registry, "completed", "payment"));
    assertEquals(1, jobCount(registry, "failed", "payment"));
    assertEquals(1, jobCount(registry, "canceled", "payment"));
    assertEquals(1, count(registry, MicrometerEngineMetrics.INCIDENT_EVENTS, "created", null));
    assertEquals(5, gauge(registry));

    metrics.onProcessed(
        List.of(event(ValueType.INCIDENT, IncidentIntent.RESOLVED, new IncidentRecord())));

    assertEquals(1, count(registry, MicrometerEngineMetrics.INCIDENT_EVENTS, "resolved", null));
    assertEquals(4, gauge(registry));

    metrics.close();
    assertNull(registry.find(MicrometerEngineMetrics.PENDING_INCIDENTS).gauge());
    registry.close();
  }

  @Test
  void ignoresNonLifecycleAndNonEventRecords() {
    final var registry = new SimpleMeterRegistry();
    final var metrics = new MicrometerEngineMetrics(registry, "cluster-a", PARTITION_ID, () -> 0);
    metrics.onRecovered();

    metrics.onProcessed(
        List.of(
            record(RecordType.COMMAND, ValueType.JOB, JobIntent.COMPLETE, new JobRecord()),
            event(ValueType.JOB, JobIntent.RETRIES_UPDATED, new JobRecord())));

    assertNull(registry.find(MicrometerEngineMetrics.JOB_EVENTS).counter());
    assertEquals(0, gauge(registry));
    metrics.close();
    registry.close();
  }

  private LoggedEvent event(
      final ValueType valueType, final Intent intent, final UnifiedRecordValue value) {
    return record(RecordType.EVENT, valueType, intent, value);
  }

  private LoggedEvent record(
      final RecordType recordType,
      final ValueType valueType,
      final Intent intent,
      final UnifiedRecordValue value) {
    return LoggedEvent.ofRecord(
        1, new RecordMetadata().recordType(recordType).valueType(valueType).intent(intent), value);
  }

  private double count(
      final SimpleMeterRegistry registry,
      final String name,
      final String action,
      final String type) {
    final var search =
        registry
            .find(name)
            .tags(
                "cluster",
                "cluster-a",
                "partition",
                Integer.toString(PARTITION_ID),
                "action",
                action);
    return (type == null ? search : search.tag("type", type)).counter().count();
  }

  private double jobCount(
      final SimpleMeterRegistry registry, final String action, final String jobType) {
    return registry
        .get(MicrometerEngineMetrics.JOB_EVENTS)
        .tags(
            "cluster",
            "cluster-a",
            "partition",
            Integer.toString(PARTITION_ID),
            "action",
            action,
            "job_type",
            jobType)
        .counter()
        .count();
  }

  private double gauge(final SimpleMeterRegistry registry) {
    return registry
        .get(MicrometerEngineMetrics.PENDING_INCIDENTS)
        .tags("cluster", "cluster-a", "partition", Integer.toString(PARTITION_ID))
        .gauge()
        .value();
  }
}
