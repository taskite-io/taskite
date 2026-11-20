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

import static org.assertj.core.api.Assertions.assertThat;

import io.taskite.engine.util.EngineRule;
import io.taskite.engine.util.record.RecordingExporter;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RecurJobTest {
  @RegisterExtension final EngineRule ENGINE = EngineRule.singlePartition();

  @Test
  void shouldAutomaticallyReleaseBackedOffJobAndRetainFailureVariables() {
    // given
    final long job = createJob();
    ENGINE.jobs().withType("work").withWorker("old").activate();

    // when
    final var failed =
        ENGINE
            .job()
            .withKey(job)
            .withRetries(2)
            .withRetryBackoff(500)
            .withVariables(Map.of("progress", 7))
            .withErrorMessage("temporary")
            .fail()
            .getValue();

    // then
    assertThat(failed.getRecurringTime()).isPositive();
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs()).isEmpty();
    final var recurred =
        RecordingExporter.jobRecords(JobIntent.RECURRED_AFTER_BACKOFF).withKey(job).getFirst();
    assertThat(recurred.getTimestamp()).isGreaterThanOrEqualTo(failed.getRecurringTime());
    assertThat(recurred.getValue().getRecurringTime()).isZero();
    assertThat(recurred.getValue().getRetryBackoff()).isZero();
    assertThat(recurred.getValue().getRetries()).isEqualTo(2);
    final var released = ENGINE.job().withKey(job).query().getValue();
    assertThat(released.getWorker()).isEmpty();
    assertThat(released.getWorkerId()).isEmpty();
    assertThat(released.getWorkerLeaseToken()).isZero();
    final var retry = ENGINE.jobs().withType("work").activate().getValue().getJobs().getFirst();
    assertThat(retry.key()).isEqualTo(job);
    assertThat(retry.record().getVariables()).containsExactlyEntriesOf(Map.of("progress", 7));
    ENGINE.job().withKey(job).complete();
    assertThat(RecordingExporter.jobRecords(JobIntent.RECURRED_AFTER_BACKOFF).withKey(job))
        .hasSize(1);
  }

  @Test
  void shouldRejectEarlyAndStaleRecurrenceWithoutChangingBackoff() {
    // given
    final long job = createJob();
    final var failed =
        ENGINE.job().withKey(job).withRetries(2).withRetryBackoff(60_000).fail().getValue();

    // when / then
    for (final long recurringTime :
        new long[] {failed.getRecurringTime(), failed.getRecurringTime() - 1}) {
      final var rejected =
          ENGINE.executeCommand(
              job,
              ValueType.JOB,
              JobIntent.RECUR,
              new JobRecord().setTenantId("<default>").setRecurringTime(recurringTime));
      assertThat(rejected.getRejectionType()).isEqualTo(RejectionType.INVALID_STATE);
      assertThat(rejected.getRejectionReason())
          .isEqualTo(
              String.format(
                  "Expected to recur job with key '%d' for tenant '<default>' in state 'FAILED' with positive retries and elapsed matching"
                      + " backoff, but state is 'FAILED', retries are '2', stored recurring time is '%d',"
                      + " and requested recurring time is '%d'",
                  job, failed.getRecurringTime(), recurringTime));
    }
    assertThat(ENGINE.job().withKey(job).query().getValue().getRecurringTime())
        .isEqualTo(failed.getRecurringTime());
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs()).isEmpty();
    assertThat(RecordingExporter.jobRecords(JobIntent.RECURRED_AFTER_BACKOFF).withKey(job))
        .isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {"missing", "foreign", "activatable", "activated", "exhausted"})
  void shouldRejectRecurrenceWithoutAnEligibleBackoff(final String scenario) {
    // given
    final long existing = createJob();
    final long job = scenario.equals("missing") ? 123 : existing;
    if (scenario.equals("activated")) {
      ENGINE.jobs().withType("work").activate();
    } else if (scenario.equals("exhausted")) {
      ENGINE.job().withKey(job).withRetries(0).withRetryBackoff(100).fail();
    }

    // when
    final var rejected =
        ENGINE.executeCommand(
            job,
            ValueType.JOB,
            JobIntent.RECUR,
            new JobRecord().setTenantId(scenario.equals("foreign") ? "other" : "<default>"));

    // then
    assertThat(rejected.getRejectionType())
        .isEqualTo(
            scenario.equals("missing") || scenario.equals("foreign")
                ? RejectionType.NOT_FOUND
                : RejectionType.INVALID_STATE);
    assertThat(RecordingExporter.jobRecords(JobIntent.RECURRED_AFTER_BACKOFF).withKey(job))
        .isEmpty();
  }

  private long createJob() {
    final long instance = ENGINE.job().withType("work").create().getKey();
    return RecordingExporter.jobRecords(JobIntent.CREATED).withKey(instance).getFirst().getKey();
  }
}
