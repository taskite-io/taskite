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

class TimeOutJobTest {
  @RegisterExtension final EngineRule ENGINE = EngineRule.singlePartition();

  @Test
  void shouldAutomaticallyReleaseExpiredJobWithoutConsumingRetriesOrLosingVariables() {
    // given
    final long instance =
        ENGINE
            .job()
            .withType("work")
            .withVariables(Map.of("amount", 42, "private", "value"))
            .create()
            .getKey();
    final long job =
        RecordingExporter.jobRecords(JobIntent.CREATED).withKey(instance).getFirst().getKey();
    final var activated =
        ENGINE
            .jobs()
            .withType("work")
            .withWorker("old")
            .withTimeout(200)
            .withFetchVariables("amount")
            .activate()
            .getValue()
            .getJobs()
            .getFirst()
            .record();

    // when
    final var timeout = RecordingExporter.jobRecords(JobIntent.TIMED_OUT).withKey(job).getFirst();

    // then
    assertThat(timeout.getTimestamp()).isGreaterThanOrEqualTo(activated.getDeadline());
    assertThat(timeout.getValue().getRetries()).isEqualTo(3);
    assertThat(timeout.getValue().getDeadline()).isZero();
    final var released = ENGINE.job().withKey(job).query().getValue();
    assertThat(released.getWorker()).isEmpty();
    assertThat(released.getWorkerId()).isEmpty();
    assertThat(released.getWorkerLeaseToken()).isZero();
    final var retry =
        ENGINE.jobs().withType("work").withWorker("new").activate().getValue().getJobs().getFirst();
    assertThat(retry.key()).isEqualTo(job);
    assertThat(retry.record().getRetries()).isEqualTo(3);
    assertThat(retry.record().getVariables())
        .containsExactlyInAnyOrderEntriesOf(Map.of("amount", 42, "private", "value"));
    ENGINE.job().withKey(job).withVariables(Map.of("answer", 42)).complete();

    assertThat(RecordingExporter.jobRecords(JobIntent.TIMED_OUT).withKey(job)).hasSize(1);
  }

  @Test
  void shouldRejectEarlyTimeoutWithoutReleasingActivatedJob() {
    // given
    final long job = createJob();
    final var activated =
        ENGINE.jobs().withType("work").activate().getValue().getJobs().getFirst().record();

    // when
    final var rejected =
        ENGINE.executeCommand(
            job,
            ValueType.JOB,
            JobIntent.TIME_OUT,
            new JobRecord().setTenantId("<default>").setDeadline(activated.getDeadline()));

    // then
    assertThat(rejected.getRejectionType()).isEqualTo(RejectionType.INVALID_STATE);
    assertThat(rejected.getRejectionReason())
        .isEqualTo(
            String.format(
                "Expected to time out job with key '%d' for tenant '<default>' in state 'ACTIVATED' after its deadline with matching"
                    + " worker session, but state is 'ACTIVATED', stored deadline is '%d', requested"
                    + " deadline is '%d', stored Worker is '%s' with lease token '%d', and requested"
                    + " Worker is '' with lease token '0'",
                job,
                activated.getDeadline(),
                activated.getDeadline(),
                activated.getWorkerId(),
                activated.getWorkerLeaseToken()));
    assertThat(ENGINE.job().withKey(job).query().getValue().getDeadline())
        .isEqualTo(activated.getDeadline());
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs()).isEmpty();
    assertThat(RecordingExporter.jobRecords(JobIntent.TIMED_OUT).withKey(job)).isEmpty();
  }

  @Test
  void shouldRejectStaleDeadlineAfterJobHasBeenActivatedAgain() {
    // given
    final long job = createJob();
    final var old =
        ENGINE
            .jobs()
            .withType("work")
            .withTimeout(100)
            .activate()
            .getValue()
            .getJobs()
            .getFirst()
            .record();
    RecordingExporter.jobRecords(JobIntent.TIMED_OUT).withKey(job).getFirst();
    final var current =
        ENGINE.jobs().withType("work").activate().getValue().getJobs().getFirst().record();

    // when
    final var rejected =
        ENGINE.executeCommand(
            job,
            ValueType.JOB,
            JobIntent.TIME_OUT,
            new JobRecord().setTenantId("<default>").setDeadline(old.getDeadline()));

    // then
    assertThat(rejected.getRejectionType()).isEqualTo(RejectionType.INVALID_STATE);
    assertThat(ENGINE.job().withKey(job).query().getValue().getDeadline())
        .isEqualTo(current.getDeadline());
    assertThat(RecordingExporter.jobRecords(JobIntent.TIMED_OUT).withKey(job)).hasSize(1);
  }

  @ParameterizedTest
  @ValueSource(strings = {"missing", "foreign", "activatable", "failed"})
  void shouldRejectTimeoutForMissingForeignOrInactiveJob(final String scenario) {
    // given
    final long existing = createJob();
    final long job = scenario.equals("missing") ? 123 : existing;
    if (scenario.equals("foreign")) {
      ENGINE.jobs().withType("work").activate();
    } else if (scenario.equals("failed")) {
      ENGINE.job().withKey(job).withRetries(2).withRetryBackoff(60_000).fail();
    }

    // when
    final var rejected =
        ENGINE.executeCommand(
            job,
            ValueType.JOB,
            JobIntent.TIME_OUT,
            new JobRecord().setTenantId(scenario.equals("foreign") ? "other" : "<default>"));

    // then
    assertThat(rejected.getRejectionType())
        .isEqualTo(
            scenario.equals("missing") || scenario.equals("foreign")
                ? RejectionType.NOT_FOUND
                : RejectionType.INVALID_STATE);
    assertThat(RecordingExporter.jobRecords(JobIntent.TIMED_OUT).withKey(job)).isEmpty();
  }

  @Test
  void shouldRemoveDeadlinesWhenJobsAreCompletedCancelledOrExhausted() {
    // given
    final long completed = createJob();
    ENGINE.jobs().withType("work").withTimeout(1000).activate();
    ENGINE.job().withKey(completed).complete();
    final long cancelled = createJob();
    ENGINE.jobs().withType("work").withTimeout(1000).activate();
    ENGINE.job().withKey(cancelled).cancel();
    final long exhausted = createJob();
    ENGINE.jobs().withType("work").withTimeout(1000).activate();
    ENGINE.job().withKey(exhausted).withRetries(0).withRetryBackoff(100).fail();
    final long witness = createJob();

    // when
    ENGINE.jobs().withType("work").withTimeout(1500).activate();
    RecordingExporter.jobRecords(JobIntent.TIMED_OUT).withKey(witness).getFirst();

    // then
    assertThat(RecordingExporter.jobRecords(JobIntent.TIMED_OUT).withKey(completed)).isEmpty();
    assertThat(RecordingExporter.jobRecords(JobIntent.TIMED_OUT).withKey(cancelled)).isEmpty();
    assertThat(RecordingExporter.jobRecords(JobIntent.TIMED_OUT).withKey(exhausted)).isEmpty();
    assertThat(RecordingExporter.jobRecords(JobIntent.RECURRED_AFTER_BACKOFF).withKey(exhausted))
        .isEmpty();
    assertThat(RecordingExporter.records().onlyCommands().withIntent(JobIntent.TIME_OUT))
        .hasSize(1);
  }

  private long createJob() {
    final long instance = ENGINE.job().withType("work").create().getKey();
    return RecordingExporter.jobRecords(JobIntent.CREATED).withKey(instance).getFirst().getKey();
  }
}
