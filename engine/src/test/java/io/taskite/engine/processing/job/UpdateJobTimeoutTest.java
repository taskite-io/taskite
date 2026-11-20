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
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class UpdateJobTimeoutTest {
  @RegisterExtension final EngineRule ENGINE = EngineRule.singlePartition();

  @ParameterizedTest
  @ValueSource(longs = {10_000, 60_000, Integer.MAX_VALUE})
  void shouldResetDeadlineFromCommandTimeAndPreserveJob(final long timeout) {
    // given
    final long job = createJob();
    final var activated =
        ENGINE
            .jobs()
            .withType("work")
            .withWorker("worker")
            .withWorker("owner", 42)
            .activate()
            .getValue()
            .getJobs()
            .getFirst()
            .record();
    final var before =
        RecordingExporter.jobRecords(JobIntent.CREATED)
            .withKey(job)
            .getFirst()
            .getValue()
            .copy()
            .setWorker(activated.getWorker())
            .setWorkerId(activated.getWorkerId())
            .setWorkerLeaseToken(activated.getWorkerLeaseToken())
            .setTimeout(activated.getTimeout())
            .setDeadline(activated.getDeadline());
    final var queriedBefore = ENGINE.job().withKey(job).query().getValue();

    // when
    final var response = ENGINE.job().withKey(job).withTimeout(timeout).updateTimeout();

    // then
    final var updated =
        RecordingExporter.jobRecords(JobIntent.TIMEOUT_UPDATED).withKey(job).getFirst();
    final var command =
        RecordingExporter.jobRecords(JobIntent.UPDATE_TIMEOUT)
            .onlyCommands()
            .withKey(job)
            .getFirst();
    assertThat(updated.getValue().getDeadline()).isEqualTo(command.getTimestamp() + timeout);
    assertThat(response.getValue().toJson()).isEqualTo(updated.getValue().toJson());
    assertThat(updated.getValue().toJson())
        .isEqualTo(before.copy().setDeadline(updated.getValue().getDeadline()).toJson());
    assertThat(updated.getValue().getWorkerId()).isEqualTo(activated.getWorkerId());
    assertThat(ENGINE.job().withKey(job).query().getValue().toJson())
        .isEqualTo(queriedBefore.copy().setDeadline(updated.getValue().getDeadline()).toJson());
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs()).isEmpty();
    ENGINE.job().withKey(job).complete();
  }

  @Test
  void shouldReplacePreviousDeadlineOnRepeatedUpdatesAndRejectStaleTimeout() {
    // given
    final long job = createJob();
    final var activated =
        ENGINE.jobs().withType("work").activate().getValue().getJobs().getFirst().record();
    ENGINE.job().withKey(job).withTimeout(60_000).updateTimeout();

    // when
    final var latest = ENGINE.job().withKey(job).withTimeout(120_000).updateTimeout().getValue();
    final var rejected =
        ENGINE.executeCommand(
            job,
            ValueType.JOB,
            JobIntent.TIME_OUT,
            new JobRecord().setTenantId("<default>").setDeadline(activated.getDeadline()));

    // then
    assertThat(rejected.getRejectionType()).isEqualTo(RejectionType.INVALID_STATE);
    assertThat(RecordingExporter.jobRecords(JobIntent.TIMEOUT_UPDATED).withKey(job)).hasSize(2);
    assertThat(RecordingExporter.jobRecords(JobIntent.TIMED_OUT).withKey(job)).isEmpty();
    assertThat(ENGINE.job().withKey(job).query().getValue().getDeadline())
        .isEqualTo(latest.getDeadline());
  }

  @Test
  void shouldTimeOutAtShortenedDeadlineAndAllowReactivation() {
    // given
    final long job = createJob();
    ENGINE.jobs().withType("work").withTimeout(60_000).activate();

    // when
    final var updated =
        ENGINE.job().withKey(job).withTimeout(Duration.ofMillis(200)).updateTimeout().getValue();
    final var timedOut = RecordingExporter.jobRecords(JobIntent.TIMED_OUT).withKey(job).getFirst();

    // then
    assertThat(timedOut.getTimestamp()).isGreaterThanOrEqualTo(updated.getDeadline());
    assertThat(timedOut.getValue().getRetries()).isEqualTo(3);
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs())
        .singleElement()
        .satisfies(
            activated -> {
              assertThat(activated.key()).isEqualTo(job);
              assertThat(activated.record().getVariables()).containsEntry("amount", 42);
            });
    ENGINE.job().withKey(job).complete();
  }

  @ParameterizedTest
  @ValueSource(longs = {0, -1, Long.MAX_VALUE})
  void shouldRejectInvalidTimeoutWithoutChangingJob(final long timeout) {
    // given
    final long job = createJob();
    ENGINE.jobs().withType("work").activate();
    final var before = ENGINE.job().withKey(job).query().getValue().toJson();

    // when
    final var rejected =
        ENGINE.job().withKey(job).withTimeout(timeout).expectRejection().updateTimeout();

    // then
    assertThat(rejected.getRejectionType()).isEqualTo(RejectionType.INVALID_ARGUMENT);
    assertThat(rejected.getRejectionReason()).contains("update timeout", Long.toString(job));
    assertThat(ENGINE.job().withKey(job).query().getValue().toJson()).isEqualTo(before);
    assertThat(RecordingExporter.jobRecords(JobIntent.TIMEOUT_UPDATED).withKey(job)).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {"missing", "foreign", "completed", "canceled"})
  void shouldRejectTimeoutUpdateForMissingOrInvisibleJob(final String scenario) {
    // given
    final long existing = createJob();
    final long job = scenario.equals("missing") ? 123 : existing;
    ENGINE.jobs().withType("work").activate();
    if (scenario.equals("completed")) {
      ENGINE.job().withKey(job).complete();
    } else if (scenario.equals("canceled")) {
      ENGINE.job().withKey(job).cancel();
      ENGINE.waitForIdleState(Duration.ofSeconds(5));
    }

    // when
    final var rejected =
        ENGINE
            .job()
            .withKey(job)
            .withTenantId(scenario.equals("foreign") ? "other" : "<default>")
            .withTimeout(60_000)
            .expectRejection()
            .updateTimeout();

    // then
    assertThat(rejected.getRejectionType()).isEqualTo(RejectionType.NOT_FOUND);
    assertThat(RecordingExporter.jobRecords(JobIntent.TIMEOUT_UPDATED).withKey(job)).isEmpty();
    if (scenario.equals("foreign")) {
      assertThat(ENGINE.job().withKey(job).query().getValue().getDeadline()).isPositive();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"activatable", "failed", "exhausted", "timed-out"})
  void shouldRejectTimeoutUpdateForInactiveJob(final String scenario) {
    // given
    final long job = createJob();
    if (scenario.equals("failed") || scenario.equals("exhausted")) {
      ENGINE
          .job()
          .withKey(job)
          .withRetries(scenario.equals("exhausted") ? 0 : 2)
          .withRetryBackoff(60_000)
          .fail();
    } else if (scenario.equals("timed-out")) {
      ENGINE.jobs().withType("work").withTimeout(100).activate();
      RecordingExporter.jobRecords(JobIntent.TIMED_OUT).withKey(job).getFirst();
    }
    final var before = ENGINE.job().withKey(job).query().getValue().toJson();

    // when
    final var rejected =
        ENGINE.job().withKey(job).withTimeout(60_000).expectRejection().updateTimeout();

    // then
    assertThat(rejected.getRejectionType()).isEqualTo(RejectionType.INVALID_STATE);
    assertThat(ENGINE.job().withKey(job).query().getValue().toJson()).isEqualTo(before);
    assertThat(RecordingExporter.jobRecords(JobIntent.TIMEOUT_UPDATED).withKey(job)).isEmpty();
  }

  private long createJob() {
    final long instance =
        ENGINE.job().withType("work").withVariables(Map.of("amount", 42)).create().getKey();
    return RecordingExporter.jobRecords(JobIntent.CREATED).withKey(instance).getFirst().getKey();
  }
}
