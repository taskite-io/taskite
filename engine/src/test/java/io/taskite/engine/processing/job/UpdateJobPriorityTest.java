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
import io.taskite.protocol.impl.record.value.broker.JobBatchRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.intent.broker.IncidentIntent;
import io.taskite.protocol.record.intent.broker.JobIntent;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class UpdateJobPriorityTest {
  @RegisterExtension final EngineRule ENGINE = EngineRule.singlePartition();

  @ParameterizedTest
  @ValueSource(ints = {Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE})
  void shouldUpdatePriorityAndPreserveJobContext(final int priority) {
    // given
    final long job = createJob();
    final var created =
        RecordingExporter.jobRecords(JobIntent.CREATED).withKey(job).getFirst().getValue();
    final var queried = ENGINE.job().withKey(job).query().getValue();
    // Creation normalizes an unowned job's Worker to an empty string in persisted state.
    final var stored = created.copy().setWorker(queried.getWorker());

    // when
    final var response = ENGINE.job().withKey(job).withPriority(priority).updatePriority();

    // then
    final var updated =
        RecordingExporter.jobRecords(JobIntent.PRIORITY_UPDATED).withKey(job).getFirst();
    assertThat(response.getValue().toJson()).isEqualTo(updated.getValue().toJson());
    assertThat(updated.getValue().toJson()).isEqualTo(stored.copy().setPriority(priority).toJson());
    assertThat(ENGINE.job().withKey(job).query().getValue().toJson())
        .isEqualTo(queried.copy().setPriority(priority).toJson());
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs())
        .singleElement()
        .satisfies(
            activated -> {
              assertThat(activated.key()).isEqualTo(job);
              assertThat(activated.record().getPriority()).isEqualTo(priority);
              assertThat(activated.record().getVariables()).containsEntry("amount", 42);
            });
    ENGINE.job().withKey(job).complete();
  }

  @Test
  void shouldReorderPendingJobsAfterRepeatedRaiseLowerResetAndEqualPriorityUpdates() {
    // given
    final long first = createJob();
    final long second = createJob();
    final long third = createJob();
    final long fourth = createJob();
    final long fifth = createJob();

    // when
    ENGINE.job().withKey(first).withPriority(Integer.MAX_VALUE).updatePriority();
    ENGINE.job().withKey(first).withPriority(Integer.MIN_VALUE).updatePriority();
    ENGINE.job().withKey(second).withPriority(-100).updatePriority();
    ENGINE.job().withKey(second).withPriority(0).updatePriority();
    ENGINE.job().withKey(third).withPriority(Integer.MAX_VALUE).updatePriority();
    ENGINE.job().withKey(fourth).withPriority(Integer.MAX_VALUE).updatePriority();
    ENGINE.job().withKey(fourth).withPriority(Integer.MAX_VALUE).updatePriority();
    ENGINE.job().withKey(fifth).withPriority(-1).updatePriority();

    // then
    final var jobs =
        ENGINE.jobs().withType("work").withMaxJobsToActivate(10).activate().getValue().getJobs();
    assertThat(jobs)
        .extracting(JobBatchRecord.ActivatedJob::key)
        .containsExactly(third, fourth, second, fifth, first);
    assertThat(jobs)
        .extracting(job -> job.record().getPriority())
        .containsExactly(Integer.MAX_VALUE, Integer.MAX_VALUE, 0, -1, Integer.MIN_VALUE);
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs()).isEmpty();
  }

  @Test
  void shouldPreserveActivationLeaseAndDeadlineWithoutReactivatingJob() {
    // given
    final long job = createJob();
    ENGINE
        .jobs()
        .withType("work")
        .withWorker("worker")
        .withWorker("owner", 42)
        .withTimeout(60_000)
        .activate();
    final var before = ENGINE.job().withKey(job).query().getValue();

    // when
    ENGINE.job().withKey(job).withPriority(10).updatePriority();

    // then
    final var updated = ENGINE.job().withKey(job).query().getValue();
    assertThat(updated.toJson()).isEqualTo(before.copy().setPriority(10).toJson());
    assertThat(updated.getWorkerId()).isEqualTo("owner");
    assertThat(updated.getWorkerLeaseToken()).isEqualTo(42);
    assertThat(updated.getDeadline()).isEqualTo(before.getDeadline());
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs()).isEmpty();
    ENGINE.job().withKey(job).withWorker("owner", 42).withRetries(2).fail();
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs())
        .singleElement()
        .satisfies(activated -> assertThat(activated.record().getPriority()).isEqualTo(10));
  }

  @Test
  void shouldPreserveTimeoutScheduleAndUseUpdatedPriorityAfterTimeout() {
    // given
    final long job = createJob();
    ENGINE.jobs().withType("work").withTimeout(60_000).activate();
    final var before = ENGINE.job().withKey(job).query().getValue();

    // when
    ENGINE.job().withKey(job).withPriority(Integer.MIN_VALUE).updatePriority();
    ENGINE.job().withKey(job).withTimeout(200).updateTimeout();
    RecordingExporter.jobRecords(JobIntent.TIMED_OUT).withKey(job).getFirst();
    ENGINE.job().withKey(job).withPriority(Integer.MAX_VALUE).updatePriority();

    // then
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs())
        .singleElement()
        .satisfies(
            activated -> {
              assertThat(activated.key()).isEqualTo(job);
              assertThat(activated.record().getPriority()).isEqualTo(Integer.MAX_VALUE);
              assertThat(activated.record().getRetries()).isEqualTo(before.getRetries());
            });
    ENGINE.job().withKey(job).complete();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void shouldKeepFailedJobInactiveUntilBackoffOrIncidentIsResolved(final boolean exhausted) {
    // given
    final long job = createJob();
    ENGINE
        .job()
        .withKey(job)
        .withRetries(exhausted ? 0 : 2)
        .withRetryBackoff(exhausted ? 0 : 60_000)
        .withErrorMessage("temporary")
        .fail();
    final var before = ENGINE.job().withKey(job).query().getValue();

    // when
    ENGINE.job().withKey(job).withPriority(100).updatePriority();

    // then
    assertThat(ENGINE.job().withKey(job).query().getValue().toJson())
        .isEqualTo(before.copy().setPriority(100).toJson());
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs()).isEmpty();
    if (exhausted) {
      final long incident =
          RecordingExporter.incidentRecords(IncidentIntent.CREATED)
              .withJobKey(job)
              .getFirst()
              .getKey();
      assertThat(RecordingExporter.incidentRecords(IncidentIntent.RESOLVED).withKey(incident))
          .isEmpty();
      ENGINE.job().withKey(job).withRetries(2).updateRetries();
      ENGINE.incident().withKey(incident).resolve();
      assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs())
          .singleElement()
          .satisfies(activated -> assertThat(activated.record().getPriority()).isEqualTo(100));
    }
  }

  @Test
  void shouldPreserveBackoffScheduleAndUseUpdatedPriorityOnRetry() {
    // given
    final long job = createJob();
    ENGINE.job().withKey(job).withRetries(2).withRetryBackoff(1000).fail();
    final var before = ENGINE.job().withKey(job).query().getValue();

    // when
    ENGINE.job().withKey(job).withPriority(10).updatePriority();
    RecordingExporter.jobRecords(JobIntent.RECURRED_AFTER_BACKOFF).withKey(job).getFirst();

    // then
    final var recurred =
        RecordingExporter.jobRecords(JobIntent.RECURRED_AFTER_BACKOFF)
            .withKey(job)
            .getFirst()
            .getValue();
    assertThat(recurred.getPriority()).isEqualTo(10);
    assertThat(recurred.getRetries()).isEqualTo(2);
    assertThat(
            RecordingExporter.jobRecords(JobIntent.PRIORITY_UPDATED)
                .withKey(job)
                .getFirst()
                .getValue()
                .getRecurringTime())
        .isEqualTo(before.getRecurringTime());
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs())
        .singleElement()
        .satisfies(activated -> assertThat(activated.record().getPriority()).isEqualTo(10));
  }

  @ParameterizedTest
  @ValueSource(strings = {"missing", "foreign", "completed", "canceled"})
  void shouldRejectPriorityUpdateForMissingOrInvisibleJob(final String scenario) {
    // given
    final long existing = createJob();
    final long job = scenario.equals("missing") ? 123 : existing;
    final var before = ENGINE.job().withKey(existing).query().getValue();
    if (scenario.equals("completed")) {
      ENGINE.job().withKey(job).complete();
    } else if (scenario.equals("canceled")) {
      ENGINE.job().withKey(job).cancel();
      ENGINE.waitForIdleState(Duration.ofSeconds(5));
    }
    final String tenant = scenario.equals("foreign") ? "other" : "<default>";

    // when
    final var rejected =
        ENGINE
            .job()
            .withKey(job)
            .withTenantId(tenant)
            .withPriority(10)
            .expectRejection()
            .updatePriority();

    // then
    assertThat(rejected.getRejectionType()).isEqualTo(RejectionType.NOT_FOUND);
    assertThat(rejected.getRejectionReason())
        .isEqualTo(
            String.format(
                "Expected to update priority for job with key '%d' for tenant '%s', but no such job was found",
                job, tenant));
    assertThat(RecordingExporter.jobRecords(JobIntent.PRIORITY_UPDATED).withKey(job)).isEmpty();
    if (scenario.equals("foreign")) {
      assertThat(ENGINE.job().withKey(job).query().getValue().toJson()).isEqualTo(before.toJson());
    }
  }

  private long createJob() {
    final long instance =
        ENGINE.job().withType("work").withVariables(Map.of("amount", 42)).create().getKey();
    return RecordingExporter.jobRecords(JobIntent.CREATED).withKey(instance).getFirst().getKey();
  }
}
