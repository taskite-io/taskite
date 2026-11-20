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
import io.taskite.engine.util.record.RecordStream;
import io.taskite.engine.util.record.RecordingExporter;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.IncidentIntent;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.protocol.record.value.broker.JobYieldStatus;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class YieldJobTest {
  @RegisterExtension final EngineRule ENGINE = EngineRule.singlePartitionWithWorkerLeases();

  @Test
  void shouldFenceSessionBeforeReleasingJobsAndCompleteOnlyAfterAllReleases() {
    // given
    synchronize();
    final long first = createJob();
    final long second = createJob();
    ENGINE.jobs().withType("work").withWorker("old", 42).withMaxJobsToActivate(2).activate();
    final var plan = plan(10, "old", 42);

    // when
    ENGINE.executeCommand(0, ValueType.JOB_YIELD, JobYieldIntent.FENCE_SESSION, plan);
    final var completion = yieldRecords(JobYieldIntent.YIELD_COMPLETED).getFirst();

    // then
    assertThat(RecordingExporter.jobRecords(JobIntent.YIELDED))
        .extracting(r -> r.getKey())
        .containsExactlyInAnyOrder(first, second);
    final var fenced = yieldRecords(JobYieldIntent.SESSION_FENCED).getFirst();
    assertThat(completion.getValue().getStatus()).isEqualTo(JobYieldStatus.COMPLETED);
    assertThat(completion.getValue().getReleasedJobCount()).isEqualTo(2);
    assertThat(RecordingExporter.jobRecords(JobIntent.YIELDED))
        .allSatisfy(
            released -> {
              assertThat(released.getPosition())
                  .isBetween(fenced.getPosition() + 1, completion.getPosition() - 1);
              assertThat(released.getValue().getRetries()).isEqualTo(3);
              assertThat(released.getValue().getDeadline()).isZero();
              final var current = ENGINE.job().withKey(released.getKey()).query().getValue();
              assertThat(current.getWorkerId()).isEmpty();
              assertThat(current.getWorkerLeaseToken()).isZero();
            });
    assertThat(
            ENGINE
                .job()
                .withKey(first)
                .withWorker("old", 42)
                .expectRejection()
                .complete()
                .getRejectionType())
        .isEqualTo(RejectionType.STALE_WORKER_SESSION);
    assertThat(
            ENGINE
                .job()
                .withKey(first)
                .withWorker("old", 42)
                .withRetries(0)
                .expectRejection()
                .fail()
                .getRejectionType())
        .isEqualTo(RejectionType.STALE_WORKER_SESSION);
    assertThat(
            ENGINE
                .jobs()
                .withType("work")
                .withWorker("old", 42)
                .expectRejection()
                .activate()
                .getRejectionType())
        .isEqualTo(RejectionType.STALE_WORKER_SESSION);
    assertThat(
            ENGINE
                .jobs()
                .withType("work")
                .withWorker("new", 43)
                .withMaxJobsToActivate(2)
                .activate()
                .getValue()
                .getJobs())
        .extracting(activated -> activated.key())
        .containsExactly(first, second);
    assertThat(
            ENGINE
                .job()
                .withKey(first)
                .withWorker("old", 42)
                .expectRejection()
                .complete()
                .getRejectionType())
        .isEqualTo(RejectionType.STALE_WORKER_SESSION);
    ENGINE.job().withKey(first).withWorker("new", 43).complete();
    ENGINE.job().withKey(second).withWorker("new", 43).complete();
  }

  @Test
  void shouldMakeRepeatedFenceIdempotentAndRejectRepeatedYield() {
    // given
    synchronize();
    final long job = createJob();
    final var activated =
        ENGINE
            .jobs()
            .withType("work")
            .withWorker("old", 42)
            .activate()
            .getValue()
            .getJobs()
            .getFirst()
            .record();

    final var plan = plan(10, "old", 42);

    // when
    ENGINE.executeCommand(0, ValueType.JOB_YIELD, JobYieldIntent.FENCE_SESSION, plan);
    yieldRecords(JobYieldIntent.YIELD_COMPLETED).getFirst();
    ENGINE.awaitProcessingOf(
        ENGINE.writeCommand(0, ValueType.JOB_YIELD, JobYieldIntent.FENCE_SESSION, plan));
    final var rejected =
        ENGINE.executeCommand(job, ValueType.JOB, JobIntent.YIELD, activated.copy());

    // then
    assertThat(rejected.getRejectionType()).isEqualTo(RejectionType.INVALID_STATE);
    assertThat(yieldRecords(JobYieldIntent.SESSION_FENCED)).hasSize(1);
    assertThat(RecordingExporter.jobRecords(JobIntent.YIELDED).withKey(job)).hasSize(1);
    assertThat(yieldRecords(JobYieldIntent.YIELD_COMPLETED)).hasSize(1);
    assertThat(
            RecordingExporter.records()
                .withValueType(ValueType.JOB)
                .withIntent(JobIntent.YIELD)
                .withKey(job)
                .onlyCommandRejections()
                .getFirst()
                .getRejectionType())
        .isEqualTo(RejectionType.INVALID_STATE);
    assertThat(
            ENGINE
                .executeCommand(
                    0,
                    ValueType.JOB_YIELD,
                    JobYieldIntent.FENCE_SESSION,
                    plan.copy().setWorkerLeaseToken(43))
                .getRejectionType())
        .isEqualTo(RejectionType.ALREADY_EXISTS);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void shouldYieldActivatedJobWithoutPendingPlanOrMatchingWorkerIdentity(
      final boolean withCompletedPlan) {
    // given
    synchronize();
    final var completed =
        plan(10, "unrelated", 99).setStatus(JobYieldStatus.COMPLETED).setReleasedJobCount(7);
    if (withCompletedPlan) {
      ENGINE.executeCommand(0, ValueType.JOB_YIELD, JobYieldIntent.FENCE_SESSION, completed);
    }
    final long job = createJob();
    ENGINE.jobs().withType("work").withWorker("old", 42).activate();

    // when
    final var response =
        ENGINE.executeCommand(
            job,
            ValueType.JOB,
            JobIntent.YIELD,
            new JobRecord()
                .setTenantId("<default>")
                .setWorkerId("delivery-failed")
                .setWorkerLeaseToken(9999));

    // then
    assertThat(response.getRecordType()).isEqualTo(RecordType.EVENT);
    assertThat(response.getIntent()).isEqualTo(JobIntent.YIELDED);
    final var yielded =
        RecordingExporter.jobRecords(JobIntent.YIELDED).withKey(job).getFirst().getValue();
    assertThat(yielded.getWorkerId()).isEqualTo("old");
    assertThat(yielded.getWorkerLeaseToken()).isEqualTo(42);
    assertThat(yielded.getRetries()).isEqualTo(3);
    assertThat(yielded.getDeadline()).isZero();
    final var current = ENGINE.job().withKey(job).query().getValue();
    assertThat(current.getVariables()).containsExactlyInAnyOrderEntriesOf(Map.of("amount", 42));
    assertThat(current.getWorker()).isEmpty();
    assertThat(current.getWorkerId()).isEmpty();
    assertThat(current.getWorkerLeaseToken()).isZero();
    assertThat(current.getDeadline()).isZero();
    assertThat(ENGINE.jobs().withType("work").withWorker("new", 43).activate().getValue().getJobs())
        .singleElement()
        .satisfies(activated -> assertThat(activated.key()).isEqualTo(job));
  }

  @ParameterizedTest
  @ValueSource(strings = {"activatable", "failed"})
  void shouldRejectYieldForInactiveJobBeforeLookingUpPlan(final String scenario) {
    // given
    synchronize();
    final long job = createJob();
    if (scenario.equals("failed")) {
      ENGINE.jobs().withType("work").withWorker("old", 42).activate();
      ENGINE
          .job()
          .withKey(job)
          .withWorker("old", 42)
          .withRetries(2)
          .withRetryBackoff(60_000)
          .fail();
    }
    final var before = ENGINE.job().withKey(job).query().getValue().toJson();

    // when
    final var rejected =
        ENGINE.executeCommand(
            job, ValueType.JOB, JobIntent.YIELD, new JobRecord().setTenantId("<default>"));

    // then
    assertThat(rejected.getRecordType()).isEqualTo(RecordType.COMMAND_REJECTION);
    assertThat(rejected.getRejectionType()).isEqualTo(RejectionType.INVALID_STATE);
    assertThat(rejected.getRejectionReason())
        .isEqualTo(
            String.format(
                "Expected to yield job with key '%d' for tenant '<default>' in state 'ACTIVATED', but it is in state '%s'",
                job, scenario.equals("failed") ? "FAILED" : "ACTIVATABLE"));
    final var persisted =
        RecordingExporter.records()
            .withValueType(ValueType.JOB)
            .withIntent(JobIntent.YIELD)
            .withKey(job)
            .onlyCommandRejections()
            .getFirst();
    assertThat(persisted.getRejectionType()).isEqualTo(rejected.getRejectionType());
    assertThat(persisted.getRejectionReason()).isEqualTo(rejected.getRejectionReason());
    assertThat(ENGINE.job().withKey(job).query().getValue().toJson()).isEqualTo(before);
    assertThat(RecordingExporter.jobRecords(JobIntent.YIELDED).withKey(job)).isEmpty();
    if (scenario.equals("failed")) {
      assertThat(
              ENGINE.jobs().withType("work").withWorker("new", 43).activate().getValue().getJobs())
          .isEmpty();
    } else {
      assertThat(
              ENGINE.jobs().withType("work").withWorker("new", 43).activate().getValue().getJobs())
          .singleElement()
          .satisfies(activated -> assertThat(activated.key()).isEqualTo(job));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"missing", "foreign", "completed"})
  void shouldRejectYieldForMissingForeignOrCompletedJob(final String scenario) {
    // given
    synchronize();
    final long existing = createJob();
    final long job = scenario.equals("missing") ? 123 : existing;
    if (!scenario.equals("missing")) {
      ENGINE.jobs().withType("work").withWorker("old", 42).activate();
      if (scenario.equals("completed")) {
        ENGINE.job().withKey(job).withWorker("old", 42).complete();
      }
    }

    // when
    final var rejected =
        ENGINE.executeCommand(
            job,
            ValueType.JOB,
            JobIntent.YIELD,
            new JobRecord()
                .setTenantId(scenario.equals("foreign") ? "other" : "<default>")
                .setWorkerId("old")
                .setWorkerLeaseToken(42));

    // then
    assertThat(rejected.getRejectionType()).isEqualTo(RejectionType.NOT_FOUND);
    assertThat(rejected.getRejectionReason())
        .isEqualTo(
            String.format(
                "Expected to yield job with key '%d' for tenant '%s', but no such job was found",
                job, scenario.equals("foreign") ? "other" : "<default>"));
    assertThat(
            RecordingExporter.records()
                .withValueType(ValueType.JOB)
                .withIntent(JobIntent.YIELD)
                .withKey(job)
                .onlyCommandRejections()
                .getFirst()
                .getRejectionType())
        .isEqualTo(RejectionType.NOT_FOUND);
    assertThat(RecordingExporter.jobRecords(JobIntent.YIELDED).withKey(job)).isEmpty();
    if (scenario.equals("foreign")) {
      assertThat(ENGINE.job().withKey(job).query().getValue().getWorkerLeaseToken()).isEqualTo(42);
      assertThat(
              ENGINE.jobs().withType("work").withWorker("new", 43).activate().getValue().getJobs())
          .isEmpty();
    }
  }

  @Test
  void shouldKeepRecoveryBarrierClosedUntilValidatedFinalPage() {
    // given
    final long job = createJob();
    assertThat(
            ENGINE
                .jobs()
                .withType("work")
                .withWorker("new", 43)
                .expectRejection()
                .activate()
                .getRejectionType())
        .isEqualTo(RejectionType.WORKER_SESSION_NOT_READY);
    final var page =
        new JobYieldRecord()
            .setClusterId("demo")
            .setPartitionId(1)
            .setSyncCursor(10)
            .setSessions(List.of(plan(10, "old", 42)));
    ENGINE.executeCommand(0, ValueType.JOB_YIELD, JobYieldIntent.SYNC_SESSIONS, page);

    // when
    ENGINE.awaitProcessingOf(
        ENGINE.writeCommand(
            0,
            ValueType.JOB_YIELD,
            JobYieldIntent.SYNC_SESSIONS,
            page.copy().setSyncComplete(true)));
    final var invalid =
        ENGINE.executeCommand(
            0,
            ValueType.JOB_YIELD,
            JobYieldIntent.SYNC_SESSIONS,
            page.copy()
                .setPreviousSyncCursor(10)
                .setSyncCursor(20)
                .setSessions(List.of(plan(20, "other", 44).setTenantId("")))
                .setSyncComplete(true));

    // then
    assertThat(invalid.getRejectionType()).isEqualTo(RejectionType.INVALID_ARGUMENT);
    assertThat(yieldRecords(JobYieldIntent.SESSIONS_SYNCED)).hasSize(1);
    assertThat(
            ENGINE
                .jobs()
                .withType("work")
                .withWorker("new", 43)
                .expectRejection()
                .activate()
                .getRejectionType())
        .isEqualTo(RejectionType.WORKER_SESSION_NOT_READY);
    ENGINE.executeCommand(
        0,
        ValueType.JOB_YIELD,
        JobYieldIntent.SYNC_SESSIONS,
        page.copy().setPreviousSyncCursor(10).setSessions(List.of()).setSyncComplete(true));
    assertThat(
            ENGINE
                .jobs()
                .withType("work")
                .withWorker("old", 42)
                .expectRejection()
                .activate()
                .getRejectionType())
        .isEqualTo(RejectionType.STALE_WORKER_SESSION);
    assertThat(ENGINE.jobs().withType("work").withWorker("new", 43).activate().getValue().getJobs())
        .singleElement()
        .satisfies(activated -> assertThat(activated.key()).isEqualTo(job));
  }

  @ParameterizedTest
  @ValueSource(strings = {"timeout", "failure", "backoff", "incident"})
  void shouldReleaseOwnershipBeforeRetryAndRejectLateWorkerResults(final String scenario) {
    // given
    synchronize();
    final long job = createJob();
    ENGINE
        .jobs()
        .withType("work")
        .withWorker("old", 42)
        .withTimeout(scenario.equals("timeout") ? 100 : 30_000)
        .activate();

    // when
    switch (scenario) {
      case "timeout" -> RecordingExporter.jobRecords(JobIntent.TIMED_OUT).withKey(job).getFirst();
      case "failure" -> ENGINE.job().withKey(job).withWorker("old", 42).withRetries(2).fail();
      case "backoff" -> {
        ENGINE.job().withKey(job).withWorker("old", 42).withRetries(2).withRetryBackoff(100).fail();
        RecordingExporter.jobRecords(JobIntent.RECURRED_AFTER_BACKOFF).withKey(job).getFirst();
      }
      case "incident" -> {
        ENGINE.job().withKey(job).withWorker("old", 42).withRetries(0).fail();
        ENGINE.job().withKey(job).withRetries(2).updateRetries();
        ENGINE
            .incident()
            .withKey(
                RecordingExporter.incidentRecords(IncidentIntent.CREATED)
                    .withJobKey(job)
                    .getFirst()
                    .getKey())
            .resolve();
      }
      default -> throw new AssertionError(scenario);
    }

    // then
    final var released = ENGINE.job().withKey(job).query().getValue();
    assertThat(released.getWorker()).isEmpty();
    assertThat(released.getWorkerId()).isEmpty();
    assertThat(released.getWorkerLeaseToken()).isZero();
    assertThat(
            ENGINE
                .job()
                .withKey(job)
                .withWorker("old", 42)
                .expectRejection()
                .complete()
                .getRejectionType())
        .isEqualTo(RejectionType.STALE_WORKER_SESSION);
    assertThat(
            ENGINE
                .job()
                .withKey(job)
                .withWorker("old", 42)
                .withRetries(0)
                .expectRejection()
                .fail()
                .getRejectionType())
        .isEqualTo(RejectionType.STALE_WORKER_SESSION);
    assertThat(ENGINE.job().withKey(job).expectRejection().complete().getRejectionType())
        .isEqualTo(RejectionType.STALE_WORKER_SESSION);
    assertThat(ENGINE.jobs().withType("work").withWorker("new", 43).activate().getValue().getJobs())
        .singleElement()
        .satisfies(
            activated ->
                assertThat(activated.record().getRetries())
                    .isEqualTo(scenario.equals("timeout") ? 3 : 2));
    ENGINE.job().withKey(job).withWorker("new", 43).complete();
  }

  @Test
  void shouldRejectControlCommandsWhenWorkerLeaseFeatureIsDisabled() {
    try (final var disabled = EngineRule.singlePartition()) {
      disabled.start();
      for (final var intent : List.of(JobYieldIntent.FENCE_SESSION, JobYieldIntent.SYNC_SESSIONS)) {
        assertThat(
                disabled
                    .executeCommand(0, ValueType.JOB_YIELD, intent, plan(10, "old", 42))
                    .getRejectionType())
            .isEqualTo(RejectionType.FEATURE_DISABLED);
      }
    }
  }

  private void synchronize() {
    ENGINE.executeCommand(
        0,
        ValueType.JOB_YIELD,
        JobYieldIntent.SYNC_SESSIONS,
        new JobYieldRecord().setSyncComplete(true));
  }

  private long createJob() {
    final long instance =
        ENGINE.job().withType("work").withVariables(Map.of("amount", 42)).create().getKey();
    return RecordingExporter.jobRecords(JobIntent.CREATED).withKey(instance).getFirst().getKey();
  }

  private static JobYieldRecord plan(final long id, final String worker, final long token) {
    return new JobYieldRecord()
        .setYieldId(id)
        .setClusterId("demo")
        .setPartitionId(1)
        .setTenantId("<default>")
        .setWorkerId(worker)
        .setWorkerLeaseToken(token);
  }

  private static RecordStream<JobYieldRecord, ?> yieldRecords(final JobYieldIntent intent) {
    return RecordingExporter.records(ValueType.JOB_YIELD, JobYieldRecord.class)
        .withIntent(intent)
        .onlyEvents();
  }
}
