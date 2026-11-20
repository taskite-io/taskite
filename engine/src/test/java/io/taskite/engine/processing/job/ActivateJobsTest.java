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
import io.taskite.protocol.record.intent.broker.JobBatchIntent;
import io.taskite.protocol.record.intent.broker.JobIntent;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ActivateJobsTest {
  @RegisterExtension final EngineRule ENGINE = EngineRule.singlePartition();

  @BeforeEach
  void registerAdditionalSubscriptions() {
    ENGINE.createSubscription("other-work", "work", "other");
    ENGINE.createSubscription("different", "different", "<default>");
  }

  @Test
  void shouldSelectOnlyMatchingTenantAndTypeAndHonorBatchLimit() {
    // given
    final long foreign = create("process", "other", Map.of());
    final long different = create("different", "<default>", Map.of());
    final long first = create("process", "<default>", Map.of());
    final long second = create("process", "<default>", Map.of());
    final long third = create("process", "<default>", Map.of());

    // when
    final var batch = ENGINE.jobs().withType("work").withMaxJobsToActivate(2).activate();

    // then
    assertThat(batch.getValue().getJobs())
        .extracting(JobBatchRecord.ActivatedJob::key)
        .containsExactly(first, second);
    assertThat(
            ENGINE.jobs().withType("work").withMaxJobsToActivate(2).activate().getValue().getJobs())
        .extracting(JobBatchRecord.ActivatedJob::key)
        .containsExactly(third);
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs()).isEmpty();
    assertThat(ENGINE.jobs().withType("work").withTenantId("other").activate().getValue().getJobs())
        .extracting(JobBatchRecord.ActivatedJob::key)
        .containsExactly(foreign);
    assertThat(ENGINE.jobs().withType("different").activate().getValue().getJobs())
        .extracting(JobBatchRecord.ActivatedJob::key)
        .containsExactly(different);
  }

  @Test
  void shouldActivateHigherPriorityFirstAndPreservePriorityOnRetry() {

    final long negative = create("priority", "<default>", Map.of("jobPriority", -1));
    final long normal = create("process", "<default>", Map.of());
    final long high = create("priority", "<default>", Map.of("jobPriority", 10));
    final long equal = create("priority", "<default>", Map.of("jobPriority", 10));

    final var first = ENGINE.jobs().withType("work").withMaxJobsToActivate(2).activate().getValue();
    assertThat(first.getJobs())
        .extracting(JobBatchRecord.ActivatedJob::key)
        .containsExactly(high, equal);
    assertThat(first.getJobs())
        .allSatisfy(job -> assertThat(job.record().getPriority()).isEqualTo(10));
    ENGINE.job().withKey(high).withRetries(2).fail();
    final var second =
        ENGINE.jobs().withType("work").withMaxJobsToActivate(3).activate().getValue();
    assertThat(second.getJobs())
        .extracting(JobBatchRecord.ActivatedJob::key)
        .containsExactly(high, normal, negative);
    assertThat(second.getJobs())
        .extracting(job -> job.record().getPriority())
        .containsExactly(10, 0, -1);
    assertThat(ENGINE.job().withKey(high).query().getValue().getPriority()).isEqualTo(10);
  }

  @Test
  void shouldReturnJobContextAndUseOneProcessingDeadlineForTheBatch() {
    // given
    final long first = create("process", "<default>", Map.of("amount", 42));
    final long second = create("process", "<default>", Map.of("amount", 7));
    final long before = System.currentTimeMillis();

    // when
    final var batch =
        ENGINE
            .jobs()
            .withType("work")
            .withWorker("worker")
            .withTimeout(30_000)
            .withMaxJobsToActivate(2)
            .activate();
    final long after = System.currentTimeMillis();

    // then
    final var value = batch.getValue();
    assertThat(value.getType()).isEqualTo("work");
    assertThat(value.getWorker()).isEqualTo("worker");
    assertThat(value.getTimeout()).isEqualTo(30_000);
    assertThat(value.getMaxJobsToActivate()).isEqualTo(2);
    assertThat(value.getJobs())
        .extracting(JobBatchRecord.ActivatedJob::key)
        .containsExactly(first, second);
    assertThat(value.getJobs())
        .allSatisfy(
            activated -> {
              final var created =
                  RecordingExporter.jobRecords(JobIntent.CREATED)
                      .withKey(activated.key())
                      .getFirst()
                      .getValue();
              assertThat(activated.record().getDeadline())
                  .isBetween(before + 30_000, after + 30_000);
              assertThat(activated.record().getWorker()).isEqualTo("worker");
              assertThat(activated.record().getRetries()).isEqualTo(3);

              assertThat(ENGINE.job().withKey(activated.key()).query().getValue().getDeadline())
                  .isEqualTo(activated.record().getDeadline());
            });
    assertThat(value.getJobs())
        .extracting(job -> job.record().getDeadline())
        .containsOnly(value.getJobs().getFirst().record().getDeadline());
    assertThat(RecordingExporter.jobBatchRecords(JobBatchIntent.ACTIVATED).withKey(batch.getKey()))
        .hasSize(1);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void shouldFetchCurrentScopeVariablesWithoutPersistingProjection(final boolean project) {
    // given

    final long job =
        create("process", "<default>", Map.of("amount", 42, "private", "local", "root", true));

    // when
    final var client = ENGINE.jobs().withType("work");
    if (project) {
      client.withFetchVariables("amount", "missing");
    }
    final var activated = client.activate().getValue().getJobs().getFirst();

    // then
    assertThat(activated.record().getVariables())
        .containsExactlyInAnyOrderEntriesOf(
            project
                ? Map.of("amount", 42)
                : Map.of("amount", 42, "private", "local", "root", true));
    assertThat(ENGINE.job().withKey(job).query().getValue().getVariables())
        .containsExactlyInAnyOrderEntriesOf(Map.of("amount", 42, "private", "local", "root", true));
    ENGINE.job().withKey(job).withRetries(2).fail();
    assertThat(
            ENGINE
                .jobs()
                .withType("work")
                .activate()
                .getValue()
                .getJobs()
                .getFirst()
                .record()
                .getVariables())
        .containsExactlyInAnyOrderEntriesOf(Map.of("amount", 42, "private", "local", "root", true));
  }

  @Test
  void shouldExcludeActivatedBackedOffExhaustedCompletedAndCancelledJobs() {
    // given
    final long activated = create("process", "<default>", Map.of());
    ENGINE.jobs().withType("work").activate();
    final long backedOff = create("process", "<default>", Map.of());
    ENGINE.job().withKey(backedOff).withRetries(2).withRetryBackoff(60_000).fail();
    final long exhausted = create("process", "<default>", Map.of());
    ENGINE.job().withKey(exhausted).withRetries(0).fail();
    final long completed = create("process", "<default>", Map.of());
    ENGINE.job().withKey(completed).complete();
    final long cancelled = create("process", "<default>", Map.of());
    ENGINE.job().withKey(cancelled).cancel();
    final long available = create("process", "<default>", Map.of());

    // when
    final var batch = ENGINE.jobs().withType("work").withMaxJobsToActivate(10).activate();

    // then
    assertThat(batch.getValue().getJobs())
        .extracting(JobBatchRecord.ActivatedJob::key)
        .containsExactly(available);
    assertThat(ENGINE.job().withKey(activated).query().getValue().getDeadline()).isPositive();
    assertThat(ENGINE.job().withKey(backedOff).query().getValue().getRecurringTime()).isPositive();
    assertThat(ENGINE.job().withKey(exhausted).query().getValue().getRetries()).isZero();
  }

  @Test
  void shouldReturnEmptyBatchForUnknownTypeOrTenant() {
    // given
    final long job = create("process", "<default>", Map.of());

    // when / then
    assertThat(ENGINE.jobs().withType("missing").activate().getValue().getJobs()).isEmpty();
    assertThat(
            ENGINE.jobs().withType("work").withTenantId("missing").activate().getValue().getJobs())
        .isEmpty();
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs())
        .extracting(JobBatchRecord.ActivatedJob::key)
        .containsExactly(job);
  }

  @ParameterizedTest
  @CsvSource({"work,0,1", "work,-1,1", "work,1,0", "work,1,-1", "' ',1,1", "'',1,1"})
  void shouldRejectInvalidActivationWithoutChangingJob(
      final String type, final long timeout, final int maximum) {
    // given
    final long job = create("process", "<default>", Map.of());

    // when
    final var rejection =
        ENGINE
            .jobs()
            .withType(type)
            .withTimeout(timeout)
            .withMaxJobsToActivate(maximum)
            .expectRejection()
            .activate();

    // then
    assertThat(rejection.getRejectionType()).isEqualTo(RejectionType.INVALID_ARGUMENT);
    assertThat(rejection.getRejectionReason())
        .isEqualTo(
            String.format(
                "Expected to activate jobs with non-blank type and tenant and positive timeout and batch size,"
                    + " but received type '%s', tenant '<default>', timeout '%d', and maxJobsToActivate '%d'",
                type, timeout, maximum));
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs())
        .extracting(JobBatchRecord.ActivatedJob::key)
        .containsExactly(job);
  }

  @Test
  void shouldRejectDeadlineOverflowAndBlankTenantWithoutChangingJob() {
    // given
    final long job = create("process", "<default>", Map.of());

    // when / then
    final var overflow =
        ENGINE.jobs().withType("work").withTimeout(Long.MAX_VALUE).expectRejection().activate();
    assertThat(overflow.getRejectionType()).isEqualTo(RejectionType.INVALID_ARGUMENT);
    assertThat(overflow.getRejectionReason())
        .isEqualTo(
            "Expected to activate jobs of type 'work' for tenant '<default>' with timeout '"
                + Long.MAX_VALUE
                + "', but adding the timeout to the processing time exceeds the signed 64-bit deadline range");
    assertThat(
            ENGINE
                .jobs()
                .withType("work")
                .withTenantId("")
                .expectRejection()
                .activate()
                .getRejectionType())
        .isEqualTo(RejectionType.INVALID_ARGUMENT);
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs())
        .extracting(JobBatchRecord.ActivatedJob::key)
        .containsExactly(job);
  }

  @Test
  void shouldRejectOversizedJobAndAllowVariableProjection() {
    // given
    final long job =
        create("process", "<default>", Map.of("large", "x".repeat(3 * 1024 * 1024), "small", 1));

    // when
    final var rejected = ENGINE.jobs().withType("work").expectRejection().activate();

    // then
    assertThat(rejected.getRejectionType()).isEqualTo(RejectionType.EXCEEDED_BATCH_RECORD_SIZE);
    assertThat(rejected.getRejectionReason())
        .startsWith(
            String.format(
                "Expected to activate job with key '%d' of type 'work' for tenant '<default>',"
                    + " but its activation batch requires '",
                job))
        .endsWith("bytes and exceeds the limit of '3145728' bytes; reduce fetchVariables");
    final var projected =
        ENGINE
            .jobs()
            .withType("work")
            .withFetchVariables("small")
            .activate()
            .getValue()
            .getJobs()
            .getFirst();
    assertThat(projected.key()).isEqualTo(job);
    assertThat(projected.record().getVariables()).containsExactlyEntriesOf(Map.of("small", 1));
  }

  @Test
  void shouldStopBeforeTransportLimitAndLeaveRemainingJobAvailable() {
    // given
    final var variables = Map.<String, Object>of("data", "x".repeat(1200 * 1024));
    final long first = create("process", "<default>", variables);
    final long second = create("process", "<default>", variables);
    final long third = create("process", "<default>", variables);

    // when / then
    assertThat(
            ENGINE.jobs().withType("work").withMaxJobsToActivate(3).activate().getValue().getJobs())
        .extracting(JobBatchRecord.ActivatedJob::key)
        .containsExactly(first, second);
    assertThat(
            ENGINE.jobs().withType("work").withMaxJobsToActivate(3).activate().getValue().getJobs())
        .extracting(JobBatchRecord.ActivatedJob::key)
        .containsExactly(third);
  }

  private long create(
      final String process, final String tenant, final Map<String, Object> variables) {
    final var client =
        ENGINE
            .job()
            .withType(process.equals("different") ? "different" : "work")
            .withTenantId(tenant)
            .withVariables(variables);
    final long key = client.create().getKey();
    if (variables.containsKey("jobPriority")) {
      ENGINE
          .job()
          .withKey(key)
          .withTenantId(tenant)
          .withPriority(((Number) variables.get("jobPriority")).intValue())
          .updatePriority();
    } else {
      ENGINE.job().withKey(key).withTenantId(tenant).withPriority(0).updatePriority();
    }
    return key;
  }
}
