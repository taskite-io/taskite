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
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.intent.broker.IncidentIntent;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.protocol.record.value.broker.ErrorType;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class FailJobTest {
  @RegisterExtension final EngineRule ENGINE = EngineRule.singlePartition();

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void shouldMakeFailedJobImmediatelyActivatableAndKeepVariablesLocal(final boolean activate) {
    // given
    final long instance =
        ENGINE.job().withType("work").withVariables(Map.of("amount", 1)).create().getKey();
    final var created =
        RecordingExporter.jobRecords(JobIntent.CREATED).withKey(instance).getFirst();
    if (activate) {
      ENGINE.jobs().withType("work").activate();
    }

    // when
    ENGINE
        .job()
        .withKey(created.getKey())
        .withRetries(2)
        .withErrorMessage("temporary")
        .withVariables(Map.of("progress", 7))
        .fail();

    // then
    final var failed =
        RecordingExporter.jobRecords(JobIntent.FAILED)
            .withKey(created.getKey())
            .getFirst()
            .getValue();
    assertThat(failed.getRetries()).isEqualTo(2);
    assertThat(failed.getErrorMessage()).isEqualTo("temporary");
    assertThat(failed.getDeadline()).isZero();
    assertThat(failed.getVariables()).containsEntry("amount", 1);
    assertThat(
            RecordingExporter.variableRecords().withJobKey(instance).withName("progress").limit(1))
        .singleElement()
        .satisfies(
            variable -> {
              assertThat(variable.getValue().getJobKey()).isEqualTo(created.getKey());
              assertThat(variable.getValue().getValue()).isEqualTo(7);
            });
    assertThat(RecordingExporter.variableRecords().withJobKey(instance).withJobKey(instance))
        .extracting(r -> r.getValue().getName())
        .containsExactly("amount", "progress");
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs())
        .singleElement()
        .satisfies(
            activated -> {
              assertThat(activated.key()).isEqualTo(created.getKey());
              assertThat(activated.record().getRetries()).isEqualTo(2);
              assertThat(activated.record().getVariables())
                  .containsExactlyInAnyOrderEntriesOf(Map.of("amount", 1, "progress", 7));
            });
    assertThat(
            RecordingExporter.incidentRecords(IncidentIntent.CREATED).withJobKey(created.getKey()))
        .isEmpty();
  }

  @Test
  void shouldCreateIncidentWithJobContextWhenRetriesAreExhausted() {
    // given
    final long instance = ENGINE.job().withType("work").create().getKey();
    final var created =
        RecordingExporter.jobRecords(JobIntent.CREATED).withKey(instance).getFirst();

    // when
    ENGINE.job().withKey(created.getKey()).withRetries(0).withErrorMessage("exhausted").fail();

    // then
    final var incident =
        RecordingExporter.incidentRecords(IncidentIntent.CREATED)
            .withJobKey(created.getKey())
            .getFirst()
            .getValue();

    assertThat(incident.getTenantId()).isEqualTo(created.getValue().getTenantId());
    assertThat(incident.getErrorType()).isEqualTo(ErrorType.JOB_NO_RETRIES);
    assertThat(incident.getErrorMessage()).isEqualTo("exhausted");
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs()).isEmpty();
    ENGINE.job().withKey(created.getKey()).withRetries(0).expectRejection().fail();
    assertThat(
            RecordingExporter.records()
                .withKey(created.getKey())
                .withIntent(JobIntent.FAIL)
                .onlyCommandRejections()
                .getFirst()
                .getRejectionType())
        .isEqualTo(RejectionType.INVALID_STATE);
    assertThat(
            RecordingExporter.incidentRecords(IncidentIntent.CREATED).withJobKey(created.getKey()))
        .hasSize(1);
  }

  @ParameterizedTest
  @CsvSource({"-1, 0, non-negative retries", "1, -1, non-negative retry backoff"})
  void shouldRejectNegativeRetriesOrBackoff(
      final int retries, final long backoff, final String reason) {
    // given
    final long instance = ENGINE.job().withType("work").create().getKey();
    final long job =
        RecordingExporter.jobRecords(JobIntent.CREATED).withKey(instance).getFirst().getKey();

    // when
    ENGINE
        .job()
        .withKey(job)
        .withRetries(retries)
        .withRetryBackoff(backoff)
        .expectRejection()
        .fail();

    // then
    final var rejected =
        RecordingExporter.records()
            .withKey(job)
            .withIntent(JobIntent.FAIL)
            .onlyCommandRejections()
            .getFirst();
    assertThat(rejected.getRejectionType()).isEqualTo(RejectionType.INVALID_ARGUMENT);
    assertThat(rejected.getRejectionReason())
        .isEqualTo(
            String.format(
                "Expected to fail job with key '%d' for tenant '<default>' with %s, but received '-1'",
                job, reason));
    assertThat(RecordingExporter.jobRecords(JobIntent.FAILED).withKey(job)).isEmpty();
    assertThat(RecordingExporter.incidentRecords(IncidentIntent.CREATED).withJobKey(job)).isEmpty();
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs())
        .singleElement()
        .satisfies(
            activated -> {
              assertThat(activated.key()).isEqualTo(job);
              assertThat(activated.record().getRetries()).isEqualTo(3);
            });
  }

  @ParameterizedTest
  @ValueSource(strings = {"[]", "null", "{broken"})
  void shouldRejectInvalidFailureVariables(final String variables) {
    // given
    final long instance = ENGINE.job().withType("work").create().getKey();
    final long job =
        RecordingExporter.jobRecords(JobIntent.CREATED).withKey(instance).getFirst().getKey();

    // when
    ENGINE.job().withKey(job).withRetries(2).withVariables(variables).expectRejection().fail();

    // then
    final var rejected =
        RecordingExporter.records()
            .withKey(job)
            .withIntent(JobIntent.FAIL)
            .onlyCommandRejections()
            .getFirst();
    assertThat(rejected.getRejectionType()).isEqualTo(RejectionType.INVALID_ARGUMENT);
    assertThat(rejected.getRejectionReason())
        .startsWith(
            String.format(
                "Expected to fail job with key '%d' for tenant '<default>' with valid variables and a supported recurring time"
                    + " for retry backoff '0', but validation failed: ",
                job));
    assertThat(RecordingExporter.jobRecords(JobIntent.FAILED).withKey(job)).isEmpty();
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs())
        .singleElement()
        .satisfies(activated -> assertThat(activated.record().getRetries()).isEqualTo(3));
  }

  @Test
  void shouldRejectFailureOfMissingJob() {
    // when
    ENGINE.job().withKey(123).withRetries(2).expectRejection().fail();

    // then
    final var rejected =
        RecordingExporter.records().withIntent(JobIntent.FAIL).onlyCommandRejections().getFirst();
    assertThat(rejected.getRejectionType()).isEqualTo(RejectionType.NOT_FOUND);
    assertThat(rejected.getRejectionReason())
        .isEqualTo(
            "Expected to fail job with key '123' for tenant '<default>', but no such job was found");
  }

  @Test
  void shouldRejectFailureForAnotherTenantWithoutChangingJob() {
    // given
    final long instance = ENGINE.job().withType("work").create().getKey();
    final long job =
        RecordingExporter.jobRecords(JobIntent.CREATED).withKey(instance).getFirst().getKey();

    // when
    ENGINE.job().withKey(job).withTenantId("other").withRetries(2).expectRejection().fail();

    // then
    assertThat(
            RecordingExporter.records()
                .withKey(job)
                .withIntent(JobIntent.FAIL)
                .onlyCommandRejections()
                .getFirst()
                .getRejectionType())
        .isEqualTo(RejectionType.NOT_FOUND);
    assertThat(RecordingExporter.jobRecords(JobIntent.FAILED).withKey(job)).isEmpty();
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs())
        .singleElement()
        .satisfies(activated -> assertThat(activated.record().getRetries()).isEqualTo(3));
  }
}
