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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class UpdateJobRetriesTest {
  @RegisterExtension final EngineRule ENGINE = EngineRule.singlePartition();

  @ParameterizedTest
  @ValueSource(ints = {1, 5, Integer.MAX_VALUE})
  void shouldUpdateRetriesOfActivatableJob(final int retries) {
    // given
    final long instance = ENGINE.job().withType("work").create().getKey();

    // when
    final long job = ENGINE.job().withKey(instance).withRetries(retries).updateRetries().getKey();

    // then
    final var updated =
        RecordingExporter.jobRecords(JobIntent.RETRIES_UPDATED).withKey(job).getFirst();
    assertThat(updated.getValue().getRetries()).isEqualTo(retries);

    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs())
        .singleElement()
        .satisfies(
            activated -> {
              assertThat(activated.key()).isEqualTo(job);
              assertThat(activated.record().getRetries()).isEqualTo(retries);
            });
  }

  @Test
  void shouldPreserveActivationWhenUpdatingRetries() {
    // given
    final long instance = ENGINE.job().withType("work").create().getKey();
    RecordingExporter.jobRecords(JobIntent.CREATED).withKey(instance).getFirst();
    final var activated = ENGINE.jobs().withType("work").activate().getValue().getJobs().getFirst();

    // when
    ENGINE.job().withKey(activated.key()).withRetries(5).updateRetries();

    // then
    final var updated =
        RecordingExporter.jobRecords(JobIntent.RETRIES_UPDATED)
            .withKey(activated.key())
            .getFirst()
            .getValue();
    assertThat(updated.getRetries()).isEqualTo(5);
    assertThat(updated.getDeadline()).isEqualTo(activated.record().getDeadline());
    assertThat(updated.getWorker()).isEqualTo(activated.record().getWorker());
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs()).isEmpty();
    ENGINE.job().withKey(activated.key()).complete();
  }

  @Test
  void shouldRequireIncidentResolutionBeforeYieldingExhaustedJob() {
    // given
    final long instance = ENGINE.job().withType("work").create().getKey();
    final long job = ENGINE.job().withKey(instance).withRetries(0).fail().getKey();
    final long incident =
        RecordingExporter.incidentRecords(IncidentIntent.CREATED)
            .withJobKey(job)
            .getFirst()
            .getKey();

    // when
    ENGINE.job().withKey(job).withRetries(2).updateRetries();

    // then
    assertThat(
            RecordingExporter.jobRecords(JobIntent.RETRIES_UPDATED)
                .withKey(job)
                .getFirst()
                .getValue()
                .getRetries())
        .isEqualTo(2);
    assertThat(RecordingExporter.incidentRecords(IncidentIntent.RESOLVED).withKey(incident))
        .isEmpty();
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs()).isEmpty();
    ENGINE.incident().withKey(incident).resolve();
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs())
        .singleElement()
        .satisfies(
            activated -> {
              assertThat(activated.key()).isEqualTo(job);
              assertThat(activated.record().getRetries()).isEqualTo(2);
            });
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1})
  void shouldRejectNonPositiveRetriesWithoutChangingJob(final int retries) {
    // given
    final long instance = ENGINE.job().withType("work").create().getKey();
    final long job =
        RecordingExporter.jobRecords(JobIntent.CREATED).withKey(instance).getFirst().getKey();

    // when
    ENGINE.job().withKey(job).withRetries(retries).expectRejection().updateRetries();

    // then
    final var rejected =
        RecordingExporter.records()
            .withKey(job)
            .withIntent(JobIntent.UPDATE_RETRIES)
            .onlyCommandRejections()
            .getFirst();
    assertThat(rejected.getRejectionType()).isEqualTo(RejectionType.INVALID_ARGUMENT);
    assertThat(rejected.getRejectionReason())
        .isEqualTo(
            String.format(
                "Expected to update retries for job with key '%d' for tenant '<default>' to a positive value, but received '%d'",
                job, retries));
    assertThat(RecordingExporter.jobRecords(JobIntent.RETRIES_UPDATED).withKey(job)).isEmpty();
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs())
        .singleElement()
        .satisfies(activated -> assertThat(activated.record().getRetries()).isEqualTo(3));
  }

  @Test
  void shouldRejectRetryUpdateForMissingJob() {
    // when
    ENGINE.job().withKey(123).withRetries(2).expectRejection().updateRetries();

    // then
    final var rejected =
        RecordingExporter.records()
            .withIntent(JobIntent.UPDATE_RETRIES)
            .onlyCommandRejections()
            .getFirst();
    assertThat(rejected.getRejectionType()).isEqualTo(RejectionType.NOT_FOUND);
    assertThat(rejected.getRejectionReason())
        .isEqualTo(
            "Expected to update retries for job with key '123' for tenant '<default>', but no such job was found");
  }

  @Test
  void shouldRejectRetryUpdateForAnotherTenantWithoutChangingJob() {
    // given
    final long instance = ENGINE.job().withType("work").create().getKey();
    final long jobKey =
        RecordingExporter.jobRecords(JobIntent.CREATED).withKey(instance).getFirst().getKey();

    // when
    ENGINE
        .job()
        .withKey(jobKey)
        .withTenantId("other")
        .withRetries(2)
        .expectRejection()
        .updateRetries();

    // then
    assertThat(
            RecordingExporter.records()
                .withKey(jobKey)
                .withIntent(JobIntent.UPDATE_RETRIES)
                .onlyCommandRejections()
                .getFirst()
                .getRejectionType())
        .isEqualTo(RejectionType.NOT_FOUND);

    assertThat(RecordingExporter.jobRecords(JobIntent.RETRIES_UPDATED).withKey(jobKey)).isEmpty();
    assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs())
        .singleElement()
        .satisfies(activated -> assertThat(activated.record().getRetries()).isEqualTo(3));
  }
}
