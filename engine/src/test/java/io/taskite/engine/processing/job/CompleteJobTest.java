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
import io.taskite.protocol.record.intent.broker.JobIntent;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CompleteJobTest {
  @RegisterExtension final EngineRule ENGINE = EngineRule.singlePartition();

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void shouldCompleteIndependentJob(final boolean activate) {
    // given
    final long instance = ENGINE.job().withType("work").create().getKey();
    final long job =
        RecordingExporter.jobRecords(JobIntent.CREATED).withKey(instance).getFirst().getKey();
    if (activate) {
      ENGINE.jobs().withType("work").activate();
    }

    // when
    ENGINE.job().withKey(job).withVariables(Map.of("answer", 42)).complete();

    // then
    final var completed = RecordingExporter.jobRecords(JobIntent.COMPLETED).withKey(job).getFirst();
    assertThat(completed.getValue().getVariables()).containsExactlyEntriesOf(Map.of("answer", 42));

    assertThat(ENGINE.job().withKey(job).expectRejection().query().getRejectionType())
        .isEqualTo(RejectionType.NOT_FOUND);
  }

  @Test
  void shouldRejectCompletionOfMissingJob() {
    // when
    ENGINE.job().withKey(123).expectRejection().complete();

    // then
    final var rejection =
        RecordingExporter.records()
            .withIntent(JobIntent.COMPLETE)
            .onlyCommandRejections()
            .getFirst();
    assertThat(rejection.getRejectionType()).isEqualTo(RejectionType.NOT_FOUND);
    assertThat(rejection.getRejectionReason())
        .isEqualTo(
            "Expected to complete job with key '123' for tenant '<default>', but no such job was found");
  }

  @Test
  void shouldRejectRepeatedCompletion() {
    // given
    final long instance = ENGINE.job().withType("work").create().getKey();
    final long job = ENGINE.job().withKey(instance).complete().getKey();

    // when
    ENGINE.job().withKey(job).expectRejection().complete();

    // then
    assertThat(
            RecordingExporter.records()
                .withKey(job)
                .withIntent(JobIntent.COMPLETE)
                .onlyCommandRejections()
                .getFirst()
                .getRejectionType())
        .isEqualTo(RejectionType.NOT_FOUND);
    assertThat(RecordingExporter.jobRecords(JobIntent.COMPLETED).withKey(job)).hasSize(1);

    assertThat(ENGINE.job().withKey(job).expectRejection().query().getRejectionType())
        .isEqualTo(RejectionType.NOT_FOUND);
  }

  @Test
  void shouldRejectCompletionForAnotherTenantWithoutChangingJob() {
    // given
    final long instance = ENGINE.job().withType("work").create().getKey();
    final long job =
        RecordingExporter.jobRecords(JobIntent.CREATED).withKey(instance).getFirst().getKey();

    // when
    ENGINE.job().withKey(job).withTenantId("other").expectRejection().complete();

    // then
    assertThat(
            RecordingExporter.records()
                .withKey(job)
                .withIntent(JobIntent.COMPLETE)
                .onlyCommandRejections()
                .getFirst()
                .getRejectionType())
        .isEqualTo(RejectionType.NOT_FOUND);
    assertThat(RecordingExporter.jobRecords(JobIntent.COMPLETED).withKey(job)).isEmpty();
    assertThat(ENGINE.job().withKey(job).complete().getKey()).isEqualTo(job);
  }

  @ParameterizedTest
  @ValueSource(strings = {"[]", "null", "{broken"})
  void shouldRejectInvalidCompletionVariables(final String variables) {
    // given
    final long instance = ENGINE.job().withType("work").create().getKey();
    final long job =
        RecordingExporter.jobRecords(JobIntent.CREATED).withKey(instance).getFirst().getKey();

    // when
    ENGINE.job().withKey(job).withVariables(variables).expectRejection().complete();

    // then
    assertThat(
            RecordingExporter.records()
                .withKey(job)
                .withIntent(JobIntent.COMPLETE)
                .onlyCommandRejections()
                .getFirst())
        .satisfies(
            rejected -> {
              assertThat(rejected.getRejectionType()).isEqualTo(RejectionType.INVALID_ARGUMENT);
              assertThat(rejected.getRejectionReason())
                  .startsWith(
                      String.format(
                          "Expected to complete job with key '%d' for tenant '<default>' with a JSON object of variables,"
                              + " but validation failed: ",
                          job));
            });
    assertThat(RecordingExporter.jobRecords(JobIntent.COMPLETED).withKey(job)).isEmpty();
    assertThat(ENGINE.job().withKey(job).complete().getKey()).isEqualTo(job);
  }
}
