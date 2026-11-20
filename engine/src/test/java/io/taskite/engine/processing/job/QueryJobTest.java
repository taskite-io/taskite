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

import io.taskite.engine.state.immutable.JobState;
import io.taskite.engine.util.EngineRule;
import io.taskite.engine.util.record.RecordingExporter;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class QueryJobTest {
  @RegisterExtension final EngineRule ENGINE = EngineRule.singlePartition();

  @ParameterizedTest
  @EnumSource(
      value = JobState.State.class,
      names = {"ACTIVATABLE", "ACTIVATED", "FAILED"})
  void shouldReturnCurrentJobWithoutChangingLifecycle(final JobState.State status) {
    // given
    final long processInstanceKey =
        ENGINE
            .job()
            .withType("work")
            .withVariables(Map.of("amount", 41, "root", true))
            .create()
            .getKey();
    final var created =
        RecordingExporter.jobRecords(JobIntent.CREATED).withKey(processInstanceKey).getFirst();
    final long job = created.getKey();
    if (status == JobState.State.ACTIVATED) {
      ENGINE.jobs().withType("work").withWorker("worker").activate();
    } else if (status == JobState.State.FAILED) {
      ENGINE
          .job()
          .withKey(job)
          .withRetries(2)
          .withRetryBackoff(60_000)
          .withErrorMessage("temporary")
          .fail();
    }

    // when
    final var first =
        ENGINE
            .job()
            .withKey(job)
            .withType("untrusted")
            .withWorker("untrusted", 99)
            .withVariables(Map.of("untrusted", true))
            .query();
    final var second = ENGINE.job().withKey(job).query();

    // then
    final var value = first.getValue();
    assertThat(second.getValue().toJson()).isEqualTo(value.toJson());
    assertThat(value.getType()).isEqualTo("work");

    assertThat(value.getVariables())
        .containsExactlyInAnyOrderEntriesOf(Map.of("amount", 41, "root", true));
    assertThat(RecordingExporter.jobRecords(JobIntent.QUERIED).withKey(job)).hasSize(2);
    if (status == JobState.State.ACTIVATABLE) {
      assertThat(value.getDeadline()).isZero();
      assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs()).hasSize(1);
    } else if (status == JobState.State.ACTIVATED) {
      assertThat(value.getWorker()).isEqualTo("worker");
      assertThat(value.getDeadline()).isPositive();
      assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs()).isEmpty();
    } else {
      assertThat(value.getRetries()).isEqualTo(2);
      assertThat(value.getErrorMessage()).isEqualTo("temporary");
      assertThat(value.getRecurringTime()).isPositive();
      assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs()).isEmpty();
    }
  }

  @Test
  void shouldReadLatestJobVariables() {
    // given
    final long processInstanceKey =
        ENGINE
            .job()
            .withType("work")
            .withVariables(Map.of("amount", 41, "root", 1))
            .create()
            .getKey();
    final long job =
        RecordingExporter.jobRecords(JobIntent.CREATED)
            .withKey(processInstanceKey)
            .getFirst()
            .getKey();
    assertThat(ENGINE.job().withKey(job).query().getValue().getVariables())
        .containsExactlyInAnyOrderEntriesOf(Map.of("amount", 41, "root", 1));

    // when
    ENGINE
        .variables()
        .withJobKey(processInstanceKey)
        .withDocument(Map.of("amount", 99, "root", 2))
        .update();

    // then
    assertThat(ENGINE.job().withKey(job).query().getValue().getVariables())
        .containsExactlyInAnyOrderEntriesOf(Map.of("amount", 99, "root", 2));
  }

  @ParameterizedTest
  @ValueSource(strings = {"missing", "foreign", "completed", "cancelled"})
  void shouldRejectUnavailableJobWithoutReturningTenantDetails(final String scenario) {
    // given
    final long processInstanceKey = ENGINE.job().withType("work").create().getKey();
    final long existing =
        RecordingExporter.jobRecords(JobIntent.CREATED)
            .withKey(processInstanceKey)
            .getFirst()
            .getKey();
    final long key = scenario.equals("missing") ? 123 : existing;
    final String tenantId = scenario.equals("foreign") ? "other" : "<default>";
    if (scenario.equals("completed")) {
      ENGINE.job().withKey(key).complete();
    } else if (scenario.equals("cancelled")) {
      ENGINE.job().withKey(processInstanceKey).cancel();
    }

    // when
    final var rejected = ENGINE.job().withKey(key).withTenantId(tenantId).expectRejection().query();

    // then
    assertThat(rejected.getRejectionType()).isEqualTo(RejectionType.NOT_FOUND);
    assertThat(rejected.getRejectionReason())
        .isEqualTo(
            String.format(
                "Expected to query job with key '%d' for tenant '%s', but no such job was found",
                key, tenantId));
    assertThat(RecordingExporter.jobRecords(JobIntent.QUERIED).withKey(key)).isEmpty();
    if (scenario.equals("foreign") || scenario.equals("missing")) {
      assertThat(ENGINE.jobs().withType("work").activate().getValue().getJobs()).hasSize(1);
    }
  }
}
