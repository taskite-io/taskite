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
package io.taskite.engine.processing.incident;

import static org.assertj.core.api.Assertions.assertThat;

import io.taskite.engine.util.EngineRule;
import io.taskite.engine.util.record.RecordingExporter;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.intent.broker.IncidentIntent;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

final class JobIncidentTest {
  @RegisterExtension final EngineRule engine = EngineRule.singlePartition();

  @Test
  void queriesExhaustedJobAndResolvesAfterRetriesAreUpdated() {
    engine.createSubscription("charge", "charge", "<default>");
    final long job =
        engine.job().withType("charge").withVariables(Map.of("amount", 7)).create().getKey();
    engine.job().withKey(job).withRetries(0).withErrorMessage("temporarily unavailable").fail();
    final long incident =
        RecordingExporter.incidentRecords(IncidentIntent.CREATED)
            .withJobKey(job)
            .getFirst()
            .getKey();
    assertThat(engine.incident().query(job).getValue().getJobKey()).isEqualTo(job);
    assertThat(engine.incident().expectRejection().resolve(incident).getRejectionType())
        .isEqualTo(RejectionType.INVALID_STATE);
    assertThat(
            engine
                .incident()
                .withTenantId("other")
                .expectRejection()
                .resolve(incident)
                .getRejectionType())
        .isEqualTo(RejectionType.NOT_FOUND);
    engine.job().withKey(job).withRetries(2).updateRetries();
    engine.incident().resolve(incident);
    final var activated = engine.jobs().withType("charge").activate().getValue().getJobs();
    assertThat(activated).hasSize(1);
    assertThat(activated.getFirst().key()).isEqualTo(job);
    assertThat(activated.getFirst().record().getVariables()).containsEntry("amount", 7);
    assertThat(engine.incident().expectRejection().resolve(incident).getRejectionType())
        .isEqualTo(RejectionType.NOT_FOUND);
  }
}
