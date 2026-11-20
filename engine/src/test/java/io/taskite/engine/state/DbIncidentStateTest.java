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
package io.taskite.engine.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.taskite.engine.state.immutable.IncidentState;
import io.taskite.engine.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.broker.IncidentRecord;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class DbIncidentStateTest {

  @TempDir Path tempDir;

  private PartitionStorage storage;
  private StateStoreProcessingStore store;
  private MutableProcessingState state;

  @BeforeEach
  void setUp() {
    storage = new PartitionStorage(tempDir.resolve("runtime"));
    store = new StateStoreProcessingStore(storage);
    state = new MutableProcessingState(store);
    store.getCurrentTransaction();
    state.getJobState().create(2, new JobRecord().setJobType("task").setTenantId("tenant"));
  }

  @AfterEach
  void tearDown() throws Exception {
    store.getCurrentTransaction().rollback();
    storage.close();
  }

  @Test
  void indexesAndDeletesJobIncident() {
    state
        .getIncidentState()
        .createIncident(
            11,
            new IncidentRecord()
                .setTenantId("tenant")
                .setJobKey(2)
                .setErrorMessage("retries exhausted"));

    assertEquals(11, state.getIncidentState().getJobIncidentKey(2));

    state.getIncidentState().deleteIncident(11);

    assertNull(state.getIncidentState().getIncident(11));
    assertEquals(IncidentState.MISSING_INCIDENT, state.getIncidentState().getJobIncidentKey(2));
  }

  @Test
  void rollsBackIncidentAndIndexTogether() throws Exception {
    store.getCurrentTransaction().commit();

    store.getCurrentTransaction();
    state
        .getIncidentState()
        .createIncident(
            12,
            new IncidentRecord().setTenantId("tenant").setJobKey(2).setErrorMessage("temporary"));

    store.getCurrentTransaction().rollback();

    assertNull(state.getIncidentState().getIncident(12));
  }
}
