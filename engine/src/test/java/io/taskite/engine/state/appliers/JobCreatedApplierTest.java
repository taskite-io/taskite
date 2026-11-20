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
package io.taskite.engine.state.appliers;

import static org.junit.jupiter.api.Assertions.*;

import io.taskite.engine.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class JobCreatedApplierTest {

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
  }

  @AfterEach
  void tearDown() throws Exception {
    store.getCurrentTransaction().rollback();
    storage.close();
  }

  @Test
  void createsJobAndRemovesItOnCompletion() {
    final var job = new JobRecord().setJobType("task").setTenantId("tenant");
    new JobCreatedApplier(state).applyState(10, job);
    assertEquals("task", state.getJobState().getJob(10).getType());
    assertEquals("tenant", state.getJobState().getJob(10).getTenantId());
    new JobCompletedApplier(state).applyState(10, job);
    assertNull(state.getJobState().getJob(10));
  }

  @Test
  void persistsIndependentJobsAcrossRecovery() throws Exception {
    final var job = new JobRecord().setJobType("task").setTenantId("tenant");
    final var applier = new JobCreatedApplier(state);
    applier.applyState(10, job);
    applier.applyState(11, job);
    store.getCurrentTransaction().commit();
    storage.close();
    setUp();
    assertEquals("task", state.getJobState().getJob(10).getType());
    assertEquals("task", state.getJobState().getJob(11).getType());
    new JobCompletedApplier(state).applyState(10, job);
    assertNull(state.getJobState().getJob(10));
    assertEquals("task", state.getJobState().getJob(11).getType());
  }

  @Test
  void rollsBackJobCreation() throws Exception {
    new JobCreatedApplier(state)
        .applyState(10, new JobRecord().setJobType("task").setTenantId("tenant"));
    store.getCurrentTransaction().rollback();
    store.getCurrentTransaction();
    assertNull(state.getJobState().getJob(10));
  }
}
