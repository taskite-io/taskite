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
package io.taskite.engine.processing;

import static org.junit.jupiter.api.Assertions.*;

import io.taskite.engine.processing.common.WorkerJobLeaseCheck;
import io.taskite.engine.state.appliers.BrokerEventAppliers;
import io.taskite.engine.state.immutable.JobState;
import io.taskite.engine.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.protocol.record.value.broker.JobYieldStatus;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class WorkerSessionStateTest {
  @TempDir Path directory;

  @Test
  void activationBarrierRequiresIndexAndSynchronizedExitSessions() {
    try (var storage = new PartitionStorage(directory)) {
      var state = new MutableProcessingState(new StateStoreProcessingStore(storage), true);
      var check = new WorkerJobLeaseCheck(state);
      assertFalse(check.canActivate("a", "w", 42));
      state.getJobState().migrateWorkerIndex(100);
      assertFalse(check.canActivate("a", "w", 42));
      var page =
          new JobYieldRecord()
              .setSyncCursor(10)
              .setSyncComplete(true)
              .setSessions(List.of(fence(10)));
      var roundTrip = page.copy();
      assertEquals("w", roundTrip.getSessions().getFirst().getWorkerId());
      BrokerEventAppliers.create(state).applyState(0, JobYieldIntent.SESSIONS_SYNCED, roundTrip, 1);
      assertTrue(state.getJobYieldState().sessionsReady());
      assertFalse(check.canActivate("a", "w", 42));
      assertTrue(check.canActivate("a", "new", 43));
      assertTrue(check.canActivate("b", "w", 42));
      state.getJobYieldState().resetSessionsReady();
      assertFalse(check.canActivate("a", "new", 43));
      assertEquals(10, state.getJobYieldState().synchronizedCursor());
    }
  }

  @Test
  void completedPlansRetainFencesAndOrderedPendingLookupSkipsCompletedPlans() throws Exception {
    try (var storage = new PartitionStorage(directory)) {
      var store = new StateStoreProcessingStore(storage);
      var state = new MutableProcessingState(store, true);
      var sessions = state.getJobYieldState();
      sessions.fence(fence(2));
      sessions.fence(fence(10).setWorkerId("other"));
      assertEquals(
          List.of(2L), sessions.pending(0, 1).stream().map(JobYieldRecord::getYieldId).toList());
      assertEquals(
          List.of(10L), sessions.pending(2, 1).stream().map(JobYieldRecord::getYieldId).toList());
      sessions.released("a", "w", 42);
      assertEquals(1, sessions.get(2).getReleasedJobCount());
      sessions.complete(sessions.get(2).setStatus(JobYieldStatus.COMPLETED));
      sessions.released("a", "w", 42);
      sessions.released("b", "other", 42);
      assertEquals(1, sessions.get(2).getReleasedJobCount());
      assertEquals(0, sessions.get(10).getReleasedJobCount());
      assertTrue(sessions.isRevoked("a", "w", 42));
      assertEquals(
          List.of(10L), sessions.pending(0, 10).stream().map(JobYieldRecord::getYieldId).toList());
      store.getCurrentTransaction().commit();
      var reopened = new MutableProcessingState(new StateStoreProcessingStore(storage), true);
      assertTrue(reopened.getJobYieldState().isRevoked("a", "w", 42));
      assertEquals(1, reopened.getJobYieldState().get(2).getReleasedJobCount());
      assertFalse(reopened.getJobYieldState().sessionsReady());
    }
  }

  @Test
  void pendingPaginationSurvivesInterleavedSessionReadsCompletionAndRollback() throws Exception {
    try (var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var sessions = new MutableProcessingState(store, true).getJobYieldState();
      for (final long id : List.of(91L, 3L, 17L)) {
        sessions.fence(fence(id).setWorkerId("worker:" + id));
      }
      store.getCurrentTransaction().commit();
      store.getCurrentTransaction();
      final var visited = new ArrayList<Long>();
      long cursor = 0;
      while (true) {
        final var page = sessions.pending(cursor, 1);
        if (page.isEmpty()) {
          break;
        }
        final long id = page.getFirst().getYieldId();
        assertTrue(sessions.isRevoked("a", "worker:" + id, 42));
        assertEquals(id, sessions.get(id).getYieldId());
        sessions.complete(sessions.get(id).setStatus(JobYieldStatus.COMPLETED));
        visited.add(id);
        cursor = id;
      }
      assertEquals(List.of(3L, 17L, 91L), visited);
      assertTrue(sessions.pending(0, 10).isEmpty());
      store.getCurrentTransaction().rollback();
      assertEquals(
          visited, sessions.pending(0, 10).stream().map(JobYieldRecord::getYieldId).toList());
      assertTrue(sessions.isRevoked("a", "worker:91", 42));
    }
  }

  @Test
  void disabledFeatureAndLegacyJobsKeepPreviousCompletionSemantics() {
    try (var storage = new PartitionStorage(directory)) {
      var store = new StateStoreProcessingStore(storage);
      var state = new MutableProcessingState(store, false);
      var check = new WorkerJobLeaseCheck(state);
      state.getJobYieldState().fence(fence(10));
      var stored = new JobRecord().setTenantId("a").setWorkerId("w").setWorkerLeaseToken(42);
      assertTrue(check.canActivate("a", "w", 42));
      assertTrue(check.canActivate("a", "", 0));
      assertTrue(check.canFinish(1, stored, new JobRecord().setWorkerLeaseToken(100)));
      var enabled = new MutableProcessingState(store, true);
      var legacy = stored.copy().setJobType("legacy").setWorkerId("").setWorkerLeaseToken(0);
      enabled.getJobState().activate(1, legacy);
      assertTrue(new WorkerJobLeaseCheck(enabled).canFinish(1, legacy, new JobRecord()));
      assertFalse(new WorkerJobLeaseCheck(enabled).canFinish(1, legacy, stored));
      enabled.getJobState().update(1, legacy, JobState.State.ACTIVATABLE);
      assertFalse(new WorkerJobLeaseCheck(enabled).canFinish(1, legacy, new JobRecord()));
    }
  }

  private static JobYieldRecord fence(long id) {
    return new JobYieldRecord()
        .setYieldId(id)
        .setClusterId("demo")
        .setPartitionId(1)
        .setTenantId("a")
        .setWorkerId("w")
        .setWorkerLeaseToken(42);
  }
}
