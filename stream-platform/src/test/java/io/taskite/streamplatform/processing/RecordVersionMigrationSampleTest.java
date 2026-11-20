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
package io.taskite.streamplatform.processing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;
import io.taskite.streamplatform.api.state.EventAppliers;
import io.taskite.streamplatform.api.state.TypedEventApplier;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Runnable v1→v2 record-versioning migration sample (ZEP004 "Record Versioning" + Testing scenario
 * ③). It is a worked template for evolving a command's logic without changing the replayed result
 * of records written by the old version — the promise that makes rolling upgrades safe.
 *
 * <p>Scenario: version 1 of the {@code CREATE} logic stamps a job's worker as {@code "v1-worker"};
 * version 2 changes that behavior to {@code "v2-worker"}. The invariants a correct migration keeps:
 *
 * <ol>
 *   <li><b>Dispatch by version.</b> A record stamped v1 is processed/applied by v1 logic; a record
 *       stamped v2 by v2 logic. Both coexist under the same {@code (ValueType, Intent)}.
 *   <li><b>Old records replay to old state.</b> Applying a v1 event through the v2-capable engine
 *       still yields the v1 result — a behavior change never rewrites history.
 * </ol>
 *
 * <p>Template to copy when you evolve a real Intent:
 *
 * <ol>
 *   <li>Add a v2 command processor and register it with {@code onCommand(valueType, intent, 2,
 *       ...)} alongside the existing v1 (keep v1 — old commands still need it during the upgrade).
 *   <li>Add a v2 event applier and register it with {@code register(intent, 2, ...)} alongside v1.
 *   <li>Have the v2 processor stamp {@code recordVersion=2} on the events it writes (via {@code
 *       appendFollowUpEvent(key, intent, value, 2)}); v1-written events keep {@code
 *       recordVersion=1} and therefore keep replaying through the v1 applier.
 * </ol>
 */
final class RecordVersionMigrationSampleTest {

  private static final int V1 = 1;
  private static final int V2 = 2;

  // ------------------------------------------------------------------------------------------
  // 1. Command dispatch: v1 record → v1 logic, v2 record → v2 logic.
  // ------------------------------------------------------------------------------------------

  @Test
  void commandDispatchSelectsProcessorByRecordVersion() {
    final Map<Integer, String> handledBy = new HashMap<>();
    final TypedRecordProcessors map =
        TypedRecordProcessors.processors()
            .onCommand(
                ValueType.JOB, JobIntent.COMPLETE, V1, new RecordingProcessor(handledBy, "v1"))
            .onCommand(
                ValueType.JOB, JobIntent.COMPLETE, V2, new RecordingProcessor(handledBy, "v2"));

    // The ProcessingStateMachine resolves the processor for a record's version like this:
    map.get(ValueType.JOB, JobIntent.COMPLETE, V1).processRecord(null);
    map.get(ValueType.JOB, JobIntent.COMPLETE, V2).processRecord(null);

    assertEquals("v1", handledBy.get(V1));
    assertEquals("v2", handledBy.get(V2));
    assertEquals(V2, map.getLatestVersion(ValueType.JOB, JobIntent.COMPLETE), "v2 is the latest");
  }

  @Test
  void unknownVersionFallsBackToLatest() {
    // A record written by an even newer engine (version 3) arrives at an engine that only knows
    // v1/v2. ProcessingStateMachine falls back to the latest known version (2). This sample asserts
    // the resolution the state machine performs.
    final Map<Integer, String> handledBy = new HashMap<>();
    final TypedRecordProcessors map =
        TypedRecordProcessors.processors()
            .onCommand(
                ValueType.JOB, JobIntent.COMPLETE, V1, new RecordingProcessor(handledBy, "v1"))
            .onCommand(
                ValueType.JOB, JobIntent.COMPLETE, V2, new RecordingProcessor(handledBy, "v2"));

    final int unknownVersion = 3;
    final TypedRecordProcessor<?> resolved =
        map.get(ValueType.JOB, JobIntent.COMPLETE, unknownVersion) != null
            ? map.get(ValueType.JOB, JobIntent.COMPLETE, unknownVersion)
            : map.get(
                ValueType.JOB,
                JobIntent.COMPLETE,
                map.getLatestVersion(ValueType.JOB, JobIntent.COMPLETE));

    assertEquals(
        map.get(ValueType.JOB, JobIntent.COMPLETE, V2),
        resolved,
        "unknown version resolves to the latest registered processor");
  }

  // ------------------------------------------------------------------------------------------
  // 2. Replay: a v1 event still reconstructs v1 state even though v2 logic exists (scenario ③).
  // ------------------------------------------------------------------------------------------

  @Test
  void replayingOldVersionEventReconstructsOldVersionState() {
    final Map<Long, String> workerByKey = new HashMap<>();
    final EventAppliers appliers =
        new EventAppliers()
            .register(JobIntent.CREATED, V1, new WorkerStampingApplier(workerByKey, "v1-worker"))
            .register(JobIntent.CREATED, V2, new WorkerStampingApplier(workerByKey, "v2-worker"));

    // An event written by the OLD engine carries recordVersion=1.
    appliers.applyState(1L, JobIntent.CREATED, job(), V1);
    // An event written by the NEW engine carries recordVersion=2.
    appliers.applyState(2L, JobIntent.CREATED, job(), V2);

    assertEquals(
        "v1-worker", workerByKey.get(1L), "v1 event must replay through v1 logic (old state kept)");
    assertEquals("v2-worker", workerByKey.get(2L), "v2 event replays through v2 logic");
  }

  /**
   * Applier that records a version-specific worker name for the key, modelling a behavior change.
   */
  private record WorkerStampingApplier(Map<Long, String> workerByKey, String worker)
      implements TypedEventApplier<JobIntent, JobRecord> {
    @Override
    public void applyState(final long key, final JobRecord value) {
      workerByKey.put(key, worker);
    }
  }

  /** Command processor that records which version handled the dispatch. */
  private record RecordingProcessor(Map<Integer, String> handledBy, String tag)
      implements TypedRecordProcessor<JobRecord> {
    @Override
    public void processRecord(final TypedRecord<JobRecord> record) {
      handledBy.put("v1".equals(tag) ? V1 : V2, tag);
    }
  }

  private static UnifiedRecordValue job() {
    return new JobRecord().setJobType("payment").setWorker("ignored").setTenantId("t");
  }
}
