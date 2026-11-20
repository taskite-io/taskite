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
package io.taskite.protocol.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.impl.record.value.placementdriver.job.JobYieldPlan;
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.util.BufferUtil;
import org.junit.jupiter.api.Test;

class JobYieldProtocolTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void shouldPreserveDurableIntentAndStateIdentifiers() {
    assertEquals(JobIntent.YIELD, JobIntent.from((short) 15));
    assertEquals(JobIntent.YIELDED, JobIntent.from((short) 16));
    assertFalse(JobIntent.YIELD.isEvent());
    assertTrue(JobIntent.YIELDED.isEvent());
    assertEquals(13, ValueType.JOB_YIELD.value());
    assertEquals(
        JobYieldIntent.YIELD_COMPLETED, Intent.fromProtocolValue(ValueType.JOB_YIELD, (short) 3));
    assertEquals(
        JobYieldIntent.DISPATCH, Intent.fromProtocolValue(ValueType.JOB_YIELD, (short) 12));
    assertEquals(
        JobYieldIntent.SYNC_PARTITION, Intent.fromProtocolValue(ValueType.JOB_YIELD, (short) 13));
    assertFalse(JobYieldIntent.DISPATCH.isEvent());
    assertFalse(JobYieldIntent.SYNC_PARTITION.isEvent());
    assertEquals(224, ColumnFamilies.PD_JOB_YIELDS.getValue());
    assertEquals(225, ColumnFamilies.PD_PENDING_JOB_YIELDS.getValue());
    assertEquals(546, ColumnFamilies.JOB_YIELDS.getValue());
    assertEquals(548, ColumnFamilies.PENDING_JOB_YIELDS.getValue());
  }

  @Test
  void shouldKeepOperationIdentityOnlyInSessionRecords() throws Exception {
    var job = new JobRecord();
    job.wrap(BufferUtil.wrapString("{\"workerId\":\"worker\",\"workerLeaseToken\":9}"));
    assertEquals("worker", job.getWorkerId());
    assertEquals(9, job.getWorkerLeaseToken());
    assertFalse(MAPPER.readTree(job.toJson()).has("yieldId"));

    var session = new JobYieldRecord();
    session.wrap(
        BufferUtil.wrapString(
            "{\"yieldId\":42,\"releasedJobCount\":7,\"sessions\":[{\"yieldId\":43}]}"));
    assertEquals(42, session.getYieldId());
    assertEquals(7, session.getReleasedJobCount());
    assertEquals(43, session.getSessions().getFirst().getYieldId());
    assertYieldId(session.toJson(), 42);
    assertYieldId(session.getSessions().getFirst().toJson(), 43);
    var restoredJob = new JobRecord();
    restoredJob.wrap(BufferUtil.wrapString(job.toJson()));
    assertEquals(job.getWorkerId(), restoredJob.getWorkerId());
    assertEquals(job.getWorkerLeaseToken(), restoredJob.getWorkerLeaseToken());
    var restoredSession = new JobYieldRecord();
    restoredSession.wrap(BufferUtil.wrapString(session.toJson()));
    assertEquals(session.getYieldId(), restoredSession.getYieldId());
    assertEquals(session.getReleasedJobCount(), restoredSession.getReleasedJobCount());
    assertEquals(43, restoredSession.getSessions().getFirst().getYieldId());
  }

  @Test
  void shouldRoundTripPlanWithYieldId() throws Exception {
    var plan =
        MAPPER.readValue(
            "{\"yieldId\":42,\"clusterId\":\"cluster\",\"tenantId\":\"tenant\","
                + "\"workerId\":\"worker\",\"workerLeaseToken\":9,"
                + "\"reason\":\"WORKER_DEREGISTERED\",\"createdAtEpochMillis\":100,"
                + "\"completedAtEpochMillis\":0,\"workerSnapshot\":null,\"partitions\":[]}",
            JobYieldPlan.class);
    assertEquals(42, plan.yieldId());
    assertEquals(9, plan.workerLeaseToken());
    assertYieldId(MAPPER.writeValueAsString(plan), 42);
    assertEquals(plan, MAPPER.readValue(MAPPER.writeValueAsString(plan), JobYieldPlan.class));
  }

  @Test
  void shouldRoundTripWorkerExitPlansWithYieldPlans() throws Exception {
    var worker = new WorkerRegistrationRecord();
    worker.wrap(
        BufferUtil.wrapString(
            "{\"workerId\":\"worker\",\"yieldPlans\":[{\"yieldId\":42,"
                + "\"clusterId\":\"cluster\",\"tenantId\":\"tenant\","
                + "\"workerId\":\"worker\",\"workerLeaseToken\":9,"
                + "\"reason\":\"WORKER_DEREGISTERED\",\"createdAtEpochMillis\":100,"
                + "\"completedAtEpochMillis\":0,\"workerSnapshot\":null,\"partitions\":[]}]}"));
    assertEquals(42, worker.getYieldPlans().getFirst().yieldId());
    var json = MAPPER.readTree(worker.toJson());
    assertTrue(json.has("yieldPlans"));
    assertYieldId(json.get("yieldPlans").get(0).toString(), 42);
    var restored = new WorkerRegistrationRecord();
    restored.wrap(BufferUtil.wrapString(worker.toJson()));
    assertEquals(worker.getYieldPlans(), restored.getYieldPlans());
  }

  private static void assertYieldId(String value, long expected) throws Exception {
    var json = MAPPER.readTree(value);
    assertEquals(expected, json.get("yieldId").longValue());
  }
}
