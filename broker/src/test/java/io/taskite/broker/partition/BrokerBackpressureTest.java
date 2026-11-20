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
package io.taskite.broker.partition;

import static org.junit.jupiter.api.Assertions.*;

import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.broker.JobBatchIntent;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.streamplatform.api.backpressure.BackpressureConfiguration;
import io.taskite.streamplatform.api.backpressure.PartitionRequestLimiter;
import java.util.List;
import org.junit.jupiter.api.Test;

final class BrokerBackpressureTest {
  @Test
  void ordinaryCommandsShareBudgetAndCompletionCleanupCommandsBypass() {
    final var limiter =
        new PartitionRequestLimiter<Intent>(
            BackpressureConfiguration.fixed(1), BrokerBackpressure::isExempt);
    limiter.tryAcquire(JobIntent.CREATE).orElseThrow();
    assertTrue(limiter.tryAcquire(JobBatchIntent.ACTIVATE).isEmpty());
    for (final var intent :
        List.of(JobIntent.COMPLETE, JobIntent.FAIL, JobIntent.TIME_OUT, JobIntent.CANCEL)) {
      limiter.tryAcquire(intent).orElseThrow().drop();
    }
    assertEquals(1, limiter.inFlight());
    assertFalse(BrokerBackpressure.isExempt(JobIntent.UPDATE_RETRIES));
  }
}
