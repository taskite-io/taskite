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
package io.taskite.placementdriver.runtime;

import static org.junit.jupiter.api.Assertions.*;

import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.BrokerIntent;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.WorkerIntent;
import io.taskite.streamplatform.api.backpressure.BackpressureConfiguration;
import io.taskite.streamplatform.api.backpressure.PartitionRequestLimiter;
import java.util.List;
import org.junit.jupiter.api.Test;

final class PlacementDriverBackpressureTest {
  @Test
  void userRegistrationAndRenewalShareBudgetWhileManagementAndMaintenanceBypass() {
    final var limiter =
        new PartitionRequestLimiter<Intent>(
            BackpressureConfiguration.fixed(1), PlacementDriverBackpressure::isExempt);
    final var permit = limiter.tryAcquire(CommandDistributionIntent.START).orElseThrow();
    for (final var intent :
        List.of(
            BrokerIntent.REGISTER, BrokerIntent.RENEW,
            BrokerIntent.DEREGISTER, WorkerIntent.REGISTER,
            WorkerIntent.RENEW, WorkerIntent.DEREGISTER)) {
      assertTrue(limiter.tryAcquire(intent).isEmpty(), intent.toString());
    }
    for (final var intent :
        List.of(
            MemberChangeIntent.REQUEST,
            CommandDistributionIntent.ACKNOWLEDGE,
            WorkerIntent.MARK_OFFLINE)) {
      limiter.tryAcquire(intent).orElseThrow().drop();
    }
    assertEquals(1, limiter.inFlight());
    permit.success();
    assertTrue(limiter.tryAcquire(WorkerIntent.RENEW).isPresent());
  }
}
