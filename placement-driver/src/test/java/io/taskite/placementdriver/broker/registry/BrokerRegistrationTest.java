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
package io.taskite.placementdriver.broker.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.MemberRole;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class BrokerRegistrationTest {
  @Test
  void offlineRenewalClearsPartitions() {
    BrokerRegistration registration =
        new BrokerRegistration(
            new BrokerRegistration.Identity("demo", "broker-0", "test", ""),
            new BrokerRegistration.Endpoint("127.0.0.1", 26501, 26601),
            new BrokerRegistration.Lease(
                Instant.EPOCH, Instant.EPOCH, BrokerRegistration.DEFAULT_RENEWAL_TIMEOUT_MILLIS),
            BrokerStatus.ONLINE,
            List.of(new BrokerPartitionState(0, MemberRole.LEADER, 1)));

    BrokerRegistration offline =
        registration.renew(Instant.EPOCH.plusSeconds(1), BrokerStatus.OFFLINE, List.of());

    assertEquals(26601, offline.raftPort());
    assertTrue(offline.partitions().isEmpty());
  }
}
