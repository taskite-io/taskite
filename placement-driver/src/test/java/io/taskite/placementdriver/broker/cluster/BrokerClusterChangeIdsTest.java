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
package io.taskite.placementdriver.broker.cluster;

import static org.junit.jupiter.api.Assertions.*;

import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.topology.RecoveryFence;
import java.util.List;
import org.junit.jupiter.api.Test;

final class BrokerClusterChangeIdsTest {
  @Test
  void scopedFrozenOperationRetainsConfigurationAndCanBeDispatched() {
    final var configuration =
        new BrokerOperationConfiguration(
            List.of(new BrokerPartitionMember("a", true, 100)),
            List.of(
                new BrokerPartitionMember("a", true, 100),
                new BrokerPartitionMember("b", false, 0)),
            42,
            new RecoveryFence(7, 7, ""));
    final var original =
        new TopologyOperation("join-b", TopologyOperationType.JOIN, "b", 0, configuration);
    final var scoped =
        BrokerClusterChangeIds.scopeOperations("change", List.of(original)).getFirst();
    assertEquals("change:join-b", scoped.opId());
    assertEquals(original.type(), scoped.type());
    assertEquals(original.targetBroker(), scoped.targetBroker());
    assertEquals(original.priority(), scoped.priority());
    assertSame(configuration, scoped.configuration());
    final var plan =
        new ChangePlan(
            new ChangePlan.Definition(
                new ChangePlan.Identity("demo", "change", 1),
                new ChangePlan.Versions(1, 2),
                List.of(scoped)),
            new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
            ChangePlan.Deadline.NONE);
    assertDoesNotThrow(() -> BrokerTopologyOperationRequests.build(plan, scoped));
  }
}
