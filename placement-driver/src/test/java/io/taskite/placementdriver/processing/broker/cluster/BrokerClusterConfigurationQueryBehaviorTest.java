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
package io.taskite.placementdriver.processing.broker.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfigState;
import io.taskite.placementdriver.state.immutable.BrokerClusterState;
import io.taskite.placementdriver.state.immutable.MemberChangeState;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

final class BrokerClusterConfigurationQueryBehaviorTest {
  @Test
  void exposesCommittedRosterAndRemovedMembers() {
    final BrokerClusterConfig config =
        new BrokerClusterConfig(2, 3, 1, List.of("broker-0", "broker-1"), 7);
    final MemberChangeRecord membership =
        new MemberChangeRecord()
            .setScope(MemberChangeScope.BROKER)
            .setResourceId("demo")
            .setRemovedMembers(List.of("broker-2"));
    final ProcessingState state =
        processingState(brokerClusterConfigState(config), memberChangeState(membership));

    final ClusterConfigRecord result =
        (ClusterConfigRecord)
            new BrokerClusterConfigurationQueryBehavior(state)
                .createResponseValue(new TopologyQueryRecord().setClusterId("demo"));

    assertEquals(List.of("broker-0", "broker-1"), result.getVoterRoster());
    assertEquals(List.of("broker-2"), result.getRemovedBrokers());
    assertTrue(!result.isRosterManaged());
  }

  private static ProcessingState processingState(
      final BrokerClusterConfigState brokerClusterConfigState,
      final MemberChangeState memberChangeState) {
    final var clusterState = mock(BrokerClusterState.class);
    when(clusterState.clusterKey("demo")).thenReturn(7L);
    return (ProcessingState)
        Proxy.newProxyInstance(
            ProcessingState.class.getClassLoader(),
            new Class<?>[] {ProcessingState.class},
            (proxy, method, arguments) ->
                switch (method.getName()) {
                  case "getBrokerClusterConfigState" -> brokerClusterConfigState;
                  case "getBrokerClusterState" -> clusterState;
                  case "getMemberChangeState" -> memberChangeState;
                  default -> null;
                });
  }

  private static BrokerClusterConfigState brokerClusterConfigState(
      final BrokerClusterConfig config) {
    return (BrokerClusterConfigState)
        Proxy.newProxyInstance(
            BrokerClusterConfigState.class.getClassLoader(),
            new Class<?>[] {BrokerClusterConfigState.class},
            (proxy, method, arguments) -> Optional.of(config));
  }

  private static MemberChangeState memberChangeState(final MemberChangeRecord membership) {
    return (MemberChangeState)
        Proxy.newProxyInstance(
            MemberChangeState.class.getClassLoader(),
            new Class<?>[] {MemberChangeState.class},
            (proxy, method, arguments) ->
                "configuration".equals(method.getName())
                    ? Optional.of(membership)
                    : Optional.empty());
  }
}
