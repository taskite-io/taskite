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
package io.taskite.placementdriver.processing.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.QueryScope;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.QueryType;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.QueryView;
import java.lang.reflect.Proxy;
import java.util.EnumSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

final class TopologyQueryBehaviorsTest {
  @Test
  void providesExactlyOneBehaviorForEveryQueryType() {
    final ProcessingState state =
        (ProcessingState)
            Proxy.newProxyInstance(
                ProcessingState.class.getClassLoader(),
                new Class<?>[] {ProcessingState.class},
                (proxy, method, arguments) -> null);
    final var behaviors = TopologyQueryProcessor.createQueryBehaviors(state);
    final var counts =
        behaviors.stream()
            .map(TopologyQueryBehavior::queryType)
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

    assertEquals(
        EnumSet.of(
            QueryType.BROKER_CLUSTER_CONFIGURATION,
            QueryType.BROKER_CLUSTERS,
            QueryType.TIMER_SUBSCRIPTIONS,
            QueryType.BROKER_TOPOLOGY,
            QueryType.PLACEMENT_DRIVER_TOPOLOGY,
            QueryType.PLACEMENT_DRIVER_HEALTH,
            QueryType.PLACEMENT_DRIVER_CONFIGURATION,
            QueryType.PLACEMENT_DRIVER_CHANGE,
            QueryType.BROKER_CLUSTER_CHANGE),
        counts.keySet());
    assertTrue(counts.values().stream().allMatch(count -> count == 1));
    for (final QueryType queryType : QueryType.values()) {
      final TopologyQueryRecord query = new TopologyQueryRecord().setQueryType(queryType);
      assertEquals(queryType.scope(), query.getScope());
      assertEquals(queryType.view(), query.getView());
      assertEquals(
          1, behaviors.stream().filter(behavior -> behavior.isSuitableForQuery(query)).count());
    }
  }

  @Test
  void rejectsMismatchedLegacyAndScopeViewSelectors() {
    final TopologyQueryRecord query =
        new TopologyQueryRecord()
            .setQueryType(QueryType.BROKER_TOPOLOGY)
            .setScope(QueryScope.PLACEMENT_DRIVER)
            .setView(QueryView.TOPOLOGY);

    assertTrue(!query.hasConsistentSelector());
  }
}
