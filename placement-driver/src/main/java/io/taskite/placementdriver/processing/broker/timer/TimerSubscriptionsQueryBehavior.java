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
package io.taskite.placementdriver.processing.broker.timer;

import io.taskite.placementdriver.processing.query.TopologyQueryBehavior;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.common.TimerSubscriptionListRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.QueryType;

/** Reads scheduled timers through the same serialized query lifecycle as other admin resources. */
public final class TimerSubscriptionsQueryBehavior implements TopologyQueryBehavior {
  private final ProcessingState state;

  public TimerSubscriptionsQueryBehavior(final ProcessingState state) {
    this.state = state;
  }

  @Override
  public QueryType queryType() {
    return QueryType.TIMER_SUBSCRIPTIONS;
  }

  @Override
  public UnifiedRecordValue createResponseValue(final TopologyQueryRecord query) {
    return new TimerSubscriptionListRecord()
        .setSubscriptions(state.getTimerSubscriptionState().subscriptions());
  }
}
