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

import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.ChangeKind;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.QueryScope;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.QueryType;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.QueryView;

/** Builds the response value for one topology query type. */
public interface TopologyQueryBehavior {
  QueryType queryType();

  default QueryScope scope() {
    return queryType().scope();
  }

  default QueryView view() {
    return queryType().view();
  }

  default ChangeKind changeKind() {
    return queryType().changeKind();
  }

  default boolean isSuitableForQuery(final TopologyQueryRecord query) {
    return scope() == query.getScope()
        && view() == query.getView()
        && (view() != QueryView.CHANGE || changeKind() == query.getChangeKind());
  }

  UnifiedRecordValue createResponseValue(TopologyQueryRecord query);
}
