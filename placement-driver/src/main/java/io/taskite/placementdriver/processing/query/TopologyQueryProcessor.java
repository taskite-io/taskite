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

import io.taskite.placementdriver.processing.broker.cluster.BrokerClusterChangeQueryBehavior;
import io.taskite.placementdriver.processing.broker.cluster.BrokerClusterConfigurationQueryBehavior;
import io.taskite.placementdriver.processing.broker.cluster.BrokerClustersQueryBehavior;
import io.taskite.placementdriver.processing.broker.timer.TimerSubscriptionsQueryBehavior;
import io.taskite.placementdriver.processing.broker.topology.BrokerTopologyQueryBehavior;
import io.taskite.placementdriver.processing.placementdriver.PlacementDriverConfigurationQueryBehavior;
import io.taskite.placementdriver.processing.placementdriver.PlacementDriverHealthQueryBehavior;
import io.taskite.placementdriver.processing.placementdriver.PlacementDriverMemberChangeQueryBehavior;
import io.taskite.placementdriver.processing.placementdriver.PlacementDriverTopologyQueryBehavior;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;
import java.util.List;

/** Owns the common query command/event lifecycle and delegates response construction by type. */
public final class TopologyQueryProcessor implements TypedRecordProcessor<TopologyQueryRecord> {
  private final Writers writers;
  private static final String SELECTOR_MISMATCH =
      "TOPOLOGY_QUERY_SELECTOR_MISMATCH: Expected to query topology for cluster '%s' and change '%s' with consistent selectors, but received queryType '%s', scope '%s', view '%s' and changeKind '%s'";
  private static final String UNSUPPORTED_QUERY =
      "Expected to query topology for cluster '%s' and change '%s', but query type '%s' is unsupported";
  private final ProcessingState state;
  private final List<TopologyQueryBehavior> queryBehaviors;

  public TopologyQueryProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
    queryBehaviors = createQueryBehaviors(state);
  }

  @Override
  public void processRecord(final TypedRecord<TopologyQueryRecord> record) {
    try {
      final TopologyQueryRecord query = record.getValue();
      if (!query.hasConsistentSelector()) {
        throw TopologyQueryRejection.invalid(
            SELECTOR_MISMATCH.formatted(
                query.getClusterId(),
                query.getChangeId(),
                query.getQueryType(),
                query.getScope(),
                query.getView(),
                query.getChangeKind()));
      }
      final UnifiedRecordValue responseValue = queryBehaviorOf(query).createResponseValue(query);
      final var response =
          new TopologyQueryRecord()
              .setClusterId(query.getClusterId())
              .setChangeId(query.getChangeId())
              .setRequestingMemberId(query.getRequestingMemberId())
              .setRequestedAtEpochMillis(query.getRequestedAtEpochMillis())
              .setRequestingHost(query.getRequestingHost())
              .setRequestingHttpPort(query.getRequestingHttpPort())
              .setRequestingGrpcPort(query.getRequestingGrpcPort())
              .setRequestingCommitIndex(query.getRequestingCommitIndex())
              .setRequestingLastApplied(query.getRequestingLastApplied())
              .setRequestingLastLogIndex(query.getRequestingLastLogIndex())
              .setQueryType(query.getQueryType())
              .setScope(query.getScope())
              .setView(query.getView())
              .setChangeKind(query.getChangeKind())
              .setResponseValue(responseValue.toJson());
      writers.state().appendFollowUpEvent(record.getKey(), TopologyIntent.QUERIED, response);
      writers
          .response()
          .writeEventOnCommand(record.getKey(), TopologyIntent.QUERIED, response, record);
    } catch (final TopologyQueryRejection rejection) {
      writers.rejection().appendRejection(rejection.rejectionType(), rejection.getMessage());
      writers
          .response()
          .writeRejectionOnCommand(record, rejection.rejectionType(), rejection.getMessage());
    }
  }

  private TopologyQueryBehavior queryBehaviorOf(final TopologyQueryRecord query) {
    return queryBehaviors.stream()
        .filter(behavior -> behavior.isSuitableForQuery(query))
        .findFirst()
        .orElseThrow(
            () ->
                new UnsupportedOperationException(
                    UNSUPPORTED_QUERY.formatted(
                        query.getClusterId(), query.getChangeId(), query.getQueryType())));
  }

  static List<TopologyQueryBehavior> createQueryBehaviors(final ProcessingState state) {
    return List.of(
        new BrokerClusterConfigurationQueryBehavior(state),
        new BrokerClustersQueryBehavior(state),
        new TimerSubscriptionsQueryBehavior(state),
        new BrokerTopologyQueryBehavior(state),
        new PlacementDriverTopologyQueryBehavior(state),
        new PlacementDriverHealthQueryBehavior(state),
        new PlacementDriverConfigurationQueryBehavior(state),
        new PlacementDriverMemberChangeQueryBehavior(state),
        new BrokerClusterChangeQueryBehavior(state));
  }
}
