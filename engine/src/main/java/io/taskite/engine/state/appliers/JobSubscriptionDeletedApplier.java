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
package io.taskite.engine.state.appliers;

import io.taskite.engine.state.immutable.IncidentState;
import io.taskite.engine.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.streamplatform.api.state.TypedEventApplier;

final class JobSubscriptionDeletedApplier
    implements TypedEventApplier<JobSubscriptionIntent, JobSubscriptionDeletionRecord> {
  private final MutableProcessingState state;

  JobSubscriptionDeletedApplier(final MutableProcessingState state) {
    this.state = state;
  }

  @Override
  public void applyState(final long key, final JobSubscriptionDeletionRecord record) {
    final long subscriptionKey = record.getSubscriptionKey();
    final var subscription = state.getJobSubscriptionState().get(subscriptionKey);
    if (subscription == null) {
      return;
    }
    // 先删订阅，避免后续任务继续创建
    state.getJobSubscriptionState().delete(subscriptionKey);

    state
        .getJobState()
        .forEachJobs(
            subscriptionKey,
            (jobKey, job) -> {
              final long incidentKey = state.getIncidentState().getJobIncidentKey(jobKey);
              if (incidentKey != IncidentState.MISSING_INCIDENT) {
                state.getIncidentState().deleteIncident(incidentKey);
              }
              state.getJobState().complete(jobKey, job);
              state.getVariableState().removeScope(jobKey);
              return true;
            });
  }
}
