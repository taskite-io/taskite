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

import io.taskite.engine.state.mutable.MutableProcessingState;
import io.taskite.protocol.record.intent.broker.IncidentIntent;
import io.taskite.protocol.record.intent.broker.JobBatchIntent;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.protocol.record.intent.broker.VariableDocumentIntent;
import io.taskite.protocol.record.intent.broker.VariableIntent;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.streamplatform.api.state.EventAppliers;

public final class BrokerEventAppliers {
  private BrokerEventAppliers() {}

  // registerEventAppliers
  public static EventAppliers create(final MutableProcessingState state) {
    final var appliers =
        new EventAppliers()
            .register(
                JobYieldIntent.INDEX_MIGRATED, new WorkerIndexMigratedApplier(state.getJobState()))
            .register(
                JobYieldIntent.SESSIONS_SYNCED,
                new WorkerSessionsSyncedApplier(state.getJobYieldState()))
            .register(JobBatchIntent.ACTIVATED, new JobBatchActivatedApplier(state.getJobState()))
            .register(JobIntent.CREATED, new JobCreatedApplier(state))
            .register(JobIntent.ACTIVATED, new JobActivatedApplier(state))
            .register(JobIntent.COMPLETED, new JobCompletedApplier(state))
            .register(JobIntent.FAILED, new JobFailedApplier(state.getJobState()))
            .register(JobIntent.TIMED_OUT, new JobTimedOutApplier(state.getJobState()))
            .register(JobIntent.RETRIES_UPDATED, new JobRetriesUpdatedApplier(state.getJobState()))
            .register(JobIntent.TIMEOUT_UPDATED, new JobTimeoutUpdatedApplier(state.getJobState()))
            .register(
                JobIntent.PRIORITY_UPDATED, new JobPriorityUpdatedApplier(state.getJobState()))
            .register(
                JobIntent.RECURRED_AFTER_BACKOFF,
                new JobRecurredAfterBackoffApplier(state.getJobState()))
            .register(JobIntent.YIELDED, new JobYieldedApplier(state))
            .register(
                JobYieldIntent.SESSION_FENCED,
                new WorkerSessionFencedApplier(state.getJobYieldState()))
            .register(
                JobYieldIntent.YIELD_COMPLETED,
                new JobYieldCompletedApplier(state.getJobYieldState()))
            .register(JobIntent.CANCELED, new JobCanceledApplier(state))
            .register(JobIntent.QUERIED, new JobQueriedApplier())
            .register(IncidentIntent.CREATED, new IncidentCreatedApplier(state.getIncidentState()))
            .register(
                IncidentIntent.RESOLVED,
                new IncidentResolvedApplier(state.getIncidentState(), state.getJobState()))
            .register(IncidentIntent.QUERIED, EventAppliers.NOOP_EVENT_APPLIER)
            .register(VariableIntent.CREATED, new VariableCreatedApplier(state.getVariableState()))
            .register(VariableIntent.UPDATED, new VariableUpdatedApplier(state.getVariableState()))
            .register(VariableDocumentIntent.UPDATED, EventAppliers.NOOP_EVENT_APPLIER)
            .register(
                JobSubscriptionIntent.CREATED,
                new JobSubscriptionCreatedApplier(state.getJobSubscriptionState()))
            .register(JobSubscriptionIntent.DELETED, new JobSubscriptionDeletedApplier(state))
            .register(CommandDistributionIntent.ACKNOWLEDGED, EventAppliers.NOOP_EVENT_APPLIER);
    return appliers;
  }
}
