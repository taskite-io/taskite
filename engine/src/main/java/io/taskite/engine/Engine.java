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
package io.taskite.engine;

import io.taskite.engine.processing.distribution.CommandDistributionAcknowledgeProcessor;
import io.taskite.engine.processing.incident.IncidentQueryProcessor;
import io.taskite.engine.processing.incident.IncidentResolveProcessor;
import io.taskite.engine.processing.job.JobBackoffChecker;
import io.taskite.engine.processing.job.JobBatchActivateProcessor;
import io.taskite.engine.processing.job.JobCancelProcessor;
import io.taskite.engine.processing.job.JobCompleteProcessor;
import io.taskite.engine.processing.job.JobCreateProcessor;
import io.taskite.engine.processing.job.JobFailProcessor;
import io.taskite.engine.processing.job.JobQueryProcessor;
import io.taskite.engine.processing.job.JobRecurProcessor;
import io.taskite.engine.processing.job.JobTimeOutProcessor;
import io.taskite.engine.processing.job.JobTimeoutChecker;
import io.taskite.engine.processing.job.JobUpdatePriorityProcessor;
import io.taskite.engine.processing.job.JobUpdateRetriesProcessor;
import io.taskite.engine.processing.job.JobUpdateTimeoutProcessor;
import io.taskite.engine.processing.job.JobYieldChecker;
import io.taskite.engine.processing.job.JobYieldCompleteProcessor;
import io.taskite.engine.processing.job.JobYieldProcessor;
import io.taskite.engine.processing.job.WorkerIndexMigrationProcessor;
import io.taskite.engine.processing.job.WorkerSessionFenceProcessor;
import io.taskite.engine.processing.job.WorkerSessionSyncProcessor;
import io.taskite.engine.processing.variable.VariableDocumentUpdateProcessor;
import io.taskite.engine.state.appliers.BrokerEventAppliers;
import io.taskite.engine.state.immutable.ProcessingState;
import io.taskite.engine.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.broker.IncidentRecord;
import io.taskite.protocol.impl.record.value.broker.JobBatchRecord;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.impl.record.value.broker.VariableDocumentRecord;
import io.taskite.protocol.impl.record.value.broker.VariableRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.IncidentIntent;
import io.taskite.protocol.record.intent.broker.JobBatchIntent;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.protocol.record.intent.broker.VariableDocumentIntent;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.state.processing.ProcessingStateStore;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContextImpl;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.Writers;
import io.taskite.streamplatform.api.writer.WritersFactory;
import java.util.function.LongSupplier;

public final class Engine {
  private Engine() {}

  public static TypedEventRegistry eventRegistry() {
    return TypedEventRegistry.builder()
        .register(ValueType.JOB_BATCH, JobBatchRecord::new)
        .register(ValueType.JOB, JobRecord::new)
        .register(ValueType.JOB_YIELD, JobYieldRecord::new)
        .register(ValueType.INCIDENT, IncidentRecord::new)
        .register(ValueType.VARIABLE, VariableRecord::new)
        .register(ValueType.VARIABLE_DOCUMENT, VariableDocumentRecord::new)
        .register(ValueType.COMMAND_DISTRIBUTION, CommandDistributionRecord::new)
        .register(ValueType.JOB_SUBSCRIPTION, JobSubscriptionRecord::new)
        .register(ValueType.JOB_SUBSCRIPTION_DELETION, JobSubscriptionDeletionRecord::new)
        .build();
  }

  public static TypedRecordProcessors createProcessors(final TypedRecordProcessorContext context) {
    return createProcessors(context, System::currentTimeMillis);
  }

  /** Creates processors sharing the caller's clock for job deadlines and retry backoff. */
  public static TypedRecordProcessors createProcessors(
      final TypedRecordProcessorContext context, final LongSupplier clock) {
    final var processors = TypedRecordProcessors.processors();
    final KeyGenerator keyGenerator = context.getKeyGenerator();
    final Writers writers = context.getWriters();
    final ProcessingState state = processingState(context);
    return processors
        .withLifecycleListener(new JobTimeoutChecker(state.getJobState(), clock))
        .withLifecycleListener(new JobBackoffChecker(state.getJobState(), clock))
        .withLifecycleListener(new JobYieldChecker(state))
        .onCommand(
            ValueType.JOB_YIELD,
            JobYieldIntent.MIGRATE_INDEX,
            new WorkerIndexMigrationProcessor(writers))
        .onCommand(
            ValueType.JOB_YIELD,
            JobYieldIntent.SYNC_SESSIONS,
            new WorkerSessionSyncProcessor(state, writers))
        .onCommand(
            ValueType.JOB_YIELD,
            JobYieldIntent.FENCE_SESSION,
            new WorkerSessionFenceProcessor(state, writers))
        .onCommand(
            ValueType.JOB_YIELD,
            JobYieldIntent.COMPLETE_YIELD,
            new JobYieldCompleteProcessor(state, writers))
        .onCommand(
            ValueType.JOB, JobIntent.YIELD, new JobYieldProcessor(state.getJobState(), writers))
        .onCommand(
            ValueType.JOB, JobIntent.CREATE, new JobCreateProcessor(state, writers, keyGenerator))
        .onCommand(ValueType.JOB, JobIntent.CANCEL, new JobCancelProcessor(state, writers))
        .onCommand(
            ValueType.VARIABLE_DOCUMENT,
            VariableDocumentIntent.UPDATE,
            new VariableDocumentUpdateProcessor(state, writers, keyGenerator))
        .onCommand(
            ValueType.INCIDENT,
            IncidentIntent.RESOLVE,
            new IncidentResolveProcessor(state, writers))
        .onCommand(
            ValueType.INCIDENT, IncidentIntent.QUERY, new IncidentQueryProcessor(state, writers))
        .onCommand(
            ValueType.JOB,
            JobIntent.FAIL,
            new JobFailProcessor(state, writers, keyGenerator, context::getProcessingTime))
        .onCommand(
            ValueType.JOB,
            JobIntent.UPDATE_RETRIES,
            new JobUpdateRetriesProcessor(state.getJobState(), writers))
        .onCommand(
            ValueType.JOB,
            JobIntent.UPDATE_PRIORITY,
            new JobUpdatePriorityProcessor(state.getJobState(), writers))
        .onCommand(
            ValueType.JOB,
            JobIntent.UPDATE_TIMEOUT,
            new JobUpdateTimeoutProcessor(state, writers, context::getProcessingTime))
        .onCommand(
            ValueType.JOB,
            JobIntent.TIME_OUT,
            new JobTimeOutProcessor(state.getJobState(), writers, context::getProcessingTime))
        .onCommand(
            ValueType.JOB,
            JobIntent.RECUR,
            new JobRecurProcessor(state.getJobState(), writers, context::getProcessingTime))
        .onCommand(
            ValueType.JOB_BATCH,
            JobBatchIntent.ACTIVATE,
            new JobBatchActivateProcessor(state, writers, keyGenerator, context::getProcessingTime))
        .onCommand(
            ValueType.JOB,
            JobIntent.COMPLETE,
            new JobCompleteProcessor(keyGenerator, writers, state))
        .onCommand(ValueType.JOB, JobIntent.QUERY, new JobQueryProcessor(writers, state))
        .onCommand(
            ValueType.COMMAND_DISTRIBUTION,
            CommandDistributionIntent.START,
            new CommandDistributionAcknowledgeProcessor(writers));
  }

  public static TypedRecordProcessorContext createProcessorContext(
      final int partitionId,
      final ProcessingStateStore storage,
      final KeyGenerator keyGenerator,
      final WritersFactory writersFactory) {
    return createProcessorContext(partitionId, storage, keyGenerator, writersFactory, true);
  }

  public static TypedRecordProcessorContext createProcessorContext(
      final int partitionId,
      final ProcessingStateStore storage,
      final KeyGenerator keyGenerator,
      final WritersFactory writersFactory,
      final boolean workerJobLeaseTokenEnabled) {
    final MutableProcessingState state =
        new MutableProcessingState(storage, workerJobLeaseTokenEnabled);
    final var eventAppliers = BrokerEventAppliers.create(state);
    final Writers writers = writersFactory.create(eventAppliers);
    return new TypedRecordProcessorContextImpl(
        partitionId, keyGenerator, writers, state, eventAppliers);
  }

  private static ProcessingState processingState(final TypedRecordProcessorContext context) {
    return context.getProcessingState(ProcessingState.class);
  }
}
