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
package io.taskite.placementdriver.processing.broker.distribution;

import io.taskite.placementdriver.broker.distribution.BrokerDistributionRouteResolver;
import io.taskite.placementdriver.broker.distribution.BrokerJobSubscriptionDistribution;
import io.taskite.placementdriver.broker.distribution.BrokerJobSubscriptionTransport;
import io.taskite.placementdriver.state.immutable.CommandDistributionState;
import io.taskite.placementdriver.state.immutable.CommandDistributionState.PendingTarget;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.StreamProcessorLifecycleAware;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;
import java.time.Duration;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reserves a durable DISPATCH attempt and sends it after commit. */
public final class CommandDistributionDispatchProcessor
    implements TypedRecordProcessor<CommandDistributionRecord<?>>, StreamProcessorLifecycleAware {
  private static final Logger LOG =
      LoggerFactory.getLogger(CommandDistributionDispatchProcessor.class);
  private final CommandDistributionState state;
  private final Writers writers;
  private final BooleanSupplier leader;
  private final Supplier<BrokerJobSubscriptionTransport> transports;
  private final BrokerDistributionRouteResolver routes;
  private final long observationTimeoutMillis;
  private volatile ProcessingScheduleService scheduling;
  private volatile long lifecycleGeneration;
  private BrokerJobSubscriptionTransport transport;

  public CommandDistributionDispatchProcessor(
      final TypedRecordProcessorContext context,
      final BooleanSupplier leader,
      final Supplier<BrokerJobSubscriptionTransport> transports,
      final BrokerDistributionRouteResolver routes,
      final Duration observationTimeout) {
    state = context.getProcessingState(ProcessingState.class).getCommandDistributionState();
    writers = context.getWriters();
    this.leader = leader;
    this.transports = transports;
    this.routes = routes;
    observationTimeoutMillis = observationTimeout.toMillis();
  }

  @Override
  public void processRecord(final TypedRecord<CommandDistributionRecord<?>> record) {
    if (scheduling == null || !leader.getAsBoolean()) {
      return;
    }
    final var command = record.getValue();
    final var target =
        new PendingTarget(
            command.getDistributionKey(), command.getClusterId(), command.getPartitionId());
    final var entry = state.scheduling().get(target).orElse(null);
    final var current = state.distribution(target.distributionKey()).orElse(null);
    if (entry == null
        || current == null
        || current.getGeneration() != command.getGeneration()
        || entry.generation() != command.getGeneration()
        || entry.attemptId() != command.getDispatchAttemptId()
        || !state.isPending(target.distributionKey(), target.clusterId(), target.partitionId())) {
      return;
    }
    if (entry.dueAt() > record.getTimestamp()
        || record.getPosition() <= 0
        || state
                .scheduling()
                .reservedTargets(
                    record.getTimestamp(), BrokerDistributionScheduler.MAX_RESERVED_TARGETS)
            >= BrokerDistributionScheduler.MAX_RESERVED_TARGETS) {
      return;
    }
    final var request =
        targetRecord(current, target)
            .setDispatchAttemptId(record.getPosition())
            .setNextAttemptAtEpochMillis(record.getTimestamp() + observationTimeoutMillis);
    writers
        .state()
        .appendFollowUpEvent(
            target.distributionKey(), CommandDistributionIntent.DISPATCHED, request);
    final long expected = lifecycleGeneration;
    writers.postCommit().append(() -> send(request, expected));
  }

  private void send(final CommandDistributionRecord<?> request, final long expected) {
    if (!active(expected)) {
      return;
    }
    try {
      final var endpoint =
          routes
              .resolveBrokerDistributionRoute(request.getClusterId())
              .leaderEndpoint(request.getPartitionId());
      if (endpoint.isEmpty()) {
        result(request, expected, false);
        return;
      }
      transport
          .distributeJobSubscription(
              endpoint.get(),
              new BrokerJobSubscriptionDistribution(
                  request.getDistributionKey(),
                  request.getClusterId(),
                  request.getPartitionId(),
                  request.getValueType(),
                  request.getIntent(),
                  request.getValue()))
          .onComplete((ignored, error) -> result(request, expected, error == null));
    } catch (final RuntimeException error) {
      result(request, expected, false);
      LOG.debug(
          "Failed to dispatch job subscription '{}' to cluster '{}' partition '{}' at attempt '{}'",
          request.getDistributionKey(),
          request.getClusterId(),
          request.getPartitionId(),
          request.getDispatchAttemptId(),
          error);
    }
  }

  private void result(
      final CommandDistributionRecord<?> request, final long expected, final boolean success) {
    final var service = scheduling;
    if (service == null || !active(expected)) {
      return;
    }
    service.schedule(
        Duration.ZERO,
        commands -> {
          if (active(expected)) {
            // Rejection/lost callback is compensated by the already committed deadline.
            commands.tryAppend(
                request.getDistributionKey(),
                ValueType.COMMAND_DISTRIBUTION,
                success ? CommandDistributionIntent.ACKNOWLEDGE : CommandDistributionIntent.RETRY,
                request);
          }
        });
  }

  private boolean active(final long expected) {
    return scheduling != null && expected == lifecycleGeneration && leader.getAsBoolean();
  }

  public boolean active() {
    return active(lifecycleGeneration);
  }

  @Override
  public void onRecovered(final ProcessingScheduleService scheduling) {
    onClose();
    transport = transports.get();
    this.scheduling = scheduling;
  }

  @Override
  public void onClose() {
    lifecycleGeneration++;
    scheduling = null;
    if (transport != null) {
      try {
        transport.close();
      } catch (final Exception error) {
        LOG.warn("Failed to close Broker distribution transport", error);
      }
      transport = null;
    }
  }

  static CommandDistributionRecord<UnifiedRecordValue> targetRecord(
      final CommandDistributionRecord<?> record, final PendingTarget target) {
    final UnifiedRecordValue value =
        switch (record.getValueType()) {
          case JOB_SUBSCRIPTION -> {
            final var subscription = (JobSubscriptionRecord) record.getValue();
            yield new JobSubscriptionRecord()
                .setClusterId(target.clusterId())
                .setTenantId(subscription.getTenantId())
                .setName(subscription.getName())
                .setJobType(subscription.getJobType())
                .setVariables(subscription.getVariables())
                .setRetries(subscription.getRetries())
                .setPriority(subscription.getPriority());
          }
          case JOB_SUBSCRIPTION_DELETION -> {
            final var deletion = (JobSubscriptionDeletionRecord) record.getValue();
            yield new JobSubscriptionDeletionRecord()
                .setClusterId(target.clusterId())
                .setTenantId(deletion.getTenantId())
                .setSubscriptionKey(deletion.getSubscriptionKey());
          }
          default -> record.getValue();
        };
    return new CommandDistributionRecord<UnifiedRecordValue>()
        .setDistributionKey(record.getDistributionKey())
        .setGeneration(record.getGeneration())
        .setLastRedistributedAt(record.getLastRedistributedAt())
        .setClusterId(target.clusterId())
        .setPartitionId(target.partitionId())
        .setValueType(record.getValueType())
        .setIntent(record.getIntent())
        .setValue(value);
  }
}
