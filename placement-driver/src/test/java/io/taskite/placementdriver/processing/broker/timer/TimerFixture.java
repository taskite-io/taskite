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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.taskite.broker.client.BrokerClient;
import io.taskite.broker.client.request.BrokerCreateJobRequest;
import io.taskite.broker.client.request.BrokerRequest;
import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.placementdriver.broker.distribution.BrokerDistributionRoute;
import io.taskite.placementdriver.broker.distribution.BrokerDistributionRouteResolver;
import io.taskite.placementdriver.broker.distribution.CommandDistributionDedupe;
import io.taskite.placementdriver.processing.PlacementDriverProcessors;
import io.taskite.placementdriver.processing.broker.distribution.CommandDistributionCompleteProcessor;
import io.taskite.placementdriver.state.appliers.PlacementDriverEventAppliers;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord;
import io.taskite.protocol.impl.record.value.common.TimerType;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.common.TimerIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService.CommandWriter;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.util.BufferUtil;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.agrona.concurrent.UnsafeBuffer;

final class TimerFixture implements AutoCloseable {
  static final long NOW = Instant.parse("2026-11-11T00:00:00Z").toEpochMilli();
  final PartitionStorage storage;
  final StateStoreProcessingStore store;
  final MutableProcessingState state;
  final TypedRecordProcessorContext context = mock(TypedRecordProcessorContext.class);
  final Writers writers = mock(Writers.class, RETURNS_DEEP_STUBS);
  final AtomicLong keys = new AtomicLong(100);
  final AtomicLong now = new AtomicLong(NOW);
  final AtomicBoolean leader = new AtomicBoolean(true);
  final AtomicBoolean capacity = new AtomicBoolean(true);
  final BrokerClient client = mock(BrokerClient.class);
  final BrokerDistributionRouteResolver routes = mock(BrokerDistributionRouteResolver.class);
  final BrokerEndpoint endpoint = new BrokerEndpoint("broker", "localhost", 26501, 26502);
  final List<Intent> timerEvents = new ArrayList<>();
  final List<EventSnapshot> events = new ArrayList<>();
  final List<Runnable> actions = new ArrayList<>();
  final List<BrokerCreateJobRequest> requests = new ArrayList<>();
  final List<BrokerEndpoint> endpoints = new ArrayList<>();
  final List<CompletableActorFuture<BrokerResponse<JobRecord>>> results = new ArrayList<>();
  final ArrayDeque<Consumer<CommandWriter>> callbacks = new ArrayDeque<>();
  final List<TimerSnapshot> commands = new ArrayList<>();
  private TypedRecord<?> lastProcessedCommand;
  final DueDateTimerChecker checker;
  final TimerTriggerProcessor processor;
  final ProcessingScheduleService scheduling =
      (delay, task) -> {
        callbacks.add(task);
        return () -> callbacks.remove(task);
      };
  final CommandWriter commandWriter =
      new CommandWriter() {
        @Override
        public void append(
            final long key,
            final ValueType type,
            final Intent intent,
            final UnifiedRecordValue value) {
          assertEquals(ValueType.TIMER, type);
          assertEquals(TimerIntent.TRIGGER, intent);
          final var copy = new TimerSubscriptionRecord();
          copy.wrap(BufferUtil.wrapString(value.toJson()));
          commands.add(new TimerSnapshot(key, copy));
        }

        @Override
        public boolean tryAppend(
            final long key,
            final ValueType type,
            final Intent intent,
            final UnifiedRecordValue value) {
          if (!capacity.get()) {
            return false;
          }
          append(key, type, intent, value);
          return true;
        }
      };

  TimerFixture(final Path directory) {
    storage = new PartitionStorage(directory.resolve("runtime"));
    store = new StateStoreProcessingStore(storage);
    state = new MutableProcessingState(store);
    store.getCurrentTransaction();
    state.getBrokerClusterConfigState().putClusterConfig("demo", new BrokerClusterConfig(1, 1, 1));
    registerSubscription(1, ordinary());
    registerSubscription(2, ordinary().setJobType("notify"));
    // Keep fixture-generated keys distinct from subscriptions restored after a restart.
    state
        .getTimerSubscriptionState()
        .subscriptions()
        .keySet()
        .forEach(key -> keys.accumulateAndGet(key, Math::max));
    when(context.getProcessingState(ProcessingState.class)).thenReturn(state);
    when(context.getWriters()).thenReturn(writers);
    when(context.getKeyGenerator()).thenReturn(keys::incrementAndGet);
    final var stateWriter = writers.state();
    final var postCommitWriter = writers.postCommit();
    doAnswer(
            invocation -> {
              final long key = invocation.getArgument(0);
              final ValueType type = invocation.getArgument(1);
              final Intent intent = invocation.getArgument(2);
              final UnifiedRecordValue value = invocation.getArgument(3);
              final var copy = PlacementDriverProcessors.createEventRegistry().create(type, intent);
              final var buffer = new UnsafeBuffer(new byte[value.getLength()]);
              value.write(buffer, 0);
              copy.wrap(buffer);
              events.add(new EventSnapshot(key, type, intent, copy));
              PlacementDriverEventAppliers.create(state).applyState(key, intent, copy, 1);
              if (type == ValueType.TIMER) {
                timerEvents.add(intent);
              }
              return null;
            })
        .when(stateWriter)
        .appendFollowUpEvent(
            anyLong(), any(ValueType.class), any(Intent.class), any(UnifiedRecordValue.class));
    doAnswer(
            invocation -> {
              final long key = invocation.getArgument(0);
              final Intent intent = invocation.getArgument(1);
              final UnifiedRecordValue value = invocation.getArgument(2);
              final var type = lastProcessedCommand.getValueType();
              final var copy = PlacementDriverProcessors.createEventRegistry().create(type, intent);
              final var buffer = new UnsafeBuffer(new byte[value.getLength()]);
              value.write(buffer, 0);
              copy.wrap(buffer);
              events.add(new EventSnapshot(key, type, intent, copy));
              PlacementDriverEventAppliers.create(state).applyState(key, intent, copy, 1);
              if (type == ValueType.TIMER) {
                timerEvents.add(intent);
              }
              return null;
            })
        .when(stateWriter)
        .appendFollowUpEvent(anyLong(), any(Intent.class), any(UnifiedRecordValue.class));
    doAnswer(
            invocation -> {
              actions.add(invocation.getArgument(0));
              return null;
            })
        .when(postCommitWriter)
        .append(any(Runnable.class));
    when(routes.resolveBrokerDistributionRoute(anyString()))
        .thenReturn(new BrokerDistributionRoute(2, Map.of(2, endpoint)));
    doAnswer(
            invocation -> {
              endpoints.add(invocation.getArgument(0));
              final BrokerCreateJobRequest request = invocation.getArgument(1);
              request.serializeValue();
              requests.add(request);
              final var result = new CompletableActorFuture<BrokerResponse<JobRecord>>();
              results.add(result);
              return result;
            })
        .when(client)
        .sendRequest(any(BrokerEndpoint.class), any(BrokerRequest.class));
    checker = new DueDateTimerChecker(state, now::get);
    processor =
        new TimerTriggerProcessor(context, checker, () -> client, leader::get, routes, now::get);
    processor.onRecovered(scheduling);
  }

  static TimerSubscriptionRecord subscription(
      final long dueDate, final long interval, final int repetitions) {
    return new TimerSubscriptionRecord()
        .setClusterId("demo")
        .setTenantId("tenant")
        .setJobTypes(List.of("charge"))
        .setName("charge")
        .setVariables("{\"amount\":42}")
        .setTimeZone("UTC")
        .setType(interval == 0 ? TimerType.DURATION : TimerType.CYCLE)
        .setExpression(
            interval == 0
                ? "PT1S"
                : (repetitions == -1 ? "R" : "R" + repetitions)
                    + "/"
                    + Instant.ofEpochMilli(dueDate)
                    + "/"
                    + java.time.Duration.ofMillis(interval));
  }

  static TimerSubscriptionRecord once() {
    return subscription(NOW + 1000, 0, 1);
  }

  static JobSubscriptionRecord ordinary() {
    return new JobSubscriptionRecord()
        .setClusterId("demo")
        .setTenantId("tenant")
        .setJobType("charge")
        .setName("charge");
  }

  void registerSubscription(final long key, final JobSubscriptionRecord subscription) {
    distribution(key, subscription);
    state.getJobSubscriptionState().put(key, subscription);
  }

  CommandDistributionRecord<?> distribution(
      final long key, final JobSubscriptionRecord deployment) {
    store.getCurrentTransaction();
    final var distribution =
        new CommandDistributionRecord<>()
            .setDistributionKey(key)
            .setClusterId(deployment.getClusterId())
            .setValueType(ValueType.JOB_SUBSCRIPTION)
            .setIntent(JobSubscriptionIntent.CREATE)
            .setValue(deployment);
    state.getCommandDistributionState().putDistribution(distribution);
    state
        .getCommandDistributionState()
        .putDedupeKey(CommandDistributionDedupe.dedupeKey(distribution), key);
    state.getCommandDistributionState().putTarget(key, deployment.getClusterId(), 1);
    return distribution;
  }

  void complete(final long key, final JobSubscriptionRecord deployment, final long timestamp) {
    complete(distribution(key, deployment), timestamp);
  }

  void complete(final long ignored, final TimerSubscriptionRecord timer, final long timestamp) {
    new TimerCreateProcessor(context, checker).processRecord(record(-1, timer, timestamp));
  }

  void complete(final CommandDistributionRecord<?> distribution, final long timestamp) {
    store.getCurrentTransaction();
    final TypedRecord<CommandDistributionRecord<?>> command =
        record(distribution.getDistributionKey(), distribution, timestamp);
    lastProcessedCommand = command;
    new CommandDistributionCompleteProcessor(context).processRecord(command);
  }

  TimerSnapshot onlyTimer() {
    final var timers = state.getTimerSubscriptionState().subscriptions();
    assertEquals(1, timers.size());
    final var entry = timers.entrySet().iterator().next();
    return new TimerSnapshot(entry.getKey(), entry.getValue());
  }

  void process(final TimerSnapshot timer, final long timestamp) {
    store.getCurrentTransaction();
    now.set(timestamp);
    final var command = record(timer.key(), timer.value(), timestamp);
    lastProcessedCommand = command;
    final boolean unassigned = timer.value().getPartitionId() == 0;
    processor.processRecord(command);
    if (unassigned
        && state
            .getTimerSubscriptionState()
            .get(timer.key())
            .filter(value -> value.getPartitionId() > 0)
            .isPresent()) {
      try {
        commitActions();
      } catch (final Exception error) {
        throw new RuntimeException(error);
      }
      final var assigned = state.getTimerSubscriptionState().get(timer.key()).orElseThrow();
      final var assignedCommand = record(timer.key(), assigned, timestamp);
      lastProcessedCommand = assignedCommand;
      processor.processRecord(assignedCommand);
    }
  }

  void commitActions() throws Exception {
    store.getCurrentTransaction().commit();
    final var committed = List.copyOf(actions);
    actions.clear();
    committed.forEach(Runnable::run);
    if (lastProcessedCommand != null) {
      processor.onProcessed(lastProcessedCommand);
    }
  }

  void callbacks() {
    while (!callbacks.isEmpty()) {
      callbacks.remove().accept(commandWriter);
    }
  }

  void success(final int index) {
    results
        .get(index)
        .complete(
            new BrokerResponse<>(
                requests.get(index).getRecordValue().copy(),
                requests.get(index).getPartitionId(),
                1000 + index));
    callbacks();
  }

  void trigger(final TimerSnapshot timer, final long timestamp) throws Exception {
    final int before = requests.size();
    process(timer, timestamp);
    commitActions();
    assertEquals(before + timer.value().getJobTypes().size(), requests.size());
    for (int index = before; index < requests.size(); index++) {
      success(index);
    }
    assertFalse(commands.isEmpty());
    final var confirmation = commands.removeLast();
    process(confirmation, timestamp);
    commitActions();
  }

  void delete(final long key) {
    store.getCurrentTransaction();
    final var timer = state.getTimerSubscriptionState().get(key).orElseThrow();
    new TimerCancelProcessor(context).processRecord(record(key, timer, now.get()));
  }

  <T extends UnifiedRecordValue> TypedRecord<T> record(
      final long key, final T value, final long timestamp) {
    @SuppressWarnings("unchecked")
    final TypedRecord<T> record = mock(TypedRecord.class);
    when(record.getKey()).thenReturn(key);
    when(record.getValue()).thenReturn(value);
    when(record.getTimestamp()).thenReturn(timestamp);
    when(record.getValueType())
        .thenReturn(
            value instanceof TimerSubscriptionRecord
                ? ValueType.TIMER
                : ValueType.COMMAND_DISTRIBUTION);
    lastProcessedCommand = record;
    return record;
  }

  @Override
  public void close() throws Exception {
    processor.onClose();
    checker.onClose();
    store.getCurrentTransaction().rollback();
    storage.close();
  }

  record TimerSnapshot(long key, TimerSubscriptionRecord value) {}

  record EventSnapshot(long key, ValueType type, Intent intent, UnifiedRecordValue value) {}
}
