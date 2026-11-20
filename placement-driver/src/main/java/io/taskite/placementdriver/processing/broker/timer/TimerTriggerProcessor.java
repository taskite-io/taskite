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

import io.taskite.broker.client.BrokerClient;
import io.taskite.broker.client.request.BrokerCreateJobRequest;
import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.placementdriver.broker.distribution.BrokerDistributionRouteResolver;
import io.taskite.placementdriver.broker.distribution.ImmutableStateBrokerDistributionRouteResolver;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.common.TimerIntent;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.StreamProcessorLifecycleAware;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;
import io.taskite.util.BufferUtil;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Persists routing before RPCs and confirms all job types before advancing a subscription. */
public final class TimerTriggerProcessor
    implements TypedRecordProcessor<TimerSubscriptionRecord>, StreamProcessorLifecycleAware {
  private static final Logger LOG = LoggerFactory.getLogger(TimerTriggerProcessor.class);
  private static final int MAX_IN_FLIGHT = 16;
  private final ProcessingState state;
  private final Writers writers;
  private final DueDateTimerChecker checker;
  private final Supplier<BrokerClient> clients;
  private final BooleanSupplier leader;
  private final BrokerDistributionRouteResolver routes;
  private final LongSupplier clock;
  private final Map<Long, Attempt> attempts = new HashMap<>();
  private ProcessingScheduleService scheduling;
  private BrokerClient client;
  private volatile long generation;
  private volatile boolean closed = true;

  public TimerTriggerProcessor(
      final TypedRecordProcessorContext context,
      final DueDateTimerChecker checker,
      final Supplier<BrokerClient> clients,
      final BooleanSupplier leader) {
    this(
        context,
        checker,
        clients,
        leader,
        new ImmutableStateBrokerDistributionRouteResolver(
            context.getProcessingState(ProcessingState.class)),
        System::currentTimeMillis);
  }

  TimerTriggerProcessor(
      final TypedRecordProcessorContext context,
      final DueDateTimerChecker checker,
      final Supplier<BrokerClient> clients,
      final BooleanSupplier leader,
      final BrokerDistributionRouteResolver routes,
      final LongSupplier clock) {
    state = context.getProcessingState(ProcessingState.class);
    writers = context.getWriters();
    this.checker = checker;
    this.clients = clients;
    this.leader = leader;
    this.routes = routes;
    this.clock = clock;
  }

  @Override
  public void onRecovered(final ProcessingScheduleService scheduling) {
    onClose();
    this.scheduling = scheduling;
    client = clients.get();
    closed = false;
  }

  @Override
  public void processRecord(final TypedRecord<TimerSubscriptionRecord> record) {
    if (closed || !leader.getAsBoolean()) {
      return;
    }
    final long key = record.getKey();
    final var timer = state.getTimerSubscriptionState().get(key).orElse(null);
    if (timer == null
        || timer.getDueDate() > record.getTimestamp()
        || !sameOccurrence(timer, record.getValue())) {
      return;
    }
    final var previous = attempts.get(key);
    final var attempt =
        previous != null && sameOccurrence(timer, previous.timer)
            ? previous
            : new Attempt(timer, generation);
    if (attempt.complete()) {
      writers.state().appendFollowUpEvent(key, TimerIntent.TRIGGERED, timer);
      final TimerSubscriptionRecord next;
      try {
        final var due =
            TimerSchedule.parse(timer.getType(), timer.getExpression())
                .next(timer, record.getTimestamp());
        next =
            due.isPresent()
                ? copy(timer)
                    .setDueDate(due.getAsLong())
                    .setRepetitions(timer.getRepetitions() == -1 ? -1 : timer.getRepetitions() - 1)
                    .setOccurrence(Math.incrementExact(timer.getOccurrence()))
                    .setPartitionId(0)
                : null;
      } catch (final RuntimeException error) {
        LOG.warn("Timer {} exhausted its representable schedule", key, error);
        return;
      }
      if (next != null) {
        writers.state().appendFollowUpEvent(key, TimerIntent.CREATED, next);
        writers.postCommit().append(() -> checker.scheduleTimer(next.getDueDate()));
      }
      return;
    }
    final io.taskite.topology.BrokerEndpoint endpoint;
    try {
      final var route = routes.resolveBrokerDistributionRoute(timer.getClusterId());
      if (timer.getPartitionId() == 0) {
        final var partitions = route.leadersByPartition().keySet().stream().sorted().toList();
        if (partitions.isEmpty()) {
          return;
        }
        timer.setPartitionId(partitions.get(Math.floorMod(key, partitions.size())));
        writers.state().appendFollowUpEvent(key, TimerIntent.ASSIGNED, timer);
        return;
      }
      endpoint = route.leaderEndpoint(timer.getPartitionId()).orElseThrow();
    } catch (final RuntimeException error) {
      LOG.debug("No route for timer {}; retaining subscription", key, error);
      return;
    }
    writers
        .postCommit()
        .append(
            () -> {
              if (!active(key, attempt) || attempts.get(key) != previous) {
                return;
              }
              attempts.put(key, attempt);
              for (int index = 0;
                  index < attempt.jobs.length && inFlight() < MAX_IN_FLIGHT;
                  index++) {
                final var job = attempt.jobs[index];
                if (!active(key, attempt)) {
                  return;
                }
                if (job.pending || job.succeeded || job.retryAfter > clock.getAsLong()) {
                  continue;
                }
                job.pending = true;
                try {
                  client
                      .sendRequest(
                          endpoint,
                          new BrokerCreateJobRequest(
                              timer.getClusterId(), timer.getPartitionId(), job.request))
                      .onComplete(
                          (response, error) -> {
                            if (!lifecycleActive(attempt)) {
                              return;
                            }
                            scheduling.schedule(
                                Duration.ZERO,
                                commands -> {
                                  if (!active(key, attempt) || attempts.get(key) != attempt) {
                                    return;
                                  }
                                  job.pending = false;
                                  if (error == null && matches(job.request, response)) {
                                    job.succeeded = true;
                                    if (attempt.complete()) {
                                      commands.tryAppend(
                                          key, ValueType.TIMER, TimerIntent.TRIGGER, timer);
                                    }
                                  } else {
                                    job.retryAfter = clock.getAsLong() + 1000;
                                    LOG.debug(
                                        "Failed job creation for timer {}, type {}",
                                        key,
                                        job.request.getType(),
                                        error);
                                  }
                                });
                          });
                } catch (final RuntimeException error) {
                  job.pending = false;
                  job.retryAfter = clock.getAsLong() + 1000;
                  LOG.debug("Failed to send timer {}, type {}", key, job.request.getType(), error);
                }
              }
            });
  }

  private long inFlight() {
    return attempts.values().stream()
        .flatMap(attempt -> Arrays.stream(attempt.jobs))
        .filter(job -> job.pending)
        .count();
  }

  private static boolean matches(
      final JobRecord request, final BrokerResponse<JobRecord> response) {
    if (response == null
        || !response.isResponse()
        || response.getResponse() == null
        || response.getKey() <= 0) {
      return false;
    }
    final var result = response.getResponse();
    return result.getTenantId().equals(request.getTenantId())
        && result.getType().equals(request.getType());
  }

  private boolean lifecycleActive(final Attempt attempt) {
    return !closed && generation == attempt.generation && leader.getAsBoolean();
  }

  private boolean active(final long key, final Attempt attempt) {
    return lifecycleActive(attempt)
        && state
            .getTimerSubscriptionState()
            .get(key)
            .filter(timer -> sameOccurrence(timer, attempt.timer))
            .isPresent();
  }

  private static boolean sameOccurrence(
      final TimerSubscriptionRecord left, final TimerSubscriptionRecord right) {
    return left.getOccurrence() == right.getOccurrence() && left.getDueDate() == right.getDueDate();
  }

  private static TimerSubscriptionRecord copy(final TimerSubscriptionRecord timer) {
    final var copy = new TimerSubscriptionRecord();
    copy.wrap(BufferUtil.wrapString(timer.toJson()));
    return copy;
  }

  @Override
  public void onProcessed(final TypedRecord<?> command) {
    if (!closed) {
      attempts.entrySet().removeIf(entry -> !active(entry.getKey(), entry.getValue()));
    }
  }

  @Override
  public void onClose() {
    closed = true;
    generation++;
    attempts.clear();
    if (client != null) {
      try {
        client.close();
      } catch (final Exception error) {
        LOG.warn("Failed to close timer trigger client", error);
      }
      client = null;
    }
  }

  private static final class Attempt {
    private final TimerSubscriptionRecord timer;
    private final long generation;
    private final JobAttempt[] jobs;

    private Attempt(final TimerSubscriptionRecord timer, final long generation) {
      this.timer = timer;
      this.generation = generation;
      jobs = new JobAttempt[timer.getJobTypes().size()];
      for (int index = 0; index < jobs.length; index++) {
        jobs[index] =
            new JobAttempt(
                new JobRecord()
                    .setTenantId(timer.getTenantId())
                    .setJobType(timer.getJobTypes().get(index))
                    .setVariablesJson(timer.getVariables())
                    .setRetries(3)
                    .setPriority(50));
      }
    }

    private boolean complete() {
      return Arrays.stream(jobs).allMatch(job -> job.succeeded);
    }
  }

  private static final class JobAttempt {
    private final JobRecord request;
    private boolean pending;
    private boolean succeeded;
    private long retryAfter;

    private JobAttempt(final JobRecord request) {
      this.request = request;
    }
  }
}
