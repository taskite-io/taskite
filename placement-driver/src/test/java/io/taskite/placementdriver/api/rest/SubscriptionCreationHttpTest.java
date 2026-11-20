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
package io.taskite.placementdriver.api.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.taskite.placementdriver.api.PlacementDriverEndpointManager;
import io.taskite.placementdriver.processing.PlacementDriverProcessors;
import io.taskite.placementdriver.processing.broker.timer.TimerSubscriptionsQueryBehavior;
import io.taskite.placementdriver.runtime.PlacementDriverCommandSubmitter;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.immutable.TimerSubscriptionState;
import io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord;
import io.taskite.protocol.impl.record.value.common.TimerType;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRequestRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.common.TimerIntent;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.QueryType;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/** Exercises HTTP decoding, endpoint routing and command/response adaptation together. */
class SubscriptionCreationHttpTest {
  private static final long KEY = 9007199254741052L;
  private final PlacementDriverCommandSubmitter submitter =
      mock(PlacementDriverCommandSubmitter.class);
  private final AtomicReference<LoggedEvent> submitted = new AtomicReference<>();
  private final MockMvc mvc;

  SubscriptionCreationHttpTest() {
    final var manager = new PlacementDriverEndpointManager();
    manager.bindRuntime(
        submitter,
        PlacementDriverProcessors.createEventRegistry(),
        () -> null,
        mock(PlacementDriverEndpointManager.BrokerTopologyWatchPort.class),
        mock(PlacementDriverEndpointManager.PlacementDriverClusterTopologyWatchPort.class),
        mock(PlacementDriverEndpointManager.WorkerRegistryQueryPort.class),
        topology -> null);
    mvc = MockMvcBuilders.standaloneSetup(new PlacementDriverAdminController(manager)).build();
    when(submitter.submitPlacementDriverCommand(any()))
        .thenAnswer(
            call -> {
              final LoggedEvent command = call.getArgument(0);
              submitted.set(command);
              if (command.value() instanceof TimerSubscriptionRecord timer) {
                final boolean cancellation = command.metadata().getIntent() == TimerIntent.CANCEL;
                return CompletableActorFuture.completed(
                    LoggedEvent.ofRecord(
                        cancellation ? command.key() : KEY,
                        new RecordMetadata()
                            .recordType(RecordType.EVENT)
                            .valueType(ValueType.TIMER)
                            .intent(cancellation ? TimerIntent.CANCELED : TimerIntent.CREATED),
                        timer.setDueDate(1793400000000L)));
              }
              final var request = (CommandDistributionRequestRecord<?>) command.value();
              final boolean deletion = request.getValue() instanceof JobSubscriptionDeletionRecord;
              return CompletableActorFuture.completed(
                  LoggedEvent.ofRecord(
                      KEY,
                      new RecordMetadata()
                          .recordType(RecordType.EVENT)
                          .valueType(ValueType.COMMAND_DISTRIBUTION)
                          .intent(CommandDistributionIntent.STARTED),
                      new CommandDistributionRecord<>()
                          .setDistributionKey(deletion ? KEY + 1 : KEY)
                          .setValueType(request.getValueType())
                          .setIntent(request.getIntent())
                          .setValue(request.getValue())));
            });
  }

  @Test
  void jobCreationUsesExistingDistributionCommandAndTemplateDefaults() throws Exception {
    final var result =
        mvc.perform(
                post("/api/v1/broker/job-subscriptions")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        """
                        {"clusterId":"demo","jobType":"payment"}
                        """))
            .andExpect(request().asyncStarted())
            .andReturn();
    mvc.perform(asyncDispatch(result))
        .andExpect(status().isAccepted())
        .andExpect(jsonPath("$.distributionKey").value(KEY));
    final var command = submitted.get();
    assertEquals(CommandDistributionIntent.START, command.metadata().getIntent());
    final var distribution =
        assertInstanceOf(CommandDistributionRequestRecord.class, command.value());
    assertEquals(JobSubscriptionIntent.CREATE.value(), distribution.getIntent());
    final var subscription = assertInstanceOf(JobSubscriptionRecord.class, distribution.getValue());
    assertEquals("demo", subscription.getClusterId());
    assertEquals("<default>", subscription.getTenantId());
    assertEquals("payment", subscription.getJobType());
    assertEquals("", subscription.getName());
    assertEquals("{}", subscription.getVariables());
    assertEquals(3, subscription.getRetries());
    assertEquals(50, subscription.getPriority());
  }

  @Test
  void jobCreationPreservesCustomTenantAndTemplate() throws Exception {
    final var result =
        mvc.perform(
                post("/api/v1/broker/job-subscriptions")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        """
                        {"clusterId":"demo","tenantId":"a","name":"charge","jobType":"payment",
                         "variables":"{\\"amount\\":42}","retries":7,"priority":90}
                        """))
            .andExpect(request().asyncStarted())
            .andReturn();
    mvc.perform(asyncDispatch(result)).andExpect(status().isAccepted());
    final var subscription =
        (JobSubscriptionRecord)
            ((CommandDistributionRequestRecord<?>) submitted.get().value()).getValue();
    assertEquals("a", subscription.getTenantId());
    assertEquals("charge", subscription.getName());
    assertEquals("{\"amount\":42}", subscription.getVariables());
    assertEquals(7, subscription.getRetries());
    assertEquals(90, subscription.getPriority());
  }

  @Test
  void timerCreationUsesTimerCommandAndReturnsDurableKeyAndDueDate() throws Exception {
    final var result =
        mvc.perform(
                post("/api/v1/broker/timer-subscriptions")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        """
                        {"clusterId":"demo","tenantId":"a","name":"settle","type":"CYCLE",
                         "expression":"R5/PT1M","jobTypes":["payment","invoice"],"variables":" "}
                        """))
            .andExpect(request().asyncStarted())
            .andReturn();
    mvc.perform(asyncDispatch(result))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.timerSubscriptionKey").value(KEY))
        .andExpect(jsonPath("$.dueDate").value(1793400000000L));
    assertEquals(TimerIntent.CREATE, submitted.get().metadata().getIntent());
    final var timer = assertInstanceOf(TimerSubscriptionRecord.class, submitted.get().value());
    assertEquals("demo", timer.getClusterId());
    assertEquals("a", timer.getTenantId());
    assertEquals("settle", timer.getName());
    assertEquals(TimerType.CYCLE, timer.getType());
    assertEquals("R5/PT1M", timer.getExpression());
    assertEquals(List.of("payment", "invoice"), timer.getJobTypes());
    assertEquals("{}", timer.getVariables());
  }

  @Test
  void malformedTimerBodiesReturnBadRequestBeforeCommandSubmission() throws Exception {
    for (final String body :
        List.of("{\"type\":\"UNSUPPORTED\"}", "{\"type\":\"DURATION\",\"jobTypes\":[null]}", "")) {
      final var result =
          mvc.perform(
                  post("/api/v1/broker/timer-subscriptions")
                      .contentType(MediaType.APPLICATION_JSON)
                      .content(body))
              .andExpect(request().asyncStarted())
              .andReturn();
      mvc.perform(asyncDispatch(result)).andExpect(status().isBadRequest());
    }
    verifyNoInteractions(submitter);
  }

  @Test
  void timerListReadsCurrentScheduledStateThroughTheQueryLifecycle() throws Exception {
    final var processingState = mock(ProcessingState.class);
    final var timerState = mock(TimerSubscriptionState.class);
    when(processingState.getTimerSubscriptionState()).thenReturn(timerState);
    final var timers = new LinkedHashMap<Long, TimerSubscriptionRecord>();
    timers.put(
        KEY,
        new TimerSubscriptionRecord()
            .setClusterId("demo")
            .setTenantId("a")
            .setName("settle")
            .setType(TimerType.CYCLE)
            .setExpression("R/PT30S")
            .setJobTypes(List.of("payment", "invoice"))
            .setTimeZone("Asia/Shanghai")
            .setDueDate(1793400000000L)
            .setRepetitions(-1)
            .setOccurrence(3));
    when(timerState.subscriptions()).thenReturn(timers);
    final var behavior = new TimerSubscriptionsQueryBehavior(processingState);
    doAnswer(
            call -> {
              final LoggedEvent command = call.getArgument(0);
              submitted.set(command);
              final var query = (TopologyQueryRecord) command.value();
              return CompletableActorFuture.completed(
                  LoggedEvent.ofRecord(
                      KEY,
                      command
                          .metadata()
                          .recordType(RecordType.EVENT)
                          .intent(TopologyIntent.QUERIED),
                      query.setResponseValue(behavior.createResponseValue(query).toJson())));
            })
        .when(submitter)
        .submitPlacementDriverCommand(any());
    final var result =
        mvc.perform(get("/api/v1/broker/timer-subscriptions"))
            .andExpect(request().asyncStarted())
            .andReturn();
    mvc.perform(asyncDispatch(result))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$.subscriptions[0].timerSubscriptionKey").value(KEY))
        .andExpect(jsonPath("$.subscriptions[0].tenantId").value("a"))
        .andExpect(jsonPath("$.subscriptions[0].type").value("CYCLE"))
        .andExpect(jsonPath("$.subscriptions[0].expression").value("R/PT30S"))
        .andExpect(jsonPath("$.subscriptions[0].dueDate").value(1793400000000L))
        .andExpect(jsonPath("$.subscriptions[0].occurrence").value(3));
    assertEquals(
        QueryType.TIMER_SUBSCRIPTIONS,
        ((TopologyQueryRecord) submitted.get().value()).getQueryType());
    timers.clear();
    final var empty =
        mvc.perform(get("/api/v1/broker/timer-subscriptions"))
            .andExpect(request().asyncStarted())
            .andReturn();
    mvc.perform(asyncDispatch(empty))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(0))
        .andExpect(jsonPath("$.subscriptions").isEmpty());
  }

  @Test
  void distributionQueryExposesDeletionAndSubscriptionKeysThroughTheExistingEndpoint()
      throws Exception {
    doAnswer(
            call -> {
              final LoggedEvent command = call.getArgument(0);
              submitted.set(command);
              final var query = (CommandDistributionQueryRecord) command.value();
              final boolean deletion = query.getDistributionKey() == KEY + 1;
              return CompletableActorFuture.completed(
                  LoggedEvent.ofRecord(
                      query.getDistributionKey(),
                      new RecordMetadata()
                          .recordType(RecordType.EVENT)
                          .valueType(ValueType.COMMAND_DISTRIBUTION)
                          .intent(CommandDistributionIntent.QUERIED),
                      query
                          .setSubscriptionKey(deletion ? KEY : 0)
                          .setDeletionDistributionKey(deletion ? 0 : KEY + 1)
                          .setValueType(deletion ? "JOB_SUBSCRIPTION_DELETION" : "JOB_SUBSCRIPTION")
                          .setName("order")
                          .setJobType("charge")
                          .setTenantId("a")));
            })
        .when(submitter)
        .submitPlacementDriverCommand(any());
    final var deletion =
        mvc.perform(get("/api/v1/broker/job-subscriptions/" + (KEY + 1)))
            .andExpect(request().asyncStarted())
            .andReturn();
    mvc.perform(asyncDispatch(deletion))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.distributionKey").value(KEY + 1))
        .andExpect(jsonPath("$.subscriptionKey").value(KEY))
        .andExpect(jsonPath("$.deletionDistributionKey").doesNotExist())
        .andExpect(jsonPath("$.name").value("order"))
        .andExpect(jsonPath("$.jobType").value("charge"))
        .andExpect(jsonPath("$.tenantId").value("a"));
    final var original =
        mvc.perform(get("/api/v1/broker/job-subscriptions/" + KEY))
            .andExpect(request().asyncStarted())
            .andReturn();
    mvc.perform(asyncDispatch(original))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.subscriptionKey").doesNotExist())
        .andExpect(jsonPath("$.deletionDistributionKey").value(KEY + 1));
  }

  @Test
  void jobDeletionUsesTheScopedDeletionDistributionCommand() throws Exception {
    final var result =
        mvc.perform(
                delete("/api/v1/broker/job-subscriptions/" + KEY)
                    .queryParam("clusterId", "demo")
                    .queryParam("tenantId", "a"))
            .andExpect(request().asyncStarted())
            .andReturn();
    mvc.perform(asyncDispatch(result))
        .andExpect(status().isAccepted())
        .andExpect(jsonPath("$.distributionKey").value(KEY + 1));
    final var command = (CommandDistributionRequestRecord<?>) submitted.get().value();
    assertEquals(JobSubscriptionIntent.DELETE.value(), command.getIntent());
    final var deletion = assertInstanceOf(JobSubscriptionDeletionRecord.class, command.getValue());
    assertEquals(KEY, deletion.getSubscriptionKey());
    assertEquals("demo", deletion.getClusterId());
    assertEquals("a", deletion.getTenantId());
  }

  @Test
  void timerDeletionUsesTheOriginalKeyAndPreservesScope() throws Exception {
    final var result =
        mvc.perform(
                delete("/api/v1/broker/timer-subscriptions/" + KEY)
                    .queryParam("clusterId", "demo")
                    .queryParam("tenantId", "a"))
            .andExpect(request().asyncStarted())
            .andReturn();
    mvc.perform(asyncDispatch(result))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.timerSubscriptionKey").value(KEY));
    assertEquals(KEY, submitted.get().key());
    assertEquals(TimerIntent.CANCEL, submitted.get().metadata().getIntent());
    final var timer = (TimerSubscriptionRecord) submitted.get().value();
    assertEquals("demo", timer.getClusterId());
    assertEquals("a", timer.getTenantId());
  }

  @Test
  void deletionRejectsInvalidKeysAndMissingScopeBeforeSubmission() throws Exception {
    for (final String kind : List.of("job", "timer")) {
      for (final String key : List.of("0", "-1", "1.5", "9223372036854775808")) {
        final var invalid =
            mvc.perform(
                    delete("/api/v1/broker/" + kind + "-subscriptions/" + key)
                        .queryParam("clusterId", "demo"))
                .andExpect(request().asyncStarted())
                .andReturn();
        mvc.perform(asyncDispatch(invalid)).andExpect(status().isBadRequest());
      }
      final var unscoped =
          mvc.perform(delete("/api/v1/broker/" + kind + "-subscriptions/" + KEY))
              .andExpect(request().asyncStarted())
              .andReturn();
      mvc.perform(asyncDispatch(unscoped)).andExpect(status().isBadRequest());
    }
    verifyNoInteractions(submitter);
  }

  @Test
  void timerSubscriptionRejectionsKeepExistingHttpErrorEnvelope() throws Exception {
    doAnswer(
            call -> {
              final LoggedEvent command = call.getArgument(0);
              return CompletableActorFuture.completed(
                  LoggedEvent.ofRecord(
                      KEY,
                      command
                          .metadata()
                          .recordType(RecordType.COMMAND_REJECTION)
                          .rejectionType(RejectionType.NOT_FOUND)
                          .rejectionReason("JOB_SUBSCRIPTION_NOT_FOUND: payment"),
                      command.value()));
            })
        .when(submitter)
        .submitPlacementDriverCommand(any());
    final var result =
        mvc.perform(
                post("/api/v1/broker/timer-subscriptions")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        """
                        {"clusterId":"demo","name":"settle","type":"DURATION",
                         "expression":"PT30S","jobTypes":["payment"]}
                        """))
            .andExpect(request().asyncStarted())
            .andReturn();
    mvc.perform(asyncDispatch(result))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.error").value("JOB_SUBSCRIPTION_NOT_FOUND"))
        .andExpect(jsonPath("$.message").value("JOB_SUBSCRIPTION_NOT_FOUND: payment"));
  }
}
