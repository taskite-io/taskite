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

import io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord;
import io.taskite.protocol.impl.record.value.common.TimerType;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.intent.common.TimerIntent;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class TimerSubscriptionTest {
  @TempDir Path directory;

  @Test
  void persistsLiteralCycleAndVariablesAcrossRestart() throws Exception {
    long key;
    final String expression = " R3/PT30S ";
    try (var f = new TimerFixture(directory)) {
      f.complete(
          42,
          TimerFixture.once()
              .setType(TimerType.CYCLE)
              .setExpression(expression)
              .setVariables("{\"count\":3,\"delaySeconds\":15}"),
          TimerFixture.NOW);
      var timer = f.onlyTimer();
      key = timer.key();
      assertEquals("R3/PT30S", timer.value().getExpression());
      assertEquals(TimerFixture.NOW + 30000, timer.value().getDueDate());
      assertEquals(java.time.ZoneId.systemDefault().getId(), timer.value().getTimeZone());
      f.commitActions();
    }
    try (var f = new TimerFixture(directory)) {
      var timer = f.onlyTimer();
      assertEquals(key, timer.key());
      assertEquals("R3/PT30S", timer.value().getExpression());
      f.trigger(timer, TimerFixture.NOW + 30000);
      assertEquals(TimerFixture.NOW + 60000, f.onlyTimer().value().getDueDate());
      assertEquals(2, f.onlyTimer().value().getRepetitions());
      assertEquals(
          "{\"count\":3,\"delaySeconds\":15}",
          f.requests.getFirst().getRecordValue().getVariablesJson());
      f.delete(key);
      f.complete(
          42,
          TimerFixture.once()
              .setType(TimerType.CYCLE)
              .setExpression(expression)
              .setVariables("{\"count\":3,\"delaySeconds\":15}"),
          TimerFixture.NOW + 90000);
      assertNotEquals(key, f.onlyTimer().key());
      assertEquals("R3/PT30S", f.onlyTimer().value().getExpression());
      assertEquals(TimerFixture.NOW + 120000, f.onlyTimer().value().getDueDate());
      verify(f.writers.response())
          .writeEventOnCommand(eq(f.onlyTimer().key()), eq(TimerIntent.CREATED), any(), any());
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "= \"PT30S\"",
        "= duration(\"PT30S\")",
        "= schedule",
        "= missing",
        "= \"PT\" + string(delay * -1) + \"S\"",
        "= delay",
        "= string(delay / 0)",
        "PT${delay}S"
      })
  void rejectsNonLiteralSchedulesBeforePersistence(final String expression) throws Exception {
    try (var f = new TimerFixture(directory)) {
      f.complete(
          42,
          TimerFixture.once()
              .setExpression(expression)
              .setVariables("{\"delay\":15,\"schedule\":\"PT30S\"}"),
          TimerFixture.NOW);
      verify(f.writers.rejection())
          .appendRejection(eq(RejectionType.INVALID_ARGUMENT), anyString());
      assertEquals(100, f.keys.get());
      assertTrue(f.state.getTimerSubscriptionState().subscriptions().isEmpty());
    }
  }

  @Test
  void createsANewIndependentDefinitionAfterCancellation() throws Exception {
    try (var f = new TimerFixture(directory)) {
      var distributions =
          f.state.getCommandDistributionState().distributions().stream()
              .map(value -> value.toJson())
              .toList();
      f.complete(
          42, TimerFixture.once().setJobTypes(List.of("charge", "notify")), TimerFixture.NOW);
      var timer = f.onlyTimer();
      assertEquals(TimerFixture.NOW + 1000, timer.value().getDueDate());
      assertEquals(1, timer.value().getRepetitions());
      assertTrue(f.state.getJobSubscriptionState().get(42).isEmpty());
      assertEquals(
          distributions,
          f.state.getCommandDistributionState().distributions().stream()
              .map(value -> value.toJson())
              .toList());
      f.commitActions();
      f.delete(timer.key());
      assertTrue(f.state.getTimerSubscriptionState().subscriptions().isEmpty());
      assertEquals(
          -1,
          f.state
              .getTimerSubscriptionState()
              .processTimersWithDueDateBefore(
                  Long.MAX_VALUE, (key, value) -> fail("orphan index")));
      assertTrue(f.timerEvents.contains(TimerIntent.CANCELED));
      var before = f.keys.get();
      f.complete(
          42,
          TimerFixture.once().setJobTypes(List.of("charge", "notify")),
          TimerFixture.NOW + 9000);
      assertTrue(f.keys.get() > before);
      assertNotEquals(timer.key(), f.onlyTimer().key());
      assertEquals(TimerFixture.NOW + 10000, f.onlyTimer().value().getDueDate());
    }
  }

  @Test
  void finiteCycleKeepsKeyAndAdvancesPlannedOccurrenceAcrossDelayedFirings() throws Exception {
    try (var f = new TimerFixture(directory)) {
      f.complete(42, TimerFixture.subscription(TimerFixture.NOW + 1000, 1000, 3), TimerFixture.NOW);
      var key = f.onlyTimer().key();
      for (int occurrence = 0; occurrence < 3; occurrence++) {
        var timer = f.onlyTimer();
        assertEquals(key, timer.key());
        assertEquals(occurrence, timer.value().getOccurrence());
        assertEquals(3 - occurrence, timer.value().getRepetitions());
        assertEquals(TimerFixture.NOW + (occurrence + 1) * 1000, timer.value().getDueDate());
        f.trigger(timer, timer.value().getDueDate() + 5000);
      }
      assertTrue(f.state.getTimerSubscriptionState().subscriptions().isEmpty());
      assertEquals(
          List.of("charge", "charge", "charge"),
          f.requests.stream().map(r -> r.getRecordValue().getType()).toList());
      assertTrue(
          f.requests.stream()
              .allMatch(
                  r ->
                      r.getRecordValue().getSubscriptionKey() == 0
                          && r.getRecordValue().getVariablesJson().equals("{\"amount\":42}")));
      f.complete(
          42,
          TimerFixture.subscription(TimerFixture.NOW + 1000, 1000, 3),
          TimerFixture.NOW + 99999);
      assertNotEquals(key, f.onlyTimer().key());
    }
  }

  @Test
  void cancellationRollsBackPrimaryAndDueIndexTogether() throws Exception {
    try (var f = new TimerFixture(directory)) {
      f.complete(42, TimerFixture.once(), TimerFixture.NOW);
      var timer = f.onlyTimer();
      f.commitActions();
      f.delete(timer.key());
      assertTrue(f.state.getTimerSubscriptionState().get(timer.key()).isEmpty());
      f.store.getCurrentTransaction().rollback();
      assertEquals(timer.key(), f.onlyTimer().key());
      f.now.set(timer.value().getDueDate());
      f.checker.check(f.commandWriter);
      assertEquals(timer.key(), f.commands.getFirst().key());
    }
  }

  @Test
  void infiniteCycleSurvivesRestartAndClusterRemovalClearsSubscriptionAndIndex() throws Exception {
    long key;
    try (var f = new TimerFixture(directory)) {
      f.complete(
          42, TimerFixture.subscription(TimerFixture.NOW + 1000, 1000, -1), TimerFixture.NOW);
      key = f.onlyTimer().key();
      f.trigger(f.onlyTimer(), TimerFixture.NOW + 7000);
      f.commitActions();
    }
    try (var f = new TimerFixture(directory)) {
      assertEquals(key, f.onlyTimer().key());
      assertEquals(1, f.onlyTimer().value().getOccurrence());
      assertEquals(TimerFixture.NOW + 2000, f.onlyTimer().value().getDueDate());
      f.state.getTimerSubscriptionState().deleteCluster("demo");
      assertTrue(f.state.getTimerSubscriptionState().subscriptions().isEmpty());
      assertTrue(f.state.getTimerSubscriptionState().get(key).isEmpty());
      assertEquals(
          -1,
          f.state
              .getTimerSubscriptionState()
              .processTimersWithDueDateBefore(Long.MAX_VALUE, (ignored, value) -> fail("orphan")));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"[]", "null", "{broken", "{} {}", "{} true"})
  void rejectsInvalidVariablesBeforeAllocatingKey(final String variables) throws Exception {
    try (var f = new TimerFixture(directory)) {
      f.complete(42, TimerFixture.once().setVariables(variables), TimerFixture.NOW);
      verify(f.writers.rejection())
          .appendRejection(eq(RejectionType.INVALID_ARGUMENT), anyString());
      assertEquals(100, f.keys.get());
      assertTrue(f.state.getTimerSubscriptionState().subscriptions().isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"emptyTypes", "blankType", "duplicateTypes", "expression", "cluster", "name"})
  void rejectsInvalidDefinitions(final String field) throws Exception {
    try (var f = new TimerFixture(directory)) {
      var timer = TimerFixture.once();
      switch (field) {
        case "emptyTypes" -> timer.setJobTypes(List.of());
        case "blankType" -> timer.setJobTypes(List.of(" "));
        case "duplicateTypes" -> timer.setJobTypes(List.of("charge", "charge"));
        case "expression" -> timer.setExpression("PT0S");
        case "cluster" -> timer.setClusterId("");
        case "name" -> timer.setName("");
      }
      f.complete(42, timer, TimerFixture.NOW);
      verify(f.writers.rejection())
          .appendRejection(eq(RejectionType.INVALID_ARGUMENT), anyString());
      assertTrue(f.state.getTimerSubscriptionState().subscriptions().isEmpty());
    }
  }

  @Test
  void unknownClusterIsRejectedAndRepeatedCreatesAllowDifferentDefinitions() throws Exception {
    try (var f = new TimerFixture(directory)) {
      f.complete(42, TimerFixture.once().setClusterId("missing"), TimerFixture.NOW);
      verify(f.writers.rejection()).appendRejection(eq(RejectionType.NOT_FOUND), anyString());
      f.complete(42, TimerFixture.once(), TimerFixture.NOW);
      var original = f.onlyTimer();
      f.complete(42, TimerFixture.once().setExpression("PT2S"), TimerFixture.NOW);
      assertEquals(2, f.state.getTimerSubscriptionState().subscriptions().size());
      assertEquals(
          original.value().toJson(),
          f.state.getTimerSubscriptionState().get(original.key()).orElseThrow().toJson());
      verify(f.writers.rejection(), never())
          .appendRejection(eq(RejectionType.ALREADY_EXISTS), anyString());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"cluster", "tenant", "key"})
  void cancellationCannotDeleteAnotherTenantsTimer(final String field) throws Exception {
    try (var f = new TimerFixture(directory)) {
      f.complete(42, TimerFixture.once(), TimerFixture.NOW);
      var timer = f.onlyTimer();
      var request = new TimerSubscriptionRecord().setClusterId("demo").setTenantId("tenant");
      if (field.equals("cluster")) request.setClusterId("other");
      if (field.equals("tenant")) request.setTenantId("other");
      new TimerCancelProcessor(f.context)
          .processRecord(
              f.record(
                  field.equals("key") ? timer.key() + 1 : timer.key(), request, TimerFixture.NOW));
      verify(f.writers.rejection()).appendRejection(eq(RejectionType.NOT_FOUND), anyString());
      assertEquals(timer.key(), f.onlyTimer().key());
    }
  }

  @Test
  void timersAreIsolatedByTenantAndDefaultVariablesAreObjects() throws Exception {
    try (var f = new TimerFixture(directory)) {
      f.registerSubscription(3, TimerFixture.ordinary().setTenantId("other"));
      f.complete(42, TimerFixture.once().setVariables("{}"), TimerFixture.NOW);
      f.complete(42, TimerFixture.once().setTenantId("other"), TimerFixture.NOW);
      assertEquals(2, f.state.getTimerSubscriptionState().subscriptions().size());
      assertEquals("{}", new TimerSubscriptionRecord().getVariables());
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"missing", "partial", "deleted", "pending", "tenant", "cluster", "staleIndex"})
  void requiresEveryJobTypeToHaveAnActiveSubscriptionInTheCurrentScope(final String scenario)
      throws Exception {
    try (var f = new TimerFixture(directory)) {
      var timer = TimerFixture.once();
      switch (scenario) {
        case "missing" -> timer.setJobTypes(List.of("unknown"));
        case "partial" -> timer.setJobTypes(List.of("charge", "unknown"));
        case "deleted" -> f.state.getJobSubscriptionState().remove(1);
        case "pending" -> {
          f.state.getJobSubscriptionState().remove(1);
          f.distribution(3, TimerFixture.ordinary());
        }
        case "tenant" -> timer.setTenantId("other");
        case "cluster" -> {
          f.state
              .getBrokerClusterConfigState()
              .putClusterConfig(
                  "other",
                  new io.taskite.placementdriver.state.immutable.BrokerClusterConfig(1, 1, 1));
          timer.setClusterId("other");
        }
        case "staleIndex" ->
            f.state.getJobSubscriptionState().put(1, TimerFixture.ordinary().setTenantId("other"));
      }
      f.complete(42, timer, TimerFixture.NOW);
      verify(f.writers.rejection())
          .appendRejection(eq(RejectionType.NOT_FOUND), contains("JOB_SUBSCRIPTION_NOT_FOUND"));
      verify(f.writers.response())
          .writeRejectionOnCommand(
              any(), eq(RejectionType.NOT_FOUND), contains("JOB_SUBSCRIPTION_NOT_FOUND"));
      assertEquals(100, f.keys.get());
      assertTrue(f.state.getTimerSubscriptionState().subscriptions().isEmpty());
      assertTrue(f.events.isEmpty());
      assertTrue(f.actions.isEmpty());
    }
  }

  @Test
  void literalDurationUsesCommandTimestampAndSchedulesOnce() throws Exception {
    try (var f = new TimerFixture(directory)) {
      f.complete(42, TimerFixture.once().setExpression(" PT30S "), TimerFixture.NOW);
      verify(f.writers.rejection(), never()).appendRejection(any(), anyString());
      assertEquals("PT30S", f.onlyTimer().value().getExpression());
      assertEquals(TimerFixture.NOW + 30_000, f.onlyTimer().value().getDueDate());
    }
  }

  @Test
  void omittedVariablesDefaultToAnEmptyObject() throws Exception {
    try (var f = new TimerFixture(directory)) {
      f.complete(42, TimerFixture.once().setVariables(null), TimerFixture.NOW);
      assertEquals("{}", f.onlyTimer().value().getVariables());
      assertEquals(TimerFixture.NOW + 1000, f.onlyTimer().value().getDueDate());
    }
  }

  @Test
  void representableDeadlineOverflowConfirmsLastFiringAndEndsSchedule() throws Exception {
    try (var f = new TimerFixture(directory)) {
      f.complete(
          42,
          TimerFixture.once().setType(TimerType.CYCLE).setExpression("R/PT1S"),
          TimerFixture.NOW);
      var timer = f.onlyTimer();
      f.state
          .getTimerSubscriptionState()
          .put(timer.key(), timer.value().setDueDate(Long.MAX_VALUE));
      f.trigger(f.onlyTimer(), Long.MAX_VALUE);
      assertTrue(f.timerEvents.contains(TimerIntent.TRIGGERED));
      assertTrue(f.state.getTimerSubscriptionState().subscriptions().isEmpty());
    }
  }
}
