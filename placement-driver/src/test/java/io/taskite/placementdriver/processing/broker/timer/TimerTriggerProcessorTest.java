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

import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.placementdriver.broker.distribution.BrokerDistributionRoute;
import io.taskite.placementdriver.processing.broker.timer.TimerFixture.TimerSnapshot;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord;
import io.taskite.protocol.record.intent.common.TimerIntent;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.util.BufferUtil;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class TimerTriggerProcessorTest {
  @TempDir Path directory;

  @Test
  void assignmentCommitsBeforeRpcAndRecoveryRetainsTheChosenPartition() throws Exception {
    try (final var fixture = new TimerFixture(directory)) {
      fixture.complete(42, TimerFixture.once(), TimerFixture.NOW);
      fixture.store.getCurrentTransaction().commit();
      final var timer = fixture.onlyTimer();
      final var command = fixture.record(timer.key(), timer.value(), timer.value().getDueDate());
      fixture.store.getCurrentTransaction();
      fixture.processor.processRecord(command);
      assertTrue(fixture.requests.isEmpty());
      assertEquals(2, fixture.onlyTimer().value().getPartitionId());
      fixture.store.getCurrentTransaction().rollback();
      assertEquals(0, fixture.onlyTimer().value().getPartitionId());
      fixture.store.getCurrentTransaction();
      fixture.processor.processRecord(command);
      fixture.commitActions();
      assertTrue(fixture.requests.isEmpty());
      assertTrue(fixture.timerEvents.contains(TimerIntent.ASSIGNED));
      fixture.processor.onRecovered(fixture.scheduling);
      final var newLeader = new BrokerEndpoint("new", "localhost", 26505, 26506);
      when(fixture.routes.resolveBrokerDistributionRoute("demo"))
          .thenReturn(new BrokerDistributionRoute(3, Map.of(1, fixture.endpoint, 2, newLeader)));
      fixture.process(fixture.onlyTimer(), timer.value().getDueDate());
      fixture.commitActions();
      assertEquals(2, fixture.requests.getFirst().getPartitionId());
      assertEquals(List.of(newLeader), fixture.endpoints);
      final var job = fixture.requests.getFirst().getRecordValue();
      assertEquals(0, job.getSubscriptionKey());
      assertEquals("charge", job.getType());
      assertEquals("tenant", job.getTenantId());
      assertEquals(timer.value().getVariables(), job.getVariablesJson());
    }
  }

  @Test
  void dispatchesOnlyAfterCommitUsingCurrentLeaderAndExactStartEvent() throws Exception {
    try (final var fixture = fixture()) {
      final var timer = fixture.onlyTimer();
      fixture.process(timer, TimerFixture.NOW + 999);
      fixture.commitActions();
      assertTrue(fixture.requests.isEmpty());
      fixture.process(timer, TimerFixture.NOW + 1000);
      assertTrue(fixture.requests.isEmpty(), "RPC must wait for transaction commit");
      fixture.commitActions();
      final var request = fixture.requests.getFirst();
      assertEquals(List.of(fixture.endpoint), fixture.endpoints);
      assertEquals("demo", request.getClusterId());
      assertEquals(2, request.getPartitionId());
      assertEquals(0, request.getRecordValue().getSubscriptionKey());
      assertEquals("charge", request.getRecordValue().getType());
      assertEquals("tenant", request.getRecordValue().getTenantId());
      assertEquals("{\"amount\":42}", request.getRecordValue().getVariablesJson());
      assertFalse(request.allowsTransportRetry());
      fixture.process(timer, TimerFixture.NOW + 2000);
      fixture.commitActions();
      assertEquals(1, fixture.requests.size(), "pending RPC is not resent");
      assertTrue(fixture.state.getTimerSubscriptionState().get(timer.key()).isPresent());
      fixture.success(0);
      fixture.process(fixture.commands.removeLast(), TimerFixture.NOW + 2000);
      fixture.commitActions();
      assertTrue(fixture.state.getTimerSubscriptionState().get(timer.key()).isEmpty());
    }
  }

  @Test
  void ignoresDuplicateTriggersAdmittedToOneUncommittedBatch() throws Exception {
    try (final var fixture = fixture()) {
      final var timer = fixture.onlyTimer();
      fixture.process(timer, timer.value().getDueDate());
      fixture.process(timer, timer.value().getDueDate());
      fixture.commitActions();
      assertEquals(1, fixture.requests.size());
    }
  }

  @Test
  void rpcFailureRetainsTimerAndRetriesWithNewLeaderAfterBackoff() throws Exception {
    try (final var fixture = fixture()) {
      final var timer = fixture.onlyTimer();
      fixture.process(timer, timer.value().getDueDate());
      fixture.commitActions();
      fixture.results.getFirst().completeExceptionally(new IllegalStateException("lost response"));
      fixture.callbacks();
      assertTrue(fixture.state.getTimerSubscriptionState().get(timer.key()).isPresent());
      fixture.process(timer, timer.value().getDueDate() + 999);
      fixture.commitActions();
      assertEquals(1, fixture.requests.size());
      final var nextLeader = new BrokerEndpoint("next", "localhost", 26503, 26504);
      when(fixture.routes.resolveBrokerDistributionRoute("demo"))
          .thenReturn(new BrokerDistributionRoute(2, Map.of(2, nextLeader)));
      fixture.process(timer, timer.value().getDueDate() + 1000);
      fixture.commitActions();
      assertEquals(List.of(fixture.endpoint, nextLeader), fixture.endpoints);
    }
  }

  @Test
  void successConfirmationBackpressureNeverResendsRpc() throws Exception {
    try (final var fixture = fixture()) {
      final var timer = fixture.onlyTimer();
      fixture.process(timer, timer.value().getDueDate());
      fixture.commitActions();
      fixture.capacity.set(false);
      fixture.success(0);
      assertTrue(fixture.commands.isEmpty());
      fixture.capacity.set(true);
      fixture.now.set(timer.value().getDueDate() + 1000);
      fixture.checker.check(fixture.commandWriter);
      fixture.process(fixture.commands.removeLast(), fixture.now.get());
      fixture.commitActions();
      assertEquals(1, fixture.requests.size());
      assertTrue(fixture.state.getTimerSubscriptionState().get(timer.key()).isEmpty());
    }
  }

  @Test
  void callbackCannotChangeStateBeforeActorTaskOrAfterNewRecovery() throws Exception {
    try (final var fixture = fixture()) {
      final var timer = fixture.onlyTimer();
      fixture.process(timer, timer.value().getDueDate());
      fixture.commitActions();
      fixture
          .results
          .getFirst()
          .complete(
              new BrokerResponse<>(fixture.requests.getFirst().getRecordValue().copy(), 2, 99));
      assertTrue(fixture.commands.isEmpty());
      fixture.processor.onRecovered(fixture.scheduling);
      fixture.callbacks();
      assertTrue(
          fixture.commands.isEmpty(), "old callback must not affect the new processor generation");
      fixture.process(timer, timer.value().getDueDate());
      fixture.commitActions();
      assertEquals(2, fixture.requests.size());
      assertEquals(
          fixture.requests.getFirst().getRecordValue().toJson(),
          fixture.requests.getLast().getRecordValue().toJson());
      fixture.success(1);
      fixture.process(fixture.commands.removeLast(), timer.value().getDueDate());
      fixture.commitActions();
      assertTrue(fixture.state.getTimerSubscriptionState().get(timer.key()).isEmpty());
    }
  }

  @Test
  void cancelledTimerDiscardsLateRpcSuccess() throws Exception {
    try (final var fixture = fixture()) {
      final var timer = fixture.onlyTimer();
      fixture.process(timer, timer.value().getDueDate());
      fixture.commitActions();
      fixture.delete(timer.key());
      fixture.success(0);
      assertTrue(fixture.commands.isEmpty());
      assertTrue(fixture.state.getTimerSubscriptionState().subscriptions().isEmpty());
    }
  }

  @Test
  void mismatchedResponseRetainsSubscriptionForRetry() throws Exception {
    try (final var fixture = fixture()) {
      final var timer = fixture.onlyTimer();
      fixture.process(timer, timer.value().getDueDate());
      fixture.commitActions();
      fixture.results.getFirst().complete(new BrokerResponse<>(new JobRecord(), 2, 99));
      fixture.callbacks();
      assertTrue(fixture.commands.isEmpty());
      assertTrue(fixture.state.getTimerSubscriptionState().get(timer.key()).isPresent());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"tenantId", "jobType"})
  void mismatchedJobIdentityRetainsSubscriptionForRetry(final String mismatch) throws Exception {
    try (final var fixture = fixture()) {
      final var timer = fixture.onlyTimer();
      fixture.process(timer, timer.value().getDueDate());
      fixture.commitActions();
      final var response = fixture.requests.getFirst().getRecordValue().copy();
      switch (mismatch) {
        case "tenantId" -> response.setTenantId("other");
        case "jobType" -> response.setJobType("other");
      }
      fixture.results.getFirst().complete(new BrokerResponse<>(response, 2, 99));
      fixture.callbacks();
      assertTrue(fixture.commands.isEmpty());
      assertTrue(fixture.state.getTimerSubscriptionState().get(timer.key()).isPresent());
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {0, 43})
  void acceptsTheSubscriptionKeyResolvedByBroker(final long subscriptionKey) throws Exception {
    try (final var fixture = fixture()) {
      final var timer = fixture.onlyTimer();
      fixture.process(timer, timer.value().getDueDate());
      fixture.commitActions();
      final var response =
          fixture.requests.getFirst().getRecordValue().copy().setSubscriptionKey(subscriptionKey);
      fixture.results.getFirst().complete(new BrokerResponse<>(response, 2, 99));
      fixture.callbacks();
      fixture.process(fixture.commands.removeLast(), timer.value().getDueDate());
      fixture.commitActions();
      assertEquals(1, fixture.requests.size());
      assertTrue(fixture.state.getTimerSubscriptionState().get(timer.key()).isEmpty());
    }
  }

  @Test
  void followerAndMissingRouteNeverSendRpc() throws Exception {
    try (final var fixture = fixture()) {
      final var timer = fixture.onlyTimer();
      fixture.leader.set(false);
      fixture.process(timer, timer.value().getDueDate());
      fixture.commitActions();
      fixture.leader.set(true);
      assertTrue(fixture.requests.isEmpty());
      when(fixture.routes.resolveBrokerDistributionRoute("demo"))
          .thenReturn(new BrokerDistributionRoute(2, Map.of()));
      fixture.process(timer, timer.value().getDueDate());
      fixture.commitActions();
      assertTrue(fixture.requests.isEmpty(), "no partition currently has a routable leader");
      assertTrue(fixture.state.getTimerSubscriptionState().get(timer.key()).isPresent());
    }
  }

  @Test
  void ordinaryJobSubscriptionStateDoesNotGateIndependentTimer() throws Exception {
    try (final var fixture = fixture()) {
      final var timer = fixture.onlyTimer();
      fixture.state.getJobSubscriptionState().remove(42);
      fixture.process(timer, timer.value().getDueDate());
      fixture.commitActions();
      assertEquals(1, fixture.requests.size());
      fixture.state.getJobSubscriptionState().remove(42);
      fixture.success(0);
      fixture.process(fixture.commands.removeLast(), timer.value().getDueDate());
      fixture.commitActions();
      assertEquals(1, fixture.requests.size());
      assertTrue(fixture.timerEvents.contains(TimerIntent.TRIGGERED));
    }
  }

  @Test
  void retriesAndRenewsAfterDistributionHistoryIsRemoved() throws Exception {
    try (final var fixture = new TimerFixture(directory)) {
      fixture.complete(
          42, TimerFixture.subscription(TimerFixture.NOW + 1000, 1000, 3), TimerFixture.NOW);
      final var timer = fixture.onlyTimer();
      fixture.process(timer, timer.value().getDueDate());
      fixture.commitActions();
      fixture.results.getFirst().completeExceptionally(new IllegalStateException("retry"));
      fixture.callbacks();
      fixture.state.getCommandDistributionState().deleteCluster("demo");
      assertTrue(fixture.state.getCommandDistributionState().distribution(42).isEmpty());
      fixture.trigger(timer, timer.value().getDueDate() + 1000);
      final var renewed = fixture.onlyTimer();
      assertEquals(2, renewed.value().getRepetitions());
      assertEquals(timer.value().getDueDate() + 1000, renewed.value().getDueDate());
      fixture.trigger(renewed, renewed.value().getDueDate());
      fixture.trigger(fixture.onlyTimer(), renewed.value().getDueDate() + 1000);
      assertTrue(fixture.state.getTimerSubscriptionState().subscriptions().isEmpty());
      assertTrue(
          fixture.requests.stream()
              .allMatch(request -> request.getRecordValue().getType().equals("charge")));
    }
  }

  @Test
  void recoversDefinitionBodyAndRotatesCurrentPartitionsWithoutDistributionHistory()
      throws Exception {
    try (final var fixture = new TimerFixture(directory)) {
      fixture.complete(
          42, TimerFixture.subscription(TimerFixture.NOW + 1000, 1000, 2), TimerFixture.NOW);
      fixture.state.getCommandDistributionState().deleteCluster("demo");
      fixture.store.getCurrentTransaction().commit();
    }
    try (final var fixture = new TimerFixture(directory)) {
      final var other = new BrokerEndpoint("other", "localhost", 26503, 26504);
      when(fixture.routes.resolveBrokerDistributionRoute("demo"))
          .thenReturn(new BrokerDistributionRoute(2, Map.of(1, other, 2, fixture.endpoint)));
      assertTrue(fixture.state.getCommandDistributionState().distribution(42).isEmpty());
      fixture.trigger(fixture.onlyTimer(), TimerFixture.NOW + 1000);
      fixture.trigger(fixture.onlyTimer(), TimerFixture.NOW + 2000);
      assertEquals(
          List.of(2, 2),
          fixture.requests.stream().map(request -> request.getPartitionId()).toList());
      assertEquals(List.of(fixture.endpoint, fixture.endpoint), fixture.endpoints);
      assertTrue(fixture.state.getTimerSubscriptionState().subscriptions().isEmpty());
    }
  }

  @Test
  void persistedDefinitionDoesNotWaitForDistributionToCompleteAgain() throws Exception {
    try (final var fixture = fixture()) {
      fixture.state.getCommandDistributionState().markPending(42, "demo", 1);
      final var timer = fixture.onlyTimer();
      fixture.trigger(timer, timer.value().getDueDate());
      assertTrue(fixture.state.getTimerSubscriptionState().subscriptions().isEmpty());
    }
  }

  @Test
  void capsInFlightAndFreesCapacityAfterCancellation() throws Exception {
    try (final var fixture = fixture()) {
      final var first = fixture.onlyTimer();
      for (int index = 0; index < 20; index++) {
        final long key = 200 + index;
        final var timer = new TimerSubscriptionRecord();
        timer.wrap(BufferUtil.wrapString(first.value().toJson()));
        fixture.state.getTimerSubscriptionState().put(key, timer);
        fixture.process(new TimerSnapshot(key, timer), timer.getDueDate());
        fixture.commitActions();
      }
      assertEquals(16, fixture.requests.size());
      fixture.state.getTimerSubscriptionState().remove(200, first.value());
      fixture.process(new TimerSnapshot(219, first.value()), first.value().getDueDate());
      assertEquals(
          16, fixture.requests.size(), "uncommitted cancellation must not release capacity");
      fixture.commitActions();
      fixture.process(new TimerSnapshot(219, first.value()), first.value().getDueDate());
      fixture.commitActions();
      assertEquals(17, fixture.requests.size());
    }
  }

  @Test
  void rolledBackTriggerHasNoNetworkSideEffectAndCanBeRetried() throws Exception {
    try (final var fixture = fixture()) {
      fixture.store.getCurrentTransaction().commit();
      final var timer = fixture.onlyTimer();
      fixture.process(timer, timer.value().getDueDate());
      fixture.store.getCurrentTransaction().rollback();
      fixture.actions.clear();
      assertTrue(fixture.requests.isEmpty());
      fixture.process(timer, timer.value().getDueDate());
      fixture.commitActions();
      assertEquals(1, fixture.requests.size());
    }
  }

  @Test
  void rolledBackConfirmationKeepsSuccessEvenWhenAnotherTriggerRunsInTheSameBatch()
      throws Exception {
    try (final var fixture = new TimerFixture(directory)) {
      fixture.complete(
          42, TimerFixture.subscription(TimerFixture.NOW + 1000, 1000, 2), TimerFixture.NOW);
      final var timer = fixture.onlyTimer();
      timer.value().setPartitionId(2);
      fixture.state.getTimerSubscriptionState().put(timer.key(), timer.value());
      final var other = new TimerSnapshot(200, timer.value());
      fixture.state.getTimerSubscriptionState().put(other.key(), other.value());
      fixture.process(timer, timer.value().getDueDate());
      fixture.commitActions();
      fixture.success(0);
      final var confirmation = fixture.commands.removeLast();
      fixture.process(confirmation, timer.value().getDueDate());
      assertEquals(
          1,
          fixture.state.getTimerSubscriptionState().get(timer.key()).orElseThrow().getOccurrence());
      fixture.process(other, timer.value().getDueDate());
      fixture.store.getCurrentTransaction().rollback();
      fixture.actions.clear();
      assertTrue(fixture.state.getTimerSubscriptionState().get(timer.key()).isPresent());
      fixture.process(confirmation, timer.value().getDueDate());
      fixture.commitActions();
      assertEquals(
          1, fixture.requests.size(), "confirmation retry must not resend a successful RPC");
      assertEquals(
          1,
          fixture.state.getTimerSubscriptionState().get(timer.key()).orElseThrow().getOccurrence());
      assertEquals(2, fixture.state.getTimerSubscriptionState().subscriptions().size());
    }
  }

  @Test
  void enforcesCapacityAcrossTriggersCommittedTogether() throws Exception {
    try (final var fixture = fixture()) {
      final var first = fixture.onlyTimer();
      for (int index = 0; index < 20; index++) {
        final long key = 200 + index;
        fixture.state.getTimerSubscriptionState().put(key, first.value());
        fixture.process(new TimerSnapshot(key, first.value()), first.value().getDueDate());
      }
      assertTrue(fixture.requests.isEmpty());
      fixture.commitActions();
      assertEquals(16, fixture.requests.size());
    }
  }

  private TimerFixture fixture() {
    final var fixture = new TimerFixture(directory);
    fixture.complete(42, TimerFixture.once(), TimerFixture.NOW);
    final var timer = fixture.onlyTimer();
    fixture.state.getTimerSubscriptionState().put(timer.key(), timer.value().setPartitionId(2));
    return fixture;
  }

  @Test
  void waitsForEveryJobTypeAndRetriesOnlyFailedTypesWithTheSameBody() throws Exception {
    try (var f = new TimerFixture(directory)) {
      f.complete(
          42, TimerFixture.once().setJobTypes(List.of("charge", "notify")), TimerFixture.NOW);
      var timer = f.onlyTimer();
      f.process(timer, timer.value().getDueDate());
      f.commitActions();
      assertEquals(
          List.of("charge", "notify"),
          f.requests.stream().map(r -> r.getRecordValue().getType()).toList());
      f.success(0);
      assertTrue(f.commands.isEmpty());
      f.results.get(1).completeExceptionally(new IllegalStateException("timeout"));
      f.callbacks();
      f.process(timer, timer.value().getDueDate() + 1000);
      f.commitActions();
      assertEquals(3, f.requests.size());
      assertEquals("notify", f.requests.get(2).getRecordValue().getType());
      assertEquals(
          f.requests.get(1).getRecordValue().toJson(), f.requests.get(2).getRecordValue().toJson());
      f.success(2);
      f.process(f.commands.removeLast(), timer.value().getDueDate() + 1000);
      f.commitActions();
      assertTrue(f.state.getTimerSubscriptionState().subscriptions().isEmpty());
    }
  }

  @Test
  void nextOccurrenceWriteFailurePropagatesForTransactionRollback() throws Exception {
    try (var f = new TimerFixture(directory)) {
      f.complete(42, TimerFixture.subscription(TimerFixture.NOW + 1000, 1000, 2), TimerFixture.NOW);
      var timer = f.onlyTimer();
      f.process(timer, timer.value().getDueDate());
      f.commitActions();
      f.success(0);
      var failure = new IllegalStateException("state write failed");
      var stateWriter = f.writers.state();
      doThrow(failure)
          .when(stateWriter)
          .appendFollowUpEvent(
              eq(timer.key()), eq(TimerIntent.CREATED), any(TimerSubscriptionRecord.class));
      assertSame(
          failure,
          assertThrows(
              IllegalStateException.class,
              () -> f.process(f.commands.removeLast(), timer.value().getDueDate())));
    }
  }
}
