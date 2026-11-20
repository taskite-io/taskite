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
package io.taskite.streamplatform.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.protocol.impl.encoding.ErrorResponse;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.record.ErrorCode;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.streamplatform.api.backpressure.BackpressureConfiguration;
import io.taskite.streamplatform.api.backpressure.BackpressureException;
import io.taskite.streamplatform.api.backpressure.PartitionRequestLimiter;
import io.taskite.streamplatform.api.metrics.CommandDispatchMetrics;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class StreamPartitionCommandDispatcherTest {
  private final DirectControl control = new DirectControl();

  @Test
  void responseOfAnotherValueTypeDoesNotCompleteRequest() {
    final var dispatcher = dispatcher(1, CompletableActorFuture.completed(2L));
    final var command = command("request-1");
    final var result = dispatcher.submit(command);
    assertFalse(result.isDone());
    final var child =
        event(
            ValueType.PARTITION_CHANGE,
            BrokerPartitionChangeIntent.SUBMITTED,
            new PartitionChangeRecord());
    dispatcher.onResponse(child);
    assertFalse(result.isDone());
    assertTrue(dispatcher.submit(command("request-2")).isCompletedExceptionally());
    dispatcher.onResponse(
        event(ValueType.MEMBER_CHANGE, MemberChangeIntent.PLANNED, new MemberChangeRecord()));
    assertTrue(result.isDone());
    assertFalse(result.isCompletedExceptionally());
    assertFalse(dispatcher.submit(command("request-2")).isCompletedExceptionally());
    dispatcher.close();
  }

  @Test
  void committedRejectionReturnsBusinessResponseAndReleasesPermit() {
    final var dispatcher = dispatcher(1, CompletableActorFuture.completed(2L));
    final var command = command("request-1");
    final var result = dispatcher.submit(command);
    final var rejection =
        event(ValueType.MEMBER_CHANGE, MemberChangeIntent.REQUEST, new MemberChangeRecord());
    rejection
        .metadata()
        .recordType(RecordType.COMMAND_REJECTION)
        .rejectionType(RejectionType.INVALID_STATE)
        .rejectionReason("stale epoch");
    dispatcher.onResponse(rejection);
    assertSame(rejection, result.join());
    assertEquals(RejectionType.INVALID_STATE, result.join().metadata().getRejectionType());
    assertEquals("stale epoch", result.join().metadata().getRejectionReason());
    assertFalse(dispatcher.submit(command("request-2")).isCompletedExceptionally());
    dispatcher.close();
  }

  @Test
  void responseIsDeliveredWhenObserverReceivesIt() {
    final var dispatcher = dispatcher(1, CompletableActorFuture.completed(2L));
    final var command = command("request-1");
    final var result = dispatcher.submit(command);
    final var response =
        event(ValueType.MEMBER_CHANGE, MemberChangeIntent.PLANNED, new MemberChangeRecord());
    dispatcher.onResponse(response);
    assertSame(response, result.join());
    dispatcher.close();
  }

  @Test
  void appendCompletionDoesNotCompleteRequests() {
    final var dispatcher = dispatcher(1, CompletableActorFuture.completed(2L));
    final var command = command("request-1");
    final var result = dispatcher.submit(command);
    assertFalse(result.isDone());
    dispatcher.onResponse(
        event(ValueType.MEMBER_CHANGE, MemberChangeIntent.PLANNED, new MemberChangeRecord()));
    assertTrue(result.isDone());
    dispatcher.close();
  }

  @Test
  void canceledCommandKeepsPermitUntilResponseAndLeadershipTransferWaits() {
    final var dispatcher = dispatcher(1, CompletableActorFuture.completed(2L));
    final var command = command("request-1");
    dispatcher.submit(command).cancel(false);
    assertTrue(dispatcher.submit(command("request-2")).isCompletedExceptionally());
    final var transfers = new AtomicInteger();
    final var transfer =
        dispatcher.transferLeadership(
            () -> {
              transfers.incrementAndGet();
              return CompletableActorFuture.completed(null);
            },
            Duration.ofSeconds(5));
    assertEquals(0, transfers.get());
    dispatcher.onResponse(
        event(ValueType.MEMBER_CHANGE, MemberChangeIntent.PLANNED, new MemberChangeRecord()));
    assertEquals(1, transfers.get());
    dispatcher.onLeadershipLost();
    assertTrue(transfer.isDone());
    assertFalse(transfer.isCompletedExceptionally());
    dispatcher.close();
  }

  @Test
  void commandsFailAndReleasePermitsOnAppendProcessingOrLeadershipFailure() {
    final var failure = new IllegalStateException("failure");
    assertSame(
        failure,
        dispatcher(1, CompletableActorFuture.completedExceptionally(failure))
            .submit(command("request-1"))
            .getException());
    for (final boolean leadershipLoss : List.of(false, true)) {
      final var dispatcher = dispatcher(1, CompletableActorFuture.completed(2L));
      final var command = command("request-1");
      final var result = dispatcher.submit(command);
      if (leadershipLoss) {
        dispatcher.onLeadershipLost();
      } else {
        dispatcher.onProcessingFailure(command, failure);
        assertSame(failure, result.getException());
      }
      assertTrue(result.isCompletedExceptionally());
      assertFalse(dispatcher.submit(command("request-2")).isCompletedExceptionally());
      dispatcher.close();
    }
  }

  @Test
  void cancellationDoesNotReleaseUntilActualProcessorResponse() {
    final var limiter =
        new PartitionRequestLimiter<Intent>(BackpressureConfiguration.fixed(1), ignored -> false);
    final var dispatcher =
        new StreamPartitionCommandDispatcher(
            1,
            limiter,
            () -> true,
            (entry, position) -> CompletableActorFuture.completed(2L),
            control,
            CommandDispatchMetrics.NOOP);
    final var canceled = dispatcher.submit(command("request-1"));
    canceled.cancel(false);
    assertEquals(1, limiter.inFlight());
    assertTrue(
        dispatcher.submit(command("request-2")).getException() instanceof BackpressureException);
    dispatcher.onResponse(
        event(ValueType.MEMBER_CHANGE, MemberChangeIntent.PLANNED, new MemberChangeRecord()));
    assertEquals(0, limiter.inFlight());
    assertFalse(dispatcher.submit(command("request-2")).isCompletedExceptionally());
    dispatcher.close();
  }

  @Test
  void exemptionsBypassSaturatedWindowAndDuplicateIdsDoNotOverwritePending() {
    final var limiter =
        new PartitionRequestLimiter<Intent>(
            BackpressureConfiguration.fixed(2), MemberChangeIntent.REQUEST::equals);
    final var appends = new AtomicInteger();
    final var dispatcher =
        new StreamPartitionCommandDispatcher(
            1,
            limiter,
            () -> true,
            (entry, position) -> {
              appends.incrementAndGet();
              return CompletableActorFuture.completed(2L);
            },
            control,
            CommandDispatchMetrics.NOOP);
    final var ordinary = command("request-1");
    ordinary.metadata().intent(MemberChangeIntent.PLANNED);
    final var first = dispatcher.submit(ordinary);
    assertTrue(dispatcher.submit(ordinary).getException() instanceof IllegalArgumentException);
    assertEquals(1, limiter.inFlight());
    final var second = command("request-2");
    second.metadata().intent(MemberChangeIntent.PLANNED);
    dispatcher.submit(second);
    assertEquals(2, limiter.inFlight());
    assertFalse(dispatcher.submit(command("management")).isCompletedExceptionally());
    assertEquals(2, limiter.inFlight());
    assertEquals(3, appends.get());
    dispatcher.onResponse(
        event(ValueType.MEMBER_CHANGE, MemberChangeIntent.PLANNED, new MemberChangeRecord()));
    assertTrue(first.isDone());
    assertEquals(1, limiter.inFlight());
    dispatcher.close();
    assertEquals(0, limiter.inFlight());
  }

  @Test
  void oldAppendCallbackCannotFailReusedRequestAfterLeadershipLoss() {
    final var oldAppend = new CompletableActorFuture<Long>();
    final var newAppend = new CompletableActorFuture<Long>();
    final var attempts = new AtomicInteger();
    final var limiter =
        new PartitionRequestLimiter<Intent>(BackpressureConfiguration.fixed(1), ignored -> false);
    final var dispatcher =
        new StreamPartitionCommandDispatcher(
            1,
            limiter,
            () -> true,
            (entry, position) -> attempts.incrementAndGet() == 1 ? oldAppend : newAppend,
            control,
            CommandDispatchMetrics.NOOP);
    dispatcher.submit(command("request-1"));
    dispatcher.onLeadershipLost();
    final var current = dispatcher.submit(command("request-1"));
    oldAppend.completeExceptionally(new IllegalStateException("old append"));
    assertFalse(current.isDone());
    assertEquals(1, limiter.inFlight());
    dispatcher.onResponse(
        event(ValueType.MEMBER_CHANGE, MemberChangeIntent.PLANNED, new MemberChangeRecord()));
    assertFalse(current.isCompletedExceptionally());
    assertEquals(0, limiter.inFlight());
    dispatcher.close();
  }

  @Test
  void businessRejectionReleasesPermitAsSuccessfulProcessing() {
    final var limiter =
        new PartitionRequestLimiter<Intent>(
            new BackpressureConfiguration(
                true,
                BackpressureConfiguration.Algorithm.AIMD,
                new BackpressureConfiguration.Aimd(Duration.ofDays(1), 1, 1, 2, 0.5),
                null,
                null),
            ignored -> false);
    final var dispatcher =
        new StreamPartitionCommandDispatcher(
            1,
            limiter,
            () -> true,
            (entry, position) -> CompletableActorFuture.completed(2L),
            control,
            CommandDispatchMetrics.NOOP);
    final var pending = dispatcher.submit(command("request-1"));
    final var rejection =
        event(ValueType.MEMBER_CHANGE, MemberChangeIntent.REQUEST, new MemberChangeRecord());
    rejection.metadata().recordType(RecordType.COMMAND_REJECTION);
    dispatcher.onResponse(rejection);
    assertSame(rejection, pending.join());
    assertEquals(2, limiter.limit());
    assertEquals(0, limiter.inFlight());
    dispatcher.close();
  }

  @Test
  void admissionAndTransferCannotRacePastAnAcceptedAppend() throws Exception {
    final var appenderEntered = new CountDownLatch(1);
    final var releaseAppender = new CountDownLatch(1);
    final var transferEntered = new CountDownLatch(1);
    final var transfers = new AtomicInteger();
    final var dispatcher =
        new StreamPartitionCommandDispatcher(
            1,
            2,
            () -> true,
            (entry, position) -> {
              appenderEntered.countDown();
              try {
                assertTrue(releaseAppender.await(5, TimeUnit.SECONDS));
              } catch (final InterruptedException error) {
                throw new IllegalStateException(error);
              }
              return CompletableActorFuture.completed(2L);
            },
            control);
    try (final var executor = Executors.newFixedThreadPool(2)) {
      final var submitted = executor.submit(() -> dispatcher.submit(command("request-1")));
      assertTrue(appenderEntered.await(5, TimeUnit.SECONDS));
      final var transferred =
          executor.submit(
              () -> {
                transferEntered.countDown();
                return dispatcher.transferLeadership(
                    () -> {
                      transfers.incrementAndGet();
                      return CompletableActorFuture.completed(null);
                    },
                    Duration.ofSeconds(5));
              });
      assertTrue(transferEntered.await(5, TimeUnit.SECONDS));
      releaseAppender.countDown();
      final var pending = submitted.get(5, TimeUnit.SECONDS);
      final var result = transferred.get(5, TimeUnit.SECONDS);
      assertFalse(pending.isDone());
      assertFalse(result.isDone());
      assertEquals(0, transfers.get());
      dispatcher.onResponse(
          event(ValueType.MEMBER_CHANGE, MemberChangeIntent.PLANNED, new MemberChangeRecord()));
      assertEquals(1, transfers.get());
      assertFalse(result.isDone());
      assertTrue(dispatcher.submit(command("request-2")).isCompletedExceptionally());
      dispatcher.onLeadershipLost();
      assertTrue(result.isDone());
      assertFalse(result.isCompletedExceptionally());
    } finally {
      releaseAppender.countDown();
      dispatcher.close();
    }
  }

  @Test
  void acknowledgedTransferStaysPausedUntilLeadershipActuallyChangesOrTimeout() {
    for (final boolean timeout : List.of(false, true)) {
      final var dispatcher = dispatcher(2, CompletableActorFuture.completed(2L));
      final var result =
          dispatcher.transferLeadership(
              () -> CompletableActorFuture.completed(null), Duration.ofSeconds(5));
      assertFalse(result.isDone());
      assertTrue(dispatcher.submit(command("request-1")).isCompletedExceptionally());
      if (timeout) {
        control.fireTimers();
      } else {
        dispatcher.onLeadershipLost();
      }
      assertTrue(result.isDone());
      assertEquals(timeout, result.isCompletedExceptionally());
      assertFalse(dispatcher.submit(command("request-2")).isCompletedExceptionally());
      dispatcher.close();
    }
  }

  @Test
  void plannedTransferWaitsForEveryResponseAndRejectsNewCommandsBeforeAppend() {
    final var appends = new AtomicInteger();
    final var transfers = new AtomicInteger();
    final var raftTransfer = new CompletableActorFuture<Void>();
    final var dispatcher =
        new StreamPartitionCommandDispatcher(
            1,
            4,
            () -> true,
            (entry, position) -> {
              appends.incrementAndGet();
              return CompletableActorFuture.completed(2L);
            },
            control);
    final var first = dispatcher.submit(command("request-1"));
    final var second = dispatcher.submit(command("request-2"));
    final var result =
        dispatcher.transferLeadership(
            () -> {
              assertTrue(first.isDone());
              assertTrue(second.isDone());
              transfers.incrementAndGet();
              return raftTransfer;
            },
            Duration.ofSeconds(5));
    final var handler =
        new CommandRequestHandler(dispatcher::submit, TypedEventRegistry.builder().build(), 0);
    final var bytes = handler.handleRequest(command("request-3")).join();
    final var rejection = new ErrorResponse();
    rejection.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
    assertEquals(ErrorCode.PARTITION_LEADER_MISMATCH, rejection.getErrorCode());
    assertEquals(2, appends.get());
    assertEquals(0, transfers.get());
    dispatcher.onResponse(
        event(
            ValueType.PARTITION_CHANGE,
            BrokerPartitionChangeIntent.SUBMITTED,
            new PartitionChangeRecord()));
    assertEquals(0, transfers.get());
    dispatcher.onResponse(
        event(ValueType.MEMBER_CHANGE, MemberChangeIntent.PLANNED, new MemberChangeRecord()));
    assertFalse(result.isDone());
    assertEquals(0, transfers.get());
    final var response =
        event(ValueType.MEMBER_CHANGE, MemberChangeIntent.PLANNED, new MemberChangeRecord());
    response.metadata().requestId("request-2");
    dispatcher.onResponse(response);
    assertEquals(1, transfers.get());
    dispatcher.onLeadershipLost();
    assertFalse(first.isCompletedExceptionally());
    assertFalse(second.isCompletedExceptionally());
    assertFalse(result.isDone());
    raftTransfer.complete(null);
    assertFalse(result.isCompletedExceptionally());
    assertTrue(result.isDone());
  }

  @Test
  void drainTimeoutResumesAdmissionWithoutFailingAcceptedCommandOrTransferring() {
    final var dispatcher = dispatcher(2, CompletableActorFuture.completed(2L));
    final var pending = dispatcher.submit(command("request-1"));
    final var transfers = new AtomicInteger();
    final var result =
        dispatcher.transferLeadership(
            () -> {
              transfers.incrementAndGet();
              return CompletableActorFuture.completed(null);
            },
            Duration.ofSeconds(5));
    control.fireTimers();
    assertTrue(result.isCompletedExceptionally());
    assertFalse(pending.isDone());
    assertEquals(0, transfers.get());
    assertFalse(dispatcher.submit(command("request-2")).isCompletedExceptionally());
    dispatcher.onResponse(
        event(ValueType.MEMBER_CHANGE, MemberChangeIntent.PLANNED, new MemberChangeRecord()));
    assertFalse(pending.isCompletedExceptionally());
    assertEquals(0, transfers.get());
  }

  @Test
  void transferFailureReleasesPauseAndOverlappingTransferCannotReplaceOwner() {
    final var dispatcher = dispatcher(2, CompletableActorFuture.completed(2L));
    final var transfer = new CompletableActorFuture<Void>();
    final var first = dispatcher.transferLeadership(() -> transfer, Duration.ofSeconds(5));
    final var second =
        dispatcher.transferLeadership(
            () -> {
              throw new AssertionError("overlapping transfer invoked");
            },
            Duration.ofSeconds(5));
    assertTrue(second.isCompletedExceptionally());
    assertTrue(dispatcher.submit(command("request-1")).isCompletedExceptionally());
    assertFalse(first.isDone());
    final var failure = new IllegalStateException("raft transfer failed");
    transfer.completeExceptionally(failure);
    assertSame(failure, first.getException());
    assertFalse(dispatcher.submit(command("request-2")).isCompletedExceptionally());
  }

  @Test
  void closeAndUnexpectedLeadershipLossAbortDrainWithoutCallingRaft() {
    for (final boolean close : List.of(false, true)) {
      final var dispatcher = dispatcher(2, CompletableActorFuture.completed(2L));
      final var pending = dispatcher.submit(command("request-1"));
      final var result =
          dispatcher.transferLeadership(
              () -> {
                throw new AssertionError("must not transfer after drain is aborted");
              },
              Duration.ofSeconds(5));
      if (close) {
        dispatcher.close();
      } else {
        dispatcher.onLeadershipLost();
      }
      assertTrue(pending.isCompletedExceptionally());
      assertTrue(result.isCompletedExceptionally());
      control.fireTimers();
      assertTrue(result.isCompletedExceptionally());
    }
  }

  @Test
  void synchronousTransferFailureAndInvalidTimeoutDoNotPauseAdmission() {
    final var dispatcher = dispatcher(2, CompletableActorFuture.completed(2L));
    assertTrue(
        dispatcher
            .transferLeadership(
                () -> {
                  throw new AssertionError("invalid timeout called raft");
                },
                Duration.ZERO)
            .isCompletedExceptionally());
    final var failure = new IllegalStateException("authority changed");
    final var result =
        dispatcher.transferLeadership(
            () -> {
              throw failure;
            },
            Duration.ofSeconds(5));
    assertSame(failure, result.getException());
    assertFalse(dispatcher.submit(command("request-1")).isCompletedExceptionally());
  }

  @Test
  void followerRejectionIsEncodedBeforeAnyAppendAttempt() {
    final var appends = new AtomicInteger();
    final var dispatcher =
        new StreamPartitionCommandDispatcher(
            1,
            1,
            () -> false,
            (entry, position) -> {
              appends.incrementAndGet();
              return CompletableActorFuture.completed(2L);
            },
            control);
    final var handler =
        new CommandRequestHandler(dispatcher::submit, TypedEventRegistry.builder().build(), 0);
    final var bytes = handler.handleRequest(command("request-1")).join();
    final var error = new ErrorResponse();
    error.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
    assertEquals(ErrorCode.PARTITION_LEADER_MISMATCH, error.getErrorCode());
    assertEquals(0, appends.get());
  }

  @Test
  void appendFailureWithLeaderMessageRemainsUnknown() {
    final var failure = new IllegalStateException("not leader for partition 1");
    final var dispatcher = dispatcher(1, CompletableActorFuture.completedExceptionally(failure));
    final var handler =
        new CommandRequestHandler(dispatcher::submit, TypedEventRegistry.builder().build(), 0);
    final var result = handler.handleRequest(command("request-1"));
    assertTrue(result.isCompletedExceptionally());
    assertSame(failure, result.getException());
  }

  @Test
  void leadershipLossAfterAppendRemainsUnknown() {
    final var dispatcher = dispatcher(1, CompletableActorFuture.completed(2L));
    final var handler =
        new CommandRequestHandler(dispatcher::submit, TypedEventRegistry.builder().build(), 0);
    final var result = handler.handleRequest(command("request-1"));
    dispatcher.onLeadershipLost();
    assertTrue(result.isCompletedExceptionally());
    assertEquals("partition 1 lost leadership", result.getException().getMessage());
  }

  @Test
  void ignoresChildEventAndAcceptsResponseMatchingCommandValueType() {
    final StreamPartitionCommandDispatcher dispatcher =
        dispatcher(1, CompletableActorFuture.completed(2L));
    final ActorFuture<LoggedEvent> response = dispatcher.submit(command("request-1"));

    dispatcher.onResponse(
        event(
            ValueType.PARTITION_CHANGE,
            BrokerPartitionChangeIntent.SUBMITTED,
            new PartitionChangeRecord()));
    assertFalse(response.isDone());

    final LoggedEvent accepted =
        event(ValueType.MEMBER_CHANGE, MemberChangeIntent.PLANNED, new MemberChangeRecord());
    dispatcher.onResponse(accepted);

    assertSame(accepted, response.join());
  }

  @Test
  void enforcesInFlightLimitUntilResponseCompletes() {
    final StreamPartitionCommandDispatcher dispatcher =
        dispatcher(1, CompletableActorFuture.completed(2L));
    final ActorFuture<LoggedEvent> first = dispatcher.submit(command("request-1"));

    assertTrue(dispatcher.submit(command("request-2")).isCompletedExceptionally());

    dispatcher.onResponse(
        event(ValueType.MEMBER_CHANGE, MemberChangeIntent.PLANNED, new MemberChangeRecord()));
    assertFalse(first.isCompletedExceptionally());
    assertFalse(dispatcher.submit(command("request-3")).isCompletedExceptionally());
  }

  @Test
  void rejectsFollowersAndFailsPendingOnClose() {
    final StreamPartitionCommandDispatcher follower =
        new StreamPartitionCommandDispatcher(
            1,
            1,
            () -> false,
            (entry, sourcePosition) -> CompletableActorFuture.completed(2L),
            control);
    assertTrue(follower.submit(command("request-1")).isCompletedExceptionally());

    final StreamPartitionCommandDispatcher leader =
        dispatcher(1, CompletableActorFuture.completed(2L));
    final ActorFuture<LoggedEvent> pending = leader.submit(command("request-2"));
    leader.close();

    assertTrue(pending.isCompletedExceptionally());
    assertTrue(leader.submit(command("request-3")).isCompletedExceptionally());
  }

  @Test
  void appendFailureFailsRequestAndReleasesInFlightPermit() {
    final AtomicInteger appendAttempts = new AtomicInteger();
    final StreamPartitionCommandDispatcher dispatcher =
        new StreamPartitionCommandDispatcher(
            1,
            1,
            () -> true,
            (entry, sourcePosition) ->
                appendAttempts.incrementAndGet() == 1
                    ? CompletableActorFuture.completedExceptionally(
                        new IllegalStateException("append failed"))
                    : CompletableActorFuture.completed(2L),
            control);

    assertTrue(dispatcher.submit(command("request-1")).isCompletedExceptionally());
    assertFalse(dispatcher.submit(command("request-2")).isCompletedExceptionally());
    assertEquals(2, appendAttempts.get());
  }

  @Test
  void leadershipLossFailsPendingRequestsAndReleasesInFlightPermit() {
    final StreamPartitionCommandDispatcher dispatcher =
        dispatcher(1, CompletableActorFuture.completed(2L));
    final ActorFuture<LoggedEvent> pending = dispatcher.submit(command("request-1"));

    dispatcher.onLeadershipLost();

    assertTrue(pending.isCompletedExceptionally());
    assertFalse(dispatcher.submit(command("request-2")).isCompletedExceptionally());
  }

  @Test
  void lateAppendFailureDoesNotOverrideCommittedResponse() {
    final CompletableActorFuture<Long> append = new CompletableActorFuture<>();
    final StreamPartitionCommandDispatcher dispatcher = dispatcher(1, append);
    final ActorFuture<LoggedEvent> response = dispatcher.submit(command("request-1"));
    final LoggedEvent committed =
        event(ValueType.MEMBER_CHANGE, MemberChangeIntent.PLANNED, new MemberChangeRecord());

    dispatcher.onResponse(committed);
    append.completeExceptionally(new IllegalStateException("late append failure"));

    assertSame(committed, response.join());
  }

  @Test
  void recordsPendingRaftAppendAndProcessingPhasesWithoutChangingCompletion() {
    final CompletableActorFuture<Long> append = new CompletableActorFuture<>();
    final RecordingCommandDispatchMetrics metrics = new RecordingCommandDispatchMetrics();
    final ArrayDeque<Long> nanoTimes = new ArrayDeque<>(List.of(100L, 300L, 800L));
    final StreamPartitionCommandDispatcher dispatcher =
        new StreamPartitionCommandDispatcher(
            1,
            1,
            () -> true,
            (entry, sourcePosition) -> append,
            control,
            metrics,
            nanoTimes::removeFirst);

    final ActorFuture<LoggedEvent> response = dispatcher.submit(command("request-1"));
    append.complete(2L);
    final LoggedEvent committed =
        event(ValueType.MEMBER_CHANGE, MemberChangeIntent.PLANNED, new MemberChangeRecord());
    dispatcher.onResponse(committed);
    dispatcher.onResponse(committed);

    assertSame(committed, response.join());
    assertEquals(List.of(0), metrics.pending);
    assertEquals(List.of(200L), metrics.raftAppendNanos);
    assertEquals(List.of(500L), metrics.processingNanos);

    dispatcher.close();
    assertTrue(metrics.closed);
  }

  @Test
  void metricsFailuresDoNotChangeCommandCompletion() {
    final StreamPartitionCommandDispatcher dispatcher =
        new StreamPartitionCommandDispatcher(
            1,
            1,
            () -> true,
            (entry, sourcePosition) -> CompletableActorFuture.completed(2L),
            control,
            new FailingCommandDispatchMetrics());
    final ActorFuture<LoggedEvent> response = dispatcher.submit(command("request-1"));
    final LoggedEvent committed =
        event(ValueType.MEMBER_CHANGE, MemberChangeIntent.PLANNED, new MemberChangeRecord());

    dispatcher.onResponse(committed);

    assertSame(committed, response.join());
    dispatcher.close();
  }

  private StreamPartitionCommandDispatcher dispatcher(
      final int maxInFlight, final ActorFuture<Long> appendResult) {
    return new StreamPartitionCommandDispatcher(
        1, maxInFlight, () -> true, (entry, sourcePosition) -> appendResult, control);
  }

  private static LoggedEvent command(final String requestId) {
    return LoggedEvent.ofRecord(
        1,
        new RecordMetadata()
            .requestId(requestId)
            .recordType(RecordType.COMMAND)
            .valueType(ValueType.MEMBER_CHANGE)
            .intent(MemberChangeIntent.REQUEST),
        new MemberChangeRecord());
  }

  private static LoggedEvent event(
      final ValueType valueType, final Intent intent, final UnifiedRecordValue value) {
    return LoggedEvent.ofRecord(
        1,
        new RecordMetadata()
            .requestId("request-1")
            .recordType(RecordType.EVENT)
            .valueType(valueType)
            .intent(intent),
        value);
  }

  private static final class DirectControl implements ConcurrencyControl {
    private final List<Runnable> timers = new ArrayList<>();

    void fireTimers() {
      for (final var timer : List.copyOf(timers)) {
        timer.run();
      }
    }

    @Override
    public <T> void runOnCompletion(
        final ActorFuture<T> future, final BiConsumer<T, Throwable> callback) {
      future.whenComplete(callback);
    }

    @Override
    public void run(final Runnable action) {
      action.run();
    }

    @Override
    public <T> ActorFuture<T> call(final Callable<T> callable) {
      try {
        return CompletableActorFuture.completed(callable.call());
      } catch (final Exception error) {
        return CompletableActorFuture.completedExceptionally(error);
      }
    }

    @Override
    public ScheduledTimer schedule(final Duration delay, final Runnable runnable) {
      timers.add(runnable);
      return () -> timers.remove(runnable);
    }
  }

  private static final class RecordingCommandDispatchMetrics implements CommandDispatchMetrics {
    private final List<Integer> pending = new ArrayList<>();
    private final List<Long> raftAppendNanos = new ArrayList<>();
    private final List<Long> processingNanos = new ArrayList<>();
    private boolean closed;

    @Override
    public void onSubmitted(final int pendingCommands) {
      pending.add(pendingCommands);
    }

    @Override
    public void onRaftAppendCompleted(final long durationNanos) {
      raftAppendNanos.add(durationNanos);
    }

    @Override
    public void onProcessingCompleted(final long durationNanos) {
      processingNanos.add(durationNanos);
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  private static final class FailingCommandDispatchMetrics implements CommandDispatchMetrics {
    @Override
    public void onSubmitted(final int pendingCommands) {
      throw new IllegalStateException("pending metric failed");
    }

    @Override
    public void onRaftAppendCompleted(final long durationNanos) {
      throw new IllegalStateException("append metric failed");
    }

    @Override
    public void onProcessingCompleted(final long durationNanos) {
      throw new IllegalStateException("processing metric failed");
    }

    @Override
    public void close() {
      throw new IllegalStateException("metrics close failed");
    }
  }
}
