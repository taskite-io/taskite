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
package io.taskite.gateway.api;

import static org.junit.jupiter.api.Assertions.*;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.taskite.broker.client.BrokerClient;
import io.taskite.broker.client.request.BrokerRequest;
import io.taskite.broker.client.response.BrokerRequestRejectedBeforeAppendException;
import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.broker.client.response.PartitionNotReadyException;
import io.taskite.gateway.ManualConcurrencyControl;
import io.taskite.gateway.api.grpc.GatewayInterceptorTestContext;
import io.taskite.gateway.api.grpc.GatewayInterceptorUtil;
import io.taskite.gateway.broker.GatewayBrokerCommandDispatcher;
import io.taskite.gateway.protocol.ActivateJobsRequest;
import io.taskite.gateway.protocol.ActivateJobsResponse;
import io.taskite.gateway.routing.GatewayBrokerRouter;
import io.taskite.gateway.routing.GatewayTopologyCache;
import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.record.value.broker.JobBatchRecord;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.ClusterTopology;
import io.taskite.topology.Partition;
import io.taskite.topology.PartitionStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class JobActivationHandlerTest {
  @Test
  void defaultWaitExpiresOnceWithoutEmittingIntermediateEmptyBatches() {
    try (var fixture = new Fixture()) {
      final var observer = fixture.activate(request(0));
      assertEquals(List.of(2, 7), fixture.broker.partitions);
      assertTrue(observer.values.isEmpty());
      fixture.control.advance(9999);
      assertEquals(0, observer.terminals);
      fixture.control.advance(1);
      assertEquals(1, observer.terminals);
      assertNull(observer.error);
      assertEquals(List.of(ActivateJobsResponse.getDefaultInstance()), observer.values);
      assertEquals(0, fixture.control.pendingTimers());
      final int calls = fixture.broker.partitions.size();
      fixture.control.advance(10_000);
      assertEquals(calls, fixture.broker.partitions.size());
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {-1, -1000, Long.MIN_VALUE})
  void negativeWaitPerformsExactlyOneScan(long wait) {
    try (var fixture = new Fixture()) {
      final var observer = fixture.activate(request(wait));
      assertEquals(List.of(2, 7), fixture.broker.partitions);
      assertEquals(1, observer.terminals);
      assertNull(observer.error);
      assertEquals(0, fixture.control.pendingTimers());
    }
  }

  @Test
  void returnsNewlyAvailableJobsOnNextScanAndStopsPolling() {
    try (var fixture = new Fixture()) {
      final var observer = fixture.activate(request(500));
      fixture.control.advance(99);
      assertTrue(observer.values.isEmpty());
      fixture.broker.available = true;
      fixture.control.advance(1);
      assertEquals(List.of(2, 7, 7, 2), fixture.broker.partitions);
      assertEquals(2, observer.jobCount());
      assertEquals(1, observer.terminals);
      assertNull(observer.error);
      assertEquals(0, fixture.control.pendingTimers());
      fixture.control.advance(1000);
      assertEquals(4, fixture.broker.partitions.size());
    }
  }

  @Test
  void explicitWaitExpiresBeforeNextPoll() {
    try (var fixture = new Fixture()) {
      final var observer = fixture.activate(request(25));
      fixture.control.advance(24);
      assertEquals(0, observer.terminals);
      fixture.control.advance(1);
      assertEquals(1, observer.terminals);
      assertEquals(List.of(2, 7), fixture.broker.partitions);
      assertEquals(0, fixture.control.pendingTimers());
    }
  }

  @Test
  void cancellationWhileWaitingStopsTimersAndScans() {
    try (var fixture = new Fixture();
        var context = Context.current().withCancellation()) {
      final var observer = fixture.activate(context, request(500));
      context.cancel(null);
      assertEquals(Status.Code.CANCELLED, Status.fromThrowable(observer.error).getCode());
      assertEquals(1, observer.terminals);
      assertTrue(observer.values.isEmpty());
      fixture.control.advance(1000);
      assertEquals(List.of(2, 7), fixture.broker.partitions);
      assertEquals(0, fixture.control.pendingTimers());
    }
  }

  @Test
  void alreadyCancelledRequestNeverDispatches() {
    try (var fixture = new Fixture();
        var context = Context.current().withCancellation()) {
      context.cancel(null);
      final var observer = fixture.activate(context, request(500));
      assertEquals(Status.Code.CANCELLED, Status.fromThrowable(observer.error).getCode());
      assertTrue(fixture.broker.partitions.isEmpty());
      assertEquals(0, fixture.control.pendingTimers());
    }
  }

  @Test
  void timeoutDeliversLateBrokerReplyBeforeCompletingAndStopsFurtherPartitions() {
    try (var fixture = new Fixture()) {
      fixture.broker.hold = true;
      final var observer = fixture.activate(request(50));
      assertEquals(List.of(2), fixture.broker.partitions);
      fixture.control.advance(50);
      assertEquals(0, observer.terminals);
      assertTrue(observer.values.isEmpty());
      fixture.broker.reply();
      assertEquals(1, observer.jobCount());
      assertEquals(1, observer.terminals);
      assertEquals(List.of(2), fixture.broker.partitions);
      assertEquals(0, fixture.control.pendingTimers());
    }
  }

  @Test
  void cancellationSuppressesLateBrokerReply() {
    try (var fixture = new Fixture();
        var context = Context.current().withCancellation()) {
      fixture.broker.hold = true;
      final var observer = fixture.activate(context, request(50));
      context.cancel(null);
      fixture.broker.reply();
      assertTrue(observer.values.isEmpty());
      assertEquals(Status.Code.CANCELLED, Status.fromThrowable(observer.error).getCode());
      assertEquals(1, observer.terminals);
      assertEquals(List.of(2), fixture.broker.partitions);
    }
  }

  @Test
  void timeoutSuppressesLateTopologyLookup() {
    try (var fixture = new Fixture()) {
      final var topology = new CompletableActorFuture<ClusterTopology>();
      fixture.lookup = topology;
      final var observer = fixture.activate(request(50));
      fixture.control.advance(50);
      topology.complete(fixture.topology);
      assertEquals(1, observer.terminals);
      assertNull(observer.error);
      assertTrue(fixture.broker.partitions.isEmpty());
    }
  }

  @Test
  void brokerFailureTerminatesWithoutRetryingForever() {
    try (var fixture = new Fixture()) {
      fixture.broker.fail = true;
      final var observer = fixture.activate(request(500));
      assertEquals(Status.Code.UNAVAILABLE, Status.fromThrowable(observer.error).getCode());
      assertEquals(1, observer.terminals);
      fixture.control.advance(500);
      assertEquals(List.of(2), fixture.broker.partitions);
    }
  }

  @Test
  void explicitPreAppendRejectionContinuesToNextPartition() {
    try (var fixture = new Fixture()) {
      fixture.broker.firstError = new BrokerRequestRejectedBeforeAppendException();
      fixture.broker.available = true;
      final var observer = fixture.activate(request(-1));
      assertEquals(List.of(2, 7), fixture.broker.partitions);
      assertEquals(1, observer.jobCount());
      assertNull(observer.error);
    }
  }

  @Test
  void typedNotReadyContinuesToNextPartition() {
    try (var fixture = new Fixture()) {
      fixture.broker.firstError = new PartitionNotReadyException();
      fixture.broker.available = true;
      final var observer = fixture.activate(request(-1));
      assertEquals(List.of(2, 7), fixture.broker.partitions);
      assertEquals(1, observer.jobCount());
      assertNull(observer.error);
    }
  }

  @Test
  void readinessChangedAfterScanOnlySkipsTheRejectedCandidate() {
    try (var fixture = new Fixture()) {
      final var t = fixture.topology;
      final var first = t.partitions().getFirst();
      fixture.cache.update(
          new ClusterTopology(
              t.clusterId(),
              t.version() + 1,
              2,
              1,
              List.of(
                  new Partition(
                      first.partitionId(),
                      first.leadership(),
                      first.membership(),
                      PartitionStatus.REBALANCING,
                      false),
                  t.partitions().getLast()),
              t.brokers(),
              Instant.EPOCH));
      fixture.broker.available = true;
      final var observer = fixture.activate(request(-1));
      assertEquals(List.of(7), fixture.broker.partitions);
      assertEquals(1, observer.jobCount());
      assertNull(observer.error);
    }
  }

  @Test
  void unknownLateFailureIsPreservedAfterWaitExpires() {
    try (var fixture = new Fixture()) {
      fixture.broker.hold = true;
      final var observer = fixture.activate(request(50));
      fixture.control.advance(50);
      fixture.broker.lateFailure.run();
      assertEquals(Status.Code.UNAVAILABLE, Status.fromThrowable(observer.error).getCode());
      assertEquals(List.of(2), fixture.broker.partitions);
      assertTrue(observer.values.isEmpty());
      assertEquals(1, observer.terminals);
    }
  }

  @Test
  void closeEndsPendingRequestsAndRejectsNewRequests() {
    final var fixture = new Fixture();
    final var observer = fixture.activate(request(500));
    fixture.close();
    assertEquals(Status.Code.UNAVAILABLE, Status.fromThrowable(observer.error).getCode());
    assertEquals(1, observer.terminals);
    fixture.control.advance(500);
    assertEquals(List.of(2, 7), fixture.broker.partitions);
    final var later = fixture.activate(request(-1));
    assertEquals(Status.Code.UNAVAILABLE, Status.fromThrowable(later.error).getCode());
    assertEquals(0, fixture.control.pendingTimers());
  }

  @Test
  void tenantsAndPartitionsShareOneLimitAndDuplicateTenantIdsAreDeduplicated() {
    try (var fixture = new Fixture()) {
      fixture.broker.available = true;
      final var request =
          request(-1).toBuilder()
              .setMaxJobsToActivate(3)
              .addTenantIds("tenant-a")
              .addTenantIds("tenant-b")
              .addTenantIds("tenant-a")
              .build();
      final var observer = fixture.activateAuthorized(request, List.of("tenant-a", "tenant-b"));
      assertEquals(List.of(2, 2, 7), fixture.broker.partitions);
      assertEquals(List.of("tenant-a", "tenant-b", "tenant-a"), fixture.broker.tenants);
      assertEquals(List.of(3, 2, 1), fixture.broker.limits);
      assertEquals(3, observer.jobCount());
      assertNull(observer.error);
      assertEquals(1, observer.terminals);
    }
  }

  @Test
  void omittedTenantsUsesCurrentTenantAndExplicitSubsetUsesOnlyRequestedTenant() {
    try (var fixture = new Fixture()) {
      fixture.broker.available = true;
      fixture.activateAuthorized(request(-1), List.of("tenant-a", "tenant-b"));
      assertEquals(List.of("tenant-a", "tenant-a"), fixture.broker.tenants);
      fixture.broker.tenants.clear();
      final var observer =
          fixture.activateAuthorized(
              request(-1).toBuilder().addTenantIds("tenant-b").build(),
              List.of("tenant-a", "tenant-b"));
      assertEquals(List.of("tenant-b", "tenant-b"), fixture.broker.tenants);
      assertEquals(2, observer.jobCount());
    }
  }

  @Test
  void unauthorizedTenantRejectsWholeRequestBeforeDispatch() {
    try (var fixture = new Fixture()) {
      final var observer =
          fixture.activate(
              request(-1).toBuilder().addTenantIds("tenant-a").addTenantIds("tenant-b").build());
      assertEquals(Status.Code.PERMISSION_DENIED, Status.fromThrowable(observer.error).getCode());
      assertTrue(fixture.broker.partitions.isEmpty());
      assertEquals(0, fixture.control.pendingTimers());
    }
  }

  @Test
  void singleTenantContextClearsBroaderAuthorization() {
    try (var fixture = new Fixture()) {
      final var observer = new Observer();
      GatewayInterceptorTestContext.run(
          "cluster",
          "tenant-a",
          () ->
              GatewayInterceptorUtil.withAuthorizedTenantIds(List.of("tenant-a", "tenant-b"))
                  .run(
                      () ->
                          GatewayInterceptorUtil.withTenantId("tenant-a")
                              .run(
                                  () ->
                                      fixture.handler.activate(
                                          request(-1).toBuilder().addTenantIds("tenant-b").build(),
                                          observer,
                                          fixture.control))));
      assertEquals(Status.Code.PERMISSION_DENIED, Status.fromThrowable(observer.error).getCode());
      assertTrue(fixture.broker.partitions.isEmpty());
    }
  }

  private static ActivateJobsRequest request(long timeout) {
    return ActivateJobsRequest.newBuilder().setType("payment").setRequestTimeout(timeout).build();
  }

  private static final class Fixture implements AutoCloseable {
    private final ManualConcurrencyControl control = new ManualConcurrencyControl();
    private final TestBroker broker = new TestBroker();
    private final GatewayTopologyCache cache = new GatewayTopologyCache();
    private final ClusterTopology topology;
    private ActorFuture<ClusterTopology> lookup;
    private final JobActivationHandler handler;

    private Fixture() {
      final var endpoint = new BrokerEndpoint("broker", "localhost", 26500, 26600);
      topology =
          new ClusterTopology(
              "cluster",
              1,
              2,
              1,
              List.of(
                  new Partition(
                      2,
                      new Partition.Leadership("broker", 1),
                      new Partition.Membership(List.of(), List.of(), Map.of(), 1),
                      PartitionStatus.ACTIVE),
                  new Partition(
                      7,
                      new Partition.Leadership("broker", 1),
                      new Partition.Membership(List.of(), List.of(), Map.of(), 1),
                      PartitionStatus.ACTIVE)),
              Map.of("broker", endpoint),
              Instant.EPOCH);
      lookup = CompletableActorFuture.completed(topology);
      cache.update(topology);
      handler =
          new JobActivationHandler(
              new GatewayBrokerCommandDispatcher(broker, new GatewayBrokerRouter(cache)),
              ignored -> lookup);
    }

    private Observer activate(ActivateJobsRequest request) {
      return activate(Context.current(), request);
    }

    private Observer activate(Context context, ActivateJobsRequest request) {
      final var observer = new Observer();
      context.run(
          () ->
              GatewayInterceptorTestContext.run(
                  "cluster", "tenant-a", () -> handler.activate(request, observer, control)));
      return observer;
    }

    private Observer activateAuthorized(ActivateJobsRequest request, List<String> tenants) {
      final var observer = new Observer();
      GatewayInterceptorTestContext.run(
          "cluster",
          "tenant-a",
          () ->
              GatewayInterceptorUtil.withAuthorizedTenantIds(tenants)
                  .run(() -> handler.activate(request, observer, control)));
      return observer;
    }

    @Override
    public void close() {
      handler.close();
    }
  }

  private static final class TestBroker implements BrokerClient {
    private final List<Integer> partitions = new ArrayList<>();
    private final List<String> tenants = new ArrayList<>();
    private final List<Integer> limits = new ArrayList<>();
    private boolean available;
    private boolean hold;
    private boolean fail;
    private Throwable firstError;
    private Runnable lateReply;
    private Runnable lateFailure;

    @Override
    public <T> ActorFuture<BrokerResponse<T>> sendRequest(
        BrokerEndpoint endpoint, BrokerRequest<T> request) {
      partitions.add(request.getPartitionId());
      final var value = (JobBatchRecord) request.getRequestWriter();
      tenants.add(value.getTenantId());
      limits.add(value.getMaxJobsToActivate());
      final var future = new CompletableActorFuture<BrokerResponse<T>>();
      final var response = new JobBatchRecord();
      if (available || hold) {
        response
            .getJobs()
            .add(
                new JobBatchRecord.ActivatedJob(
                    Protocol.encodePartitionId(request.getPartitionId(), partitions.size()),
                    new JobRecord().setTenantId(value.getTenantId()).setJobType(value.getType())));
      }
      if (firstError != null && partitions.size() == 1) {
        future.completeExceptionally(firstError);
      } else if (fail) {
        future.completeExceptionally(Status.UNAVAILABLE.asRuntimeException());
      } else if (hold) {
        lateReply =
            () -> future.complete(request.mapResponse(request.getPartitionId(), 1, response));
        lateFailure = () -> future.completeExceptionally(Status.UNAVAILABLE.asRuntimeException());
      } else {
        future.complete(request.mapResponse(request.getPartitionId(), 1, response));
      }
      return future;
    }

    private void reply() {
      lateReply.run();
    }
  }

  private static final class Observer implements StreamObserver<ActivateJobsResponse> {
    private final List<ActivateJobsResponse> values = new ArrayList<>();
    private Throwable error;
    private int terminals;

    @Override
    public void onNext(ActivateJobsResponse value) {
      assertEquals(0, terminals);
      values.add(value);
    }

    @Override
    public void onError(Throwable error) {
      this.error = error;
      terminals++;
    }

    @Override
    public void onCompleted() {
      terminals++;
    }

    private int jobCount() {
      return values.stream().mapToInt(ActivateJobsResponse::getJobsCount).sum();
    }
  }
}
