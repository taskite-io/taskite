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
package io.taskite.placementdriver.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.taskite.placementdriver.api.PlacementDriverEndpointManager.BrokerTopologyWatchPort;
import io.taskite.placementdriver.api.PlacementDriverEndpointManager.PlacementDriverClusterTopologyWatchPort;
import io.taskite.placementdriver.api.PlacementDriverEndpointManager.WorkerRegistryQueryPort;
import io.taskite.placementdriver.processing.PlacementDriverProcessors;
import io.taskite.placementdriver.protocol.BrokerTopology;
import io.taskite.placementdriver.protocol.GetBrokerTopologyRequest;
import io.taskite.placementdriver.protocol.PlacementDriverTopology;
import io.taskite.placementdriver.protocol.RegisterWorkerRequest;
import io.taskite.placementdriver.protocol.RegisterWorkerResponse;
import io.taskite.placementdriver.protocol.WatchPlacementDriverTopologyRequest;
import io.taskite.placementdriver.runtime.PlacementDriverCommandSubmitter;
import io.taskite.protocol.impl.encoding.ExecuteCommandRequest;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberExecutionRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

final class PlacementDriverEndpointManagerTest {

  @Test
  void reportsUnavailableForGrpcRequestsBeforeRuntimeBinding() {
    final PlacementDriverEndpointManager manager = new PlacementDriverEndpointManager();
    final RecordingObserver<byte[]> command = new RecordingObserver<>();
    final RecordingObserver<BrokerTopology> brokerTopology = new RecordingObserver<>();
    final RecordingObserver<PlacementDriverTopology> clusterTopology = new RecordingObserver<>();
    final RecordingObserver<RegisterWorkerResponse> workerRegistration = new RecordingObserver<>();

    manager.submitCommand(new byte[0], command);
    manager.watchBrokerTopology(GetBrokerTopologyRequest.getDefaultInstance(), brokerTopology);
    manager.registerWorker(RegisterWorkerRequest.getDefaultInstance(), workerRegistration);
    manager.watchPlacementDriverClusterTopology(
        WatchPlacementDriverTopologyRequest.getDefaultInstance(), clusterTopology);

    assertUnavailable(command);
    assertUnavailable(brokerTopology);
    assertUnavailable(clusterTopology);
    assertUnavailable(workerRegistration);
  }

  @ParameterizedTest
  @EnumSource(
      value = MemberChangeIntent.class,
      names = {
        "ADVANCE",
        "EXECUTE",
        "EXECUTION_RESULT",
        "JOIN",
        "LEAVE",
        "PROMOTE",
        "TRANSFER_LEADER"
      })
  void rejectsInternalMemberCommandsAtExternalSubmissionPort(final MemberChangeIntent intent) {
    final var manager = new PlacementDriverEndpointManager();
    final var submitter = mock(PlacementDriverCommandSubmitter.class);
    manager.bindRuntime(
        submitter,
        PlacementDriverProcessors.createEventRegistry(),
        () -> null,
        mock(BrokerTopologyWatchPort.class),
        mock(PlacementDriverClusterTopologyWatchPort.class),
        mock(WorkerRegistryQueryPort.class),
        topology -> null);
    final UnifiedRecordValue value =
        intent == MemberChangeIntent.EXECUTION_RESULT
            ? new MemberExecutionRecord()
            : new MemberChangeRecord()
                .setResourceId("taskite-placement-driver")
                .setChangeId("change");
    final byte[] bytes = new byte[value.getLength()];
    value.write(new UnsafeBuffer(bytes), 0);
    final var request =
        new ExecuteCommandRequest()
            .setPartitionId(1)
            .setValueType(ValueType.MEMBER_CHANGE)
            .setIntent(intent)
            .setValue(new UnsafeBuffer(bytes), 0, bytes.length);
    final byte[] envelope = new byte[request.getLength()];
    request.write(new UnsafeBuffer(envelope), 0);
    final RecordingObserver<byte[]> observer = new RecordingObserver<>();
    manager.submitCommand(envelope, observer);
    assertNotNull(observer.error.get());
    assertEquals(
        Status.Code.INVALID_ARGUMENT, Status.fromThrowable(observer.error.get()).getCode());
    assertEquals(
        "Internal command requires the internal submission port",
        Status.fromThrowable(observer.error.get()).getDescription());
    verifyNoInteractions(submitter);
  }

  @Test
  void keepsMemberRequestsAvailableAtExternalSubmissionPort() {
    final var manager = new PlacementDriverEndpointManager();
    final var submitter = mock(PlacementDriverCommandSubmitter.class);
    when(submitter.submitPlacementDriverCommand(any()))
        .thenAnswer(call -> CompletableActorFuture.completed(call.getArgument(0)));
    manager.bindRuntime(
        submitter,
        PlacementDriverProcessors.createEventRegistry(),
        () -> null,
        mock(BrokerTopologyWatchPort.class),
        mock(PlacementDriverClusterTopologyWatchPort.class),
        mock(WorkerRegistryQueryPort.class),
        topology -> null);
    final var value =
        new MemberChangeRecord().setResourceId("taskite-placement-driver").setChangeId("change");
    final byte[] bytes = new byte[value.getLength()];
    value.write(new UnsafeBuffer(bytes), 0);
    final var request =
        new ExecuteCommandRequest()
            .setPartitionId(1)
            .setValueType(ValueType.MEMBER_CHANGE)
            .setIntent(MemberChangeIntent.REQUEST)
            .setValue(new UnsafeBuffer(bytes), 0, bytes.length);
    final byte[] envelope = new byte[request.getLength()];
    request.write(new UnsafeBuffer(envelope), 0);
    final RecordingObserver<byte[]> observer = new RecordingObserver<>();
    manager.submitCommand(envelope, observer);
    verify(submitter).submitPlacementDriverCommand(any());
    assertNull(observer.error.get());
    assertNotNull(observer.value.get());
  }

  private static void assertUnavailable(final RecordingObserver<?> observer) {
    final Throwable error = observer.error.get();
    assertNotNull(error);
    assertEquals(Status.Code.UNAVAILABLE, Status.fromThrowable(error).getCode());
    assertEquals(
        "placement-driver runtime is not ready", Status.fromThrowable(error).getDescription());
    assertNull(observer.value.get());
  }

  private static final class RecordingObserver<T> implements StreamObserver<T> {
    private final AtomicReference<T> value = new AtomicReference<>();
    private final AtomicReference<Throwable> error = new AtomicReference<>();

    @Override
    public void onNext(final T next) {
      value.set(next);
    }

    @Override
    public void onError(final Throwable throwable) {
      error.set(throwable);
    }

    @Override
    public void onCompleted() {}
  }
}
