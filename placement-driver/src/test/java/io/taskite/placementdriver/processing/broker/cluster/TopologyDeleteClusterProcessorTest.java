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
package io.taskite.placementdriver.processing.broker.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.state.appliers.BrokerClusterDeletedApplier;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.mirror.JobSubscriptionMirrorRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.StateWriter;
import io.taskite.streamplatform.api.writer.TypedRejectionWriter;
import io.taskite.streamplatform.api.writer.TypedResponseWriter;
import io.taskite.streamplatform.api.writer.Writers;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class TopologyDeleteClusterProcessorTest {
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void acceptsDeletionWithNoBrokersOrOnlyOfflineBrokers(final boolean registeredBroker) {
    try (var storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      if (registeredBroker) {
        registerBroker(state);
        state.getBrokerRegistrationState().markBrokerOffline("a", "broker-0", Instant.EPOCH);
      }
      final var fixture = new Fixture(state);
      final var value = new ClusterConfigRecord().setClusterId("a");
      final var record = fixture.process(value);

      verifyNoInteractions(fixture.rejection);
      verify(fixture.events).appendFollowUpEvent(1L, TopologyIntent.CLUSTER_DELETED, value);
      verify(fixture.response)
          .writeEventOnCommand(1L, TopologyIntent.CLUSTER_DELETED, value, record);
      // Only applying the committed event mutates mirror state.
      assertEquals(2, state.getJobSubscriptionMirrorState().mirrors().size());
      new BrokerClusterDeletedApplier(state).applyState(1L, value);
      assertTrue(state.getJobSubscriptionMirrorState().mirrors().isEmpty());
      assertTrue(state.getJobSubscriptionMirrorState().mirror("a", "b").isEmpty());
      assertTrue(state.getJobSubscriptionMirrorState().mirror("b", "a").isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONLINE", "DRAINING"})
  void rejectedDeletionKeepsAllMirrors(final String status) {
    try (var storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      registerBroker(state);
      if (BrokerStatus.valueOf(status) == BrokerStatus.DRAINING) {
        state.getBrokerRegistrationState().markBrokerDraining("a", "broker-0", Instant.EPOCH);
      }
      final var fixture = new Fixture(state);
      final var record = fixture.process(new ClusterConfigRecord().setClusterId("a"));

      verify(fixture.rejection)
          .appendRejection(eq(RejectionType.INVALID_STATE), contains("CLUSTER_HAS_ACTIVE_BROKERS"));
      verify(fixture.response)
          .writeRejectionOnCommand(
              eq(record), eq(RejectionType.INVALID_STATE), contains("CLUSTER_HAS_ACTIVE_BROKERS"));
      verifyNoInteractions(fixture.events);
      assertEquals(2, state.getJobSubscriptionMirrorState().mirrors().size());
      assertTrue(state.getJobSubscriptionMirrorState().mirror("a", "b").isPresent());
      assertTrue(state.getJobSubscriptionMirrorState().mirror("b", "a").isPresent());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void persistedMirrorTargetPreventsDeletionEvenAfterAcknowledgement(final boolean completed) {
    try (var storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      final var distributions = state.getCommandDistributionState();
      distributions.putDistribution(
          new CommandDistributionRecord<>()
              .setDistributionKey(10)
              .setClusterId("b")
              .setValueType(ValueType.JOB_SUBSCRIPTION)
              .setIntent(JobSubscriptionIntent.CREATE.value())
              .setValue(
                  new JobSubscriptionRecord()
                      .setClusterId("b")
                      .setTenantId("tenant")
                      .setVariables("<xml/>")));
      distributions.putTarget(10, "a", 1);
      distributions.markPending(10, "a", 1);
      if (completed) {
        distributions.acknowledge(10, "a", 1);
      }
      final var fixture = new Fixture(state);
      fixture.process(new ClusterConfigRecord().setClusterId("a"));
      verify(fixture.rejection)
          .appendRejection(
              eq(RejectionType.INVALID_STATE), contains("CLUSTER_HAS_JOB_SUBSCRIPTION_REFERENCES"));
      verifyNoInteractions(fixture.events);
      assertTrue(distributions.isTarget(10, "a"));
      assertEquals(2, state.getJobSubscriptionMirrorState().mirrors().size());
    }
  }

  @Test
  void clusterDeletionClearsMemberStateWithoutDeletingOtherScopesOrClusters() throws Exception {
    try (var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      final var members = state.getMemberChangeState();
      final var brokerScope = MemberChangeScope.BROKER;
      final var pdScope = MemberChangeScope.PLACEMENT_DRIVER;
      for (final var change :
          List.of(
              new MemberChangeRecord()
                  .setScope(brokerScope)
                  .setResourceId("a")
                  .setChangeId("change"),
              new MemberChangeRecord()
                  .setScope(brokerScope)
                  .setResourceId("a-other")
                  .setChangeId("change"),
              new MemberChangeRecord()
                  .setScope(pdScope)
                  .setResourceId("a")
                  .setChangeId("change"))) {
        members.put(change);
        members.setActive(change);
        members.putConfiguration(change);
      }
      store.getCurrentTransaction().commit();
      store.getCurrentTransaction();
      new BrokerClusterDeletedApplier(state)
          .applyState(1, new ClusterConfigRecord().setClusterId("a"));
      assertTrue(members.active(brokerScope, "a").isEmpty());
      assertTrue(members.configuration(brokerScope, "a").isEmpty());
      assertTrue(members.recentChanges(brokerScope, "a", 100).isEmpty());
      assertTrue(members.active(brokerScope, "a-other").isPresent());
      assertTrue(members.active(pdScope, "a").isPresent());
      store.getCurrentTransaction().rollback();
      assertTrue(members.active(brokerScope, "a").isPresent());
      store.getCurrentTransaction();
      new BrokerClusterDeletedApplier(state)
          .applyState(1, new ClusterConfigRecord().setClusterId("a"));
      store.getCurrentTransaction().commit();
    }
    try (var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      final var scope = MemberChangeScope.BROKER;
      assertTrue(state.getMemberChangeState().configuration(scope, "a").isEmpty());
      assertTrue(state.getMemberChangeState().get(scope, "a", "change").isEmpty());
      assertTrue(state.getMemberChangeState().configuration(scope, "a-other").isPresent());
    }
  }

  private static MutableProcessingState state(final PartitionStorage storage) {
    final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
    state.getBrokerClusterState().remember("a", 1);
    state.getBrokerClusterState().remember("b", 2);
    state
        .getJobSubscriptionMirrorState()
        .put(
            new JobSubscriptionMirrorRecord()
                .setMirrorKey(1)
                .setSourceClusterId("a")
                .setTargetClusterId("b"));
    state
        .getJobSubscriptionMirrorState()
        .put(
            new JobSubscriptionMirrorRecord()
                .setMirrorKey(2)
                .setSourceClusterId("b")
                .setTargetClusterId("a"));
    return state;
  }

  private static void registerBroker(final MutableProcessingState state) {
    state
        .getBrokerRegistrationState()
        .registerBroker(
            "a", "broker-0", "127.0.0.1", 26501, 26502, "test", 30_000, Instant.EPOCH, List.of());
  }

  private static final class Fixture {
    private final StateWriter events = mock(StateWriter.class);
    private final TypedRejectionWriter rejection = mock(TypedRejectionWriter.class);
    private final TypedResponseWriter response = mock(TypedResponseWriter.class);
    private final TypedRecordProcessorContext context = mock(TypedRecordProcessorContext.class);

    private Fixture(final ProcessingState state) {
      final var writers = mock(Writers.class);
      when(writers.state()).thenReturn(events);
      when(writers.rejection()).thenReturn(rejection);
      when(writers.response()).thenReturn(response);
      when(context.getWriters()).thenReturn(writers);
      when(context.getProcessingState(ProcessingState.class)).thenReturn(state);
    }

    @SuppressWarnings("unchecked")
    private TypedRecord<ClusterConfigRecord> process(final ClusterConfigRecord value) {
      final TypedRecord<ClusterConfigRecord> record = mock(TypedRecord.class);
      when(record.getKey()).thenReturn(1L);
      when(record.getValue()).thenReturn(value);
      new TopologyDeleteClusterProcessor(context).processRecord(record);
      return record;
    }
  }
}
