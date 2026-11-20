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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.taskite.placementdriver.state.appliers.BrokerClusterCreatedApplier;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.protocol.record.value.placementdriver.topology.ClusterConfigurationOutcome;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.StateWriter;
import io.taskite.streamplatform.api.writer.TypedRejectionWriter;
import io.taskite.streamplatform.api.writer.TypedResponseWriter;
import io.taskite.streamplatform.api.writer.Writers;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

final class TopologyCreateClusterProcessorTest {
  @Test
  @SuppressWarnings("unchecked")
  void allocatesSixteenNamespacesAndRejectsNewOnesAfterDeletion(@TempDir final Path directory)
      throws Exception {
    try (var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      final var writers = mock(Writers.class);
      final var events = mock(StateWriter.class);
      final var rejections = mock(TypedRejectionWriter.class);
      final var responses = mock(TypedResponseWriter.class);
      when(writers.state()).thenReturn(events);
      when(writers.rejection()).thenReturn(rejections);
      when(writers.response()).thenReturn(responses);
      final var context = mock(TypedRecordProcessorContext.class);
      when(context.getWriters()).thenReturn(writers);
      when(context.getProcessingState(ProcessingState.class)).thenReturn(state);
      final var processor = new TopologyCreateClusterProcessor(context);
      final var applier = new BrokerClusterCreatedApplier(state);
      doAnswer(
              call -> {
                applier.applyState(call.getArgument(0), call.getArgument(2));
                return null;
              })
          .when(events)
          .appendFollowUpEvent(anyLong(), eq(TopologyIntent.CLUSTER_CREATED), any());
      for (int key = 1; key < Protocol.MAXIMUM_CLUSTERS; key++) {
        final TypedRecord<ClusterConfigRecord> command = mock(TypedRecord.class);
        when(command.getValue()).thenReturn(configuration("cluster-" + key, 1));
        processor.processRecord(command);
        assertEquals(key, state.getBrokerClusterState().clusterKey("cluster-" + key));
        store.getCurrentTransaction().commit();
      }
      final TypedRecord<ClusterConfigRecord> retry = mock(TypedRecord.class);
      when(retry.getValue()).thenReturn(configuration("cluster-1", 1));
      processor.processRecord(retry);
      assertEquals(1, retry.getValue().getClusterKey());
      verify(events, times(15))
          .appendFollowUpEvent(anyLong(), eq(TopologyIntent.CLUSTER_CREATED), any());
      when(retry.getValue()).thenReturn(configuration("cluster-1", 2));
      processor.processRecord(retry);
      verify(rejections)
          .appendRejection(eq(RejectionType.ALREADY_EXISTS), contains("CLUSTER_CONFIG_CONFLICT"));
      state.getBrokerClusterState().delete("cluster-1");
      state.getBrokerClusterConfigState().delete("cluster-1");
      store.getCurrentTransaction().commit();
      when(retry.getValue()).thenReturn(configuration("cluster-1", 1));
      processor.processRecord(retry);
      verify(rejections)
          .appendRejection(eq(RejectionType.INVALID_STATE), contains("CLUSTER_LIMIT_EXCEEDED"));
      assertEquals(15, state.getBrokerClusterState().latestClusterKey());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void partitionLimitComesFromProtocol(@TempDir final Path directory) {
    try (var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      final var writers = mock(Writers.class);
      final var events = mock(StateWriter.class);
      final var rejections = mock(TypedRejectionWriter.class);
      when(writers.state()).thenReturn(events);
      when(writers.rejection()).thenReturn(rejections);
      when(writers.response()).thenReturn(mock(TypedResponseWriter.class));
      final var context = mock(TypedRecordProcessorContext.class);
      when(context.getWriters()).thenReturn(writers);
      when(context.getProcessingState(ProcessingState.class)).thenReturn(state);
      final var processor = new TopologyCreateClusterProcessor(context);
      final TypedRecord<ClusterConfigRecord> command = mock(TypedRecord.class);
      when(command.getValue()).thenReturn(configuration("demo", Protocol.MAXIMUM_PARTITIONS));
      processor.processRecord(command);
      verify(events).appendFollowUpEvent(anyLong(), eq(TopologyIntent.CLUSTER_CREATED), any());
      when(command.getValue()).thenReturn(configuration("demo", Protocol.MAXIMUM_PARTITIONS + 1));
      processor.processRecord(command);
      verify(rejections).appendRejection(eq(RejectionType.INVALID_ARGUMENT), any());
      assertEquals(
          Protocol.PLACEMENT_DRIVER_CLUSTER_KEY, state.getBrokerClusterState().latestClusterKey());
    }
  }

  private static ClusterConfigRecord configuration(final String clusterId, final int partitions) {
    return new ClusterConfigRecord()
        .setClusterId(clusterId)
        .setClusterSize(1)
        .setPartitionSize(partitions)
        .setReplicationFactor(1);
  }

  @Test
  @SuppressWarnings("unchecked")
  void repeatedCreationRespondsWithoutOverwritingManagedRoster(@TempDir final Path directory) {
    try (var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      state.getBrokerClusterState().remember("demo", 1);
      final var config = new BrokerClusterConfig(3, 1, 3, List.of("b", "c", "d"), 42);
      state.getBrokerClusterConfigState().putClusterConfig("demo", config);
      final var writers = mock(Writers.class);
      final var events = mock(StateWriter.class);
      final var response = mock(TypedResponseWriter.class);
      when(writers.state()).thenReturn(events);
      when(writers.response()).thenReturn(response);
      final var context = mock(TypedRecordProcessorContext.class);
      when(context.getWriters()).thenReturn(writers);
      when(context.getProcessingState(ProcessingState.class)).thenReturn(state);
      final TypedRecord<ClusterConfigRecord> command = mock(TypedRecord.class);
      when(command.getValue())
          .thenReturn(
              new ClusterConfigRecord()
                  .setClusterId("demo")
                  .setClusterSize(3)
                  .setPartitionSize(1)
                  .setReplicationFactor(3));
      new TopologyCreateClusterProcessor(context).processRecord(command);
      verifyNoInteractions(events);
      final var capture = ArgumentCaptor.forClass(ClusterConfigRecord.class);
      verify(response)
          .writeEventOnCommand(
              eq(0L), eq(TopologyIntent.CLUSTER_CREATED), capture.capture(), eq(command));
      assertEquals(ClusterConfigurationOutcome.EXISTS, capture.getValue().getOutcome());
      assertEquals(config, state.getBrokerClusterConfigState().clusterConfig("demo"));
      // Replay the legacy EXISTS event that older leaders persisted.
      new BrokerClusterCreatedApplier(state).applyState(1, capture.getValue());
      assertEquals(config, state.getBrokerClusterConfigState().clusterConfig("demo"));
    }
  }
}
