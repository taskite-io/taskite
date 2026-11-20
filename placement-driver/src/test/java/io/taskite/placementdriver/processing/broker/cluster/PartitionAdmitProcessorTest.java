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

import static org.junit.jupiter.api.Assertions.*;

import io.taskite.placementdriver.broker.cluster.*;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.appliers.*;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.record.*;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.state.*;
import io.taskite.streamplatform.api.writer.*;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class PartitionAdmitProcessorTest {
  @Test
  void serializedAdmissionRechecksCommittedReservationsAndRejectsStaleCommands(
      @TempDir Path directory) {
    try (var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 2, 1));
      state
          .getBrokerRegistrationState()
          .registerBroker(
              "demo", "b1", "127.0.0.1", 26501, 26502, "test", 30000, Instant.EPOCH, List.of());
      final var first = plan(1);
      final var second = plan(2);
      for (var plan : List.of(first, second)) {
        new PartitionChangeSubmittedApplier(state)
            .applyState(1, BrokerPartitionChangeRecordMapper.fromPlan(plan));
      }
      final var admitted = process(state, first);
      assertNull(admitted.reason);
      assertEquals(BrokerPartitionChangeIntent.ADMITTED, admitted.intent);
      new PartitionChangeAdmittedApplier(state).applyState(2, admitted.event);
      assertEquals(
          "MIGRATION_BUDGET_EXHAUSTED: Expected to admit topology change 'p2' in cluster 'demo' for partition '2', but concurrent migrations have exhausted the migration budget",
          process(state, second).reason);
      assertEquals(
          "CHANGE_NOT_QUEUED: Expected to admit topology change 'p1' in cluster 'demo' at operation index '0' and generation '0', but no matching owned QUEUED plan was found",
          process(state, first).reason);
      assertEquals(
          BrokerPartitionChangeStatus.QUEUED,
          state.getBrokerTopologyChangeState().changePlan("demo", "p2").orElseThrow().status());
    }
  }

  private static ChangePlan plan(int id) {
    return new ChangePlan(
        new ChangePlan.Definition(
            new ChangePlan.Identity("demo", "p" + id, id),
            new ChangePlan.Versions(1, 2),
            List.of(TopologyOperation.join(id, "b2"))),
        new ChangePlan.Progress(0, BrokerPartitionChangeStatus.QUEUED),
        new ChangePlan.Deadline(1, 1001));
  }

  private static Capture process(MutableProcessingState state, ChangePlan plan) {
    final var capture = new Capture();
    final var value = BrokerPartitionChangeRecordMapper.fromPlan(plan);
    new PartitionAdmitProcessor(new Context(state, capture))
        .processRecord(
            new TypedRecord<>() {
              public long getKey() {
                return 1;
              }

              public PartitionChangeRecord getValue() {
                return value;
              }

              public String getRequestStreamId() {
                return "";
              }

              public String getRequestId() {
                return "";
              }

              public int getLength() {
                return value.getLength();
              }
            });
    return capture;
  }

  private record Context(MutableProcessingState state, Capture capture)
      implements TypedRecordProcessorContext {
    public long getProcessingTime() {
      return 100;
    }

    public int getPartitionId() {
      return 1;
    }

    public KeyGenerator getKeyGenerator() {
      return () -> 1;
    }

    public Writers getWriters() {
      return capture;
    }

    public Object getProcessingState() {
      return state;
    }

    public EventAppliers getEventAppliers() {
      return PlacementDriverEventAppliers.create(state);
    }
  }

  private static final class Capture implements Writers {
    private String reason;
    private Intent intent;
    private PartitionChangeRecord event;

    public TypedCommandWriter command() {
      return (key, intent, value) -> {};
    }

    public StateWriter state() {
      return new StateWriter() {
        public void appendFollowUpEvent(long key, Intent intent, UnifiedRecordValue value) {
          Capture.this.intent = intent;
          event = (PartitionChangeRecord) value;
        }

        public void appendFollowUpEvent(
            long key, Intent intent, UnifiedRecordValue value, int version) {
          appendFollowUpEvent(key, intent, value);
        }

        public void appendFollowUpEvent(
            long key, ValueType type, Intent intent, UnifiedRecordValue value) {
          appendFollowUpEvent(key, intent, value);
        }

        public void appendFollowUpEvent(
            long key, ValueType type, Intent intent, UnifiedRecordValue value, int version) {
          appendFollowUpEvent(key, intent, value);
        }
      };
    }

    public TypedRejectionWriter rejection() {
      return (type, message) -> reason = message;
    }

    public TypedResponseWriter response() {
      return new TypedResponseWriter() {
        public void writeEventOnCommand(
            long key, Intent intent, UnifiedRecordValue value, TypedRecord<?> command) {}

        public void writeRejectionOnCommand(
            TypedRecord<?> command, RejectionType type, String reason) {}
      };
    }

    public PostCommitActionWriter postCommit() {
      return action -> {};
    }
  }
}
