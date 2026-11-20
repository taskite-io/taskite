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
package io.taskite.placementdriver.processing.broker.mirror;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.state.appliers.BrokerClusterCreatedApplier;
import io.taskite.placementdriver.state.appliers.JobSubscriptionMirrorCreatedApplier;
import io.taskite.placementdriver.state.appliers.JobSubscriptionMirrorDeletedApplier;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.mirror.JobSubscriptionMirrorQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.mirror.JobSubscriptionMirrorRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionMirrorIntent;
import io.taskite.protocol.record.value.placementdriver.mirror.JobSubscriptionMirrorCreationOutcome;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.state.EventAppliers;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.PostCommitActionWriter;
import io.taskite.streamplatform.api.writer.StateWriter;
import io.taskite.streamplatform.api.writer.TypedCommandWriter;
import io.taskite.streamplatform.api.writer.TypedRejectionWriter;
import io.taskite.streamplatform.api.writer.TypedResponseWriter;
import io.taskite.streamplatform.api.writer.Writers;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class JobSubscriptionMirrorProcessorTest {
  @TempDir Path tempDir;

  @Test
  void createsAndDeduplicatesMirror() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("create"))) {
      final Fixture fixture = fixture(storage);
      registerCluster(fixture.state, "a");
      registerCluster(fixture.state, "b");

      fixture.create.processRecord(record(mirror("a", "b")));
      fixture.create.processRecord(record(mirror("a", "b")));

      assertFalse(fixture.writers.hasRejection());
      assertEquals(101, fixture.keys.get());
      assertEquals(1, fixture.state.getJobSubscriptionMirrorState().mirrors().size());
      assertEquals(
          101,
          fixture
              .state
              .getJobSubscriptionMirrorState()
              .mirror("a", "b")
              .orElseThrow()
              .getMirrorKey());
      assertEquals(
          JobSubscriptionMirrorCreationOutcome.EXISTS, fixture.writers.lastMirror.getOutcome());
    }
  }

  @Test
  void permitsReciprocalMirrors() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("reciprocal"))) {
      final Fixture fixture = fixture(storage);
      registerCluster(fixture.state, "a");
      registerCluster(fixture.state, "b");

      fixture.create.processRecord(record(mirror("a", "b")));
      fixture.create.processRecord(record(mirror("b", "a")));

      assertEquals(2, fixture.state.getJobSubscriptionMirrorState().mirrors().size());
      assertTrue(fixture.state.getJobSubscriptionMirrorState().mirror("a", "b").isPresent());
      assertTrue(fixture.state.getJobSubscriptionMirrorState().mirror("b", "a").isPresent());
    }
  }

  @Test
  void rejectsSelfMirrorAndUnknownTarget() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("invalid"))) {
      final Fixture fixture = fixture(storage);
      registerCluster(fixture.state, "a");

      fixture.create.processRecord(record(mirror("a", "a")));
      assertEquals(RejectionType.INVALID_ARGUMENT, fixture.writers.rejectionType);

      fixture.writers.resetRejection();
      fixture.create.processRecord(record(mirror("a", "missing")));
      assertEquals(RejectionType.NOT_FOUND, fixture.writers.rejectionType);
      assertTrue(fixture.writers.rejectionReason.startsWith("TARGET_CLUSTER_NOT_FOUND:"));
    }
  }

  @Test
  void queriesAndDeletesMirror() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("query-delete"))) {
      final Fixture fixture = fixture(storage);
      registerCluster(fixture.state, "a");
      registerCluster(fixture.state, "b");
      fixture.create.processRecord(record(mirror("a", "b")));

      fixture.query.processRecord(
          queryRecord(new JobSubscriptionMirrorQueryRecord().setMirrorKey(101)));
      assertEquals(1, fixture.writers.lastQuery.getMirrors().size());

      fixture.delete.processRecord(record(new JobSubscriptionMirrorRecord().setMirrorKey(101)));
      assertTrue(fixture.state.getJobSubscriptionMirrorState().mirrors().isEmpty());
    }
  }

  private static JobSubscriptionMirrorRecord mirror(
      final String sourceClusterId, final String targetClusterId) {
    return new JobSubscriptionMirrorRecord()
        .setSourceClusterId(sourceClusterId)
        .setTargetClusterId(targetClusterId)
        .setCreatedAtEpochMillis(1);
  }

  private static void registerCluster(final MutableProcessingState state, final String clusterId) {
    new BrokerClusterCreatedApplier(state)
        .applyState(
            1,
            new ClusterConfigRecord()
                .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                .setClusterId(clusterId)
                .setClusterSize(1)
                .setPartitionSize(1)
                .setReplicationFactor(1));
  }

  private static TypedRecord<JobSubscriptionMirrorRecord> record(
      final JobSubscriptionMirrorRecord value) {
    return typedRecord(value);
  }

  private static TypedRecord<JobSubscriptionMirrorQueryRecord> queryRecord(
      final JobSubscriptionMirrorQueryRecord value) {
    return typedRecord(value);
  }

  private static <T extends UnifiedRecordValue> TypedRecord<T> typedRecord(final T value) {
    return new TypedRecord<>() {
      @Override
      public long getKey() {
        return 1;
      }

      @Override
      public T getValue() {
        return value;
      }

      @Override
      public String getRequestStreamId() {
        return "stream";
      }

      @Override
      public String getRequestId() {
        return "request";
      }

      @Override
      public int getLength() {
        return value.getLength();
      }
    };
  }

  private static Fixture fixture(final PartitionStorage storage) {
    final MutableProcessingState state =
        new MutableProcessingState(new StateStoreProcessingStore(storage));
    final CapturingWriters writers = new CapturingWriters(state);
    final AtomicLong keys = new AtomicLong(100);
    final TestContext context = new TestContext(state, writers, keys::incrementAndGet);
    return new Fixture(
        state,
        writers,
        keys,
        new JobSubscriptionMirrorCreateProcessor(context),
        new JobSubscriptionMirrorDeleteProcessor(context),
        new JobSubscriptionMirrorQueryProcessor(context));
  }

  private record Fixture(
      MutableProcessingState state,
      CapturingWriters writers,
      AtomicLong keys,
      JobSubscriptionMirrorCreateProcessor create,
      JobSubscriptionMirrorDeleteProcessor delete,
      JobSubscriptionMirrorQueryProcessor query) {}

  private record TestContext(
      MutableProcessingState state, CapturingWriters writers, KeyGenerator keyGenerator)
      implements TypedRecordProcessorContext {
    @Override
    public int getPartitionId() {
      return 1;
    }

    @Override
    public KeyGenerator getKeyGenerator() {
      return keyGenerator;
    }

    @Override
    public Writers getWriters() {
      return writers;
    }

    @Override
    public Object getProcessingState() {
      return state;
    }

    @Override
    public EventAppliers getEventAppliers() {
      return null;
    }
  }

  private static final class CapturingWriters implements Writers {
    private final MutableProcessingState state;
    private final StateWriter stateWriter = new CapturingStateWriter();
    private RejectionType rejectionType;
    private String rejectionReason;
    private JobSubscriptionMirrorRecord lastMirror;
    private JobSubscriptionMirrorQueryRecord lastQuery;

    private CapturingWriters(final MutableProcessingState state) {
      this.state = state;
    }

    private boolean hasRejection() {
      return rejectionType != null;
    }

    private void resetRejection() {
      rejectionType = null;
      rejectionReason = null;
    }

    @Override
    public TypedCommandWriter command() {
      return (key, intent, value) -> {};
    }

    @Override
    public StateWriter state() {
      return stateWriter;
    }

    @Override
    public TypedRejectionWriter rejection() {
      return (type, reason) -> {
        rejectionType = type;
        rejectionReason = reason;
      };
    }

    @Override
    public TypedResponseWriter response() {
      return new TypedResponseWriter() {
        @Override
        public void writeEventOnCommand(
            final long eventKey,
            final Intent eventIntent,
            final UnifiedRecordValue eventValue,
            final TypedRecord<?> sourceCommand) {}

        @Override
        public void writeRejectionOnCommand(
            final TypedRecord<?> sourceCommand, final RejectionType type, final String reason) {}
      };
    }

    @Override
    public PostCommitActionWriter postCommit() {
      return action -> {};
    }

    private final class CapturingStateWriter implements StateWriter {
      @Override
      public void appendFollowUpEvent(
          final long key, final Intent intent, final UnifiedRecordValue value) {
        appendFollowUpEvent(key, intent, value, 1);
      }

      @Override
      public void appendFollowUpEvent(
          final long key,
          final Intent intent,
          final UnifiedRecordValue value,
          final int recordVersion) {
        if (intent == JobSubscriptionMirrorIntent.CREATED) {
          lastMirror = (JobSubscriptionMirrorRecord) value;
          new JobSubscriptionMirrorCreatedApplier(state).applyState(key, lastMirror);
        } else if (intent == JobSubscriptionMirrorIntent.DELETED) {
          lastMirror = (JobSubscriptionMirrorRecord) value;
          new JobSubscriptionMirrorDeletedApplier(state).applyState(key, lastMirror);
        } else if (intent == JobSubscriptionMirrorIntent.QUERIED) {
          lastQuery = (JobSubscriptionMirrorQueryRecord) value;
        }
      }

      @Override
      public void appendFollowUpEvent(
          final long key,
          final ValueType valueType,
          final Intent intent,
          final UnifiedRecordValue value) {
        appendFollowUpEvent(key, intent, value);
      }

      @Override
      public void appendFollowUpEvent(
          final long key,
          final ValueType valueType,
          final Intent intent,
          final UnifiedRecordValue value,
          final int recordVersion) {
        appendFollowUpEvent(key, intent, value, recordVersion);
      }
    }
  }
}
