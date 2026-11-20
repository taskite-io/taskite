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
package io.taskite.placementdriver.processing.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.RegistrationPolicy;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.broker.registry.BrokerRegistryKey;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.processing.broker.registry.BrokerMarkOfflineProcessor;
import io.taskite.placementdriver.processing.broker.registry.BrokerRegisterProcessor;
import io.taskite.placementdriver.processing.broker.registry.BrokerRenewProcessor;
import io.taskite.placementdriver.processing.worker.registry.WorkerDeleteProcessor;
import io.taskite.placementdriver.processing.worker.registry.WorkerMarkOfflineProcessor;
import io.taskite.placementdriver.state.appliers.BrokerRegisteredApplier;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.BrokerIntent;
import io.taskite.protocol.record.intent.placementdriver.WorkerIntent;
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
import io.taskite.topology.PartitionStatus;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class RegistrationDeadlineProcessorTest {
  private static final Instant T0 = Instant.ofEpochMilli(1_000);
  private static final Duration TIMEOUT = Duration.ofSeconds(1);

  @TempDir Path directory;

  @Test
  void brokerRegisterRejectsActiveIdAndAcceptsOfflineId() {
    try (var storage = new PartitionStorage(directory.resolve("broker-register"))) {
      final var state = state(storage);
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 1, 1));
      state.getBrokerClusterState().remember("demo", 1);
      state
          .getBrokerRegistrationState()
          .registerBroker(
              "demo", "broker-0", "127.0.0.1", 26501, 26502, "1", 10_000, T0, List.of());
      final var writers = new CapturingWriters();
      final var processor = new BrokerRegisterProcessor(new TestContext(state, writers));
      final BrokerRegistrationRecord command = brokerRegistration();

      processor.processRecord(record(command, 2_000));
      assertEquals(RejectionType.ALREADY_EXISTS, writers.rejectionType);
      assertTrue(writers.rejectionReason.startsWith("BROKER_ID_ALREADY_REGISTERED:"));
      assertTrue(writers.rejectionReason.contains("broker 'broker-0' in cluster 'demo'"));
      assertTrue(writers.rejectionReason.contains("status 'ONLINE'"));

      state.getBrokerRegistrationState().markBrokerDraining("demo", "broker-0", T0);
      writers.reset();
      processor.processRecord(record(command, 2_000));
      assertEquals(RejectionType.ALREADY_EXISTS, writers.rejectionType);
      assertTrue(writers.rejectionReason.contains("status 'DRAINING'"));

      state.getBrokerRegistrationState().markBrokerOffline("demo", "broker-0", T0);
      writers.reset();
      processor.processRecord(record(command, 2_000));
      assertNull(writers.rejectionType);
      assertEquals(BrokerIntent.REGISTERED, writers.intent);
    }
  }

  @Test
  void sameStartupCanRetryRegisterAndOldStartupCannotRenew() {
    try (var storage = new PartitionStorage(directory.resolve("incarnation"))) {
      final var state = state(storage);
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 1, 1));
      state.getBrokerClusterState().remember("demo", 1);
      new BrokerRegisteredApplier(state, () -> T0)
          .applyState(1, brokerRegistration().setIncarnationId("startup-a"));
      final var writers = new CapturingWriters();
      final var context = new TestContext(state, writers);
      final var register = new BrokerRegisterProcessor(context);
      register.processRecord(record(brokerRegistration().setIncarnationId("startup-a"), 2_000));
      assertNull(writers.rejectionType);
      assertEquals(BrokerIntent.REGISTERED, writers.intent);

      writers.reset();
      register.processRecord(record(brokerRegistration().setIncarnationId("startup-b"), 2_000));
      assertEquals(RejectionType.ALREADY_EXISTS, writers.rejectionType);
      writers.reset();
      new BrokerRenewProcessor(context)
          .processRecord(record(brokerRegistration().setIncarnationId("startup-b"), 2_000));
      assertEquals(RejectionType.INVALID_STATE, writers.rejectionType);
      assertTrue(writers.rejectionReason.contains("STALE_BROKER_INCARNATION"));
      writers.reset();
      new BrokerRenewProcessor(context)
          .processRecord(record(brokerRegistration().setIncarnationId("startup-a"), 2_000));
      assertNull(writers.rejectionType);
      assertEquals(BrokerIntent.RENEWED, writers.intent);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void offlineMemberCanUpdateRaftEndpointWhileRetainingMembership(final boolean learner) {
    try (var storage = new PartitionStorage(directory.resolve("endpoint-" + learner))) {
      final var state = state(storage);
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 1, 1));
      state.getBrokerClusterState().remember("demo", 1);
      new BrokerRegisteredApplier(state, () -> T0)
          .applyState(1, brokerRegistration().setIncarnationId("startup-a"));
      state.getBrokerRegistrationState().markBrokerOffline("demo", "broker-0", T0);
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(1, List.of("broker-0"), "broker-0"),
                  PartitionTopology.Membership.initial(
                      learner ? List.of("other") : List.of("broker-0"),
                      learner ? List.of("broker-0") : List.of(),
                      learner ? "other" : "broker-0"),
                  PartitionStatus.ACTIVE,
                  Map.of()));
      final var writers = new CapturingWriters();
      final var processor = new BrokerRegisterProcessor(new TestContext(state, writers));
      processor.processRecord(
          record(brokerRegistration().setIncarnationId("startup-b").setRaftPort(26503), 2_000));
      assertNull(writers.rejectionType);
      assertEquals(BrokerIntent.REGISTERED, writers.intent);
      assertEquals(
          26502,
          state
              .getBrokerRegistrationState()
              .registration(new BrokerRegistryKey("demo", "broker-0"))
              .orElseThrow()
              .raftPort());
      writers.reset();
      processor.processRecord(record(brokerRegistration().setIncarnationId("startup-b"), 2_000));
      assertNull(writers.rejectionType);
      assertEquals(BrokerIntent.REGISTERED, writers.intent);
      state.getBrokerPartitionTopologyState().deletePartitionTopology("demo", 1);
      writers.reset();
      processor.processRecord(
          record(brokerRegistration().setIncarnationId("startup-b").setRaftPort(26503), 2_000));
      assertNull(writers.rejectionType);
      assertEquals(BrokerIntent.REGISTERED, writers.intent);
    }
  }

  @Test
  void pendingRaftOperationAllowsOfflineEndpointUpdate() {
    try (var storage = new PartitionStorage(directory.resolve("pending-endpoint"))) {
      final var state = state(storage);
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 1, 1));
      state.getBrokerClusterState().remember("demo", 1);
      new BrokerRegisteredApplier(state, () -> T0)
          .applyState(1, brokerRegistration().setIncarnationId("startup-a"));
      state.getBrokerRegistrationState().markBrokerOffline("demo", "broker-0", T0);
      final var plan =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "join", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(TopologyOperation.join(1, "broker-0"))),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              new ChangePlan.Deadline(1, Long.MAX_VALUE));
      state.getBrokerTopologyChangeState().putChangePlan(plan);
      state.getBrokerTopologyChangeState().markPending(plan);
      final var writers = new CapturingWriters();
      final var processor = new BrokerRegisterProcessor(new TestContext(state, writers));
      processor.processRecord(
          record(brokerRegistration().setIncarnationId("startup-b").setHost("2001:db8::1"), 2_000));
      assertNull(writers.rejectionType);
      assertEquals(BrokerIntent.REGISTERED, writers.intent);
      writers.reset();
      processor.processRecord(
          record(brokerRegistration().setIncarnationId("startup-b").setPort(26503), 2_000));
      assertNull(writers.rejectionType);
      assertEquals(BrokerIntent.REGISTERED, writers.intent);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void newStartupWaitsForDurableRemovalToFinish(final boolean childOnly) {
    try (var storage = new PartitionStorage(directory.resolve("removal-" + childOnly))) {
      final var state = state(storage);
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 1, 1));
      state.getBrokerClusterState().remember("demo", 1);
      new BrokerRegisteredApplier(state, () -> T0)
          .applyState(1, brokerRegistration().setIncarnationId("startup-a"));
      state.getBrokerRegistrationState().excludeFromPlacement("demo", "broker-0");
      state.getBrokerRegistrationState().removeBroker("demo", "broker-0");
      final var parent =
          new MemberChangeRecord()
              .setScope(MemberChangeScope.BROKER)
              .setResourceId("demo")
              .setChangeId("remove")
              .setRemovedMembers(List.of("broker-0"))
              .setStatus(MemberChangeStatus.PENDING);
      final var child =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "leave", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(TopologyOperation.leave(1, "broker-0"))),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              new ChangePlan.Deadline(1, Long.MAX_VALUE));
      if (childOnly) {
        state.getBrokerTopologyChangeState().putChangePlan(child);
        state.getBrokerTopologyChangeState().markPending(child);
      } else {
        state.getMemberChangeState().put(parent);
        state.getMemberChangeState().setActive(parent);
      }
      final var writers = new CapturingWriters();
      final var processor = new BrokerRegisterProcessor(new TestContext(state, writers));
      processor.processRecord(record(brokerRegistration().setIncarnationId("startup-b"), 2_000));
      assertEquals(RejectionType.INVALID_STATE, writers.rejectionType);
      assertEquals(
          "BROKER_REMOVAL_IN_PROGRESS: Expected to register broker 'broker-0' in cluster 'demo', but a pending topology or member change is removing it",
          writers.rejectionReason);
      if (childOnly) {
        state.getBrokerTopologyChangeState().removePending("demo", 1);
      } else {
        state.getMemberChangeState().clearActive(MemberChangeScope.BROKER, "demo");
      }
      writers.reset();
      processor.processRecord(record(brokerRegistration().setIncarnationId("startup-b"), 2_000));
      assertNull(writers.rejectionType);
      assertEquals(BrokerIntent.REGISTERED, writers.intent);
      writers.reset();
      processor.processRecord(record(brokerRegistration().setIncarnationId("startup-a"), 2_000));
      assertEquals(RejectionType.INVALID_STATE, writers.rejectionType);
    }
  }

  @Test
  void brokerRenewRequiresExistingNonOfflineRegistration() {
    try (var storage = new PartitionStorage(directory.resolve("broker-renew"))) {
      final var state = state(storage);
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 1, 1));
      state.getBrokerClusterState().remember("demo", 1);
      final var writers = new CapturingWriters();
      final var processor = new BrokerRenewProcessor(new TestContext(state, writers));
      final BrokerRegistrationRecord command =
          new BrokerRegistrationRecord().setClusterId("demo").setBrokerId("broker-0");

      processor.processRecord(record(command, 2_000));
      assertEquals(RejectionType.NOT_FOUND, writers.rejectionType);
      assertTrue(writers.rejectionReason.contains("NO_BROKER_REGISTERED"));

      state
          .getBrokerRegistrationState()
          .registerBroker(
              "demo", "broker-0", "127.0.0.1", 26501, 26502, "1", 10_000, T0, List.of());
      state.getBrokerRegistrationState().markBrokerOffline("demo", "broker-0", T0);
      writers.reset();
      processor.processRecord(record(command, 2_000));
      assertEquals(RejectionType.NOT_FOUND, writers.rejectionType);
      assertTrue(writers.rejectionReason.contains("NO_ONLINE_BROKER"));

      state.getBrokerRegistrationState().markBrokerDraining("demo", "broker-0", T0);
      writers.reset();
      processor.processRecord(record(command, 2_000));
      assertNull(writers.rejectionType);
      assertEquals(BrokerIntent.RENEWED, writers.intent);
    }
  }

  @Test
  void brokerOfflineProcessorRejectsEarlyCommandAndAcceptsExpiredLease() {
    try (var storage = new PartitionStorage(directory.resolve("broker"))) {
      final var state = state(storage);
      state
          .getBrokerRegistrationState()
          .registerBroker(
              "demo", "broker-0", "127.0.0.1", 26501, 26502, "1", 10_000, T0, List.of());
      final var writers = new CapturingWriters();
      final var processor =
          new BrokerMarkOfflineProcessor(new TestContext(state, writers), TIMEOUT);
      final var command =
          new BrokerRegistrationRecord()
              .setClusterId("demo")
              .setBrokerId("broker-0")
              .setExpectedLastRenewalEpochMillis(T0.toEpochMilli());

      processor.processRecord(record(command, 1_999));
      assertEquals(RejectionType.INVALID_STATE, writers.rejectionType);

      writers.reset();
      processor.processRecord(record(command, 2_000));
      assertNull(writers.rejectionType);
      assertEquals(BrokerIntent.MARKED_OFFLINE, writers.intent);
    }
  }

  @Test
  void workerOfflineAndDeleteProcessorsRejectCommandsBeforeTheirDeadlines() {
    try (var storage = new PartitionStorage(directory.resolve("worker"))) {
      final var state = state(storage);
      final WorkerRegistrationRecord heartbeat = worker();
      state.getWorkerRegistrationState().registerWorker(heartbeat, "tenant-a", T0);
      final var writers = new CapturingWriters();
      final var context = new TestContext(state, writers);
      final var offlineProcessor = new WorkerMarkOfflineProcessor(context, TIMEOUT);
      final WorkerRegistrationRecord offlineCommand =
          workerIdentity().setExpectedLastRenewalEpochMillis(T0.toEpochMilli());

      offlineProcessor.processRecord(record(offlineCommand, 1_999));
      assertEquals(RejectionType.INVALID_STATE, writers.rejectionType);

      writers.reset();
      offlineProcessor.processRecord(record(offlineCommand, 2_000));
      assertNull(writers.rejectionType);
      assertEquals(WorkerIntent.MARKED_OFFLINE, writers.intent);

      state
          .getWorkerRegistrationState()
          .markWorkerOffline(
              state.getWorkerRegistrationState().registrations().getFirst().key(),
              Instant.ofEpochMilli(2_000));
      final var deleteProcessor = new WorkerDeleteProcessor(context, TIMEOUT);
      final WorkerRegistrationRecord deleteCommand =
          workerIdentity().setExpectedStatusChangedAtEpochMillis(2_000);

      writers.reset();
      deleteProcessor.processRecord(record(deleteCommand, 2_999));
      assertEquals(RejectionType.INVALID_STATE, writers.rejectionType);

      writers.reset();
      deleteProcessor.processRecord(record(deleteCommand, 3_000));
      assertNull(writers.rejectionType);
      assertEquals(WorkerIntent.DELETED, writers.intent);
    }
  }

  private static MutableProcessingState state(final PartitionStorage storage) {
    return new MutableProcessingState(
        new StateStoreProcessingStore(storage),
        () -> 0,
        () -> 0,
        50,
        new RegistrationPolicy(TIMEOUT, TIMEOUT, TIMEOUT));
  }

  private static WorkerRegistrationRecord worker() {
    return workerIdentity()
        .setJobType("payment")
        .setMaxJobsActive(32)
        .setConcurrency(4)
        .setRenewalTimeoutMillis(10_000);
  }

  private static WorkerRegistrationRecord workerIdentity() {
    return new WorkerRegistrationRecord()
        .setClusterId("demo")
        .setTenantIds(List.of("tenant-a"))
        .setWorkerId("instance-1")
        .setName("payments");
  }

  private static BrokerRegistrationRecord brokerRegistration() {
    return new BrokerRegistrationRecord()
        .setClusterId("demo")
        .setBrokerId("broker-0")
        .setHost("127.0.0.1")
        .setPort(26501)
        .setRaftPort(26502)
        .setVersion("1")
        .setRenewalTimeoutMillis(10_000);
  }

  private static <T extends UnifiedRecordValue> TypedRecord<T> record(
      final T value, final long timestamp) {
    return new TypedRecord<>() {
      @Override
      public long getKey() {
        return 0;
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

      @Override
      public long getTimestamp() {
        return timestamp;
      }
    };
  }

  private record TestContext(MutableProcessingState state, CapturingWriters writers)
      implements TypedRecordProcessorContext {
    @Override
    public int getPartitionId() {
      return 1;
    }

    @Override
    public KeyGenerator getKeyGenerator() {
      return () -> 1;
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
    private Intent intent;
    private RejectionType rejectionType;
    private String rejectionReason;

    private void reset() {
      intent = null;
      rejectionType = null;
      rejectionReason = null;
    }

    @Override
    public TypedCommandWriter command() {
      return (key, commandIntent, commandValue) -> {};
    }

    @Override
    public StateWriter state() {
      return new StateWriter() {
        @Override
        public void appendFollowUpEvent(
            final long key, final Intent eventIntent, final UnifiedRecordValue eventValue) {
          intent = eventIntent;
        }

        @Override
        public void appendFollowUpEvent(
            final long key,
            final Intent eventIntent,
            final UnifiedRecordValue eventValue,
            final int recordVersion) {
          intent = eventIntent;
        }

        @Override
        public void appendFollowUpEvent(
            final long key,
            final ValueType valueType,
            final Intent eventIntent,
            final UnifiedRecordValue eventValue) {
          intent = eventIntent;
        }

        @Override
        public void appendFollowUpEvent(
            final long key,
            final ValueType valueType,
            final Intent eventIntent,
            final UnifiedRecordValue eventValue,
            final int recordVersion) {
          intent = eventIntent;
        }
      };
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
            final long key,
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
  }
}
