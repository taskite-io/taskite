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
package io.taskite.placementdriver.processing.broker.distribution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.broker.distribution.CommandDistributionDedupe;
import io.taskite.placementdriver.state.appliers.BrokerClusterCreatedApplier;
import io.taskite.placementdriver.state.appliers.CommandDistributionAcknowledgedApplier;
import io.taskite.placementdriver.state.appliers.CommandDistributionCompletedApplier;
import io.taskite.placementdriver.state.appliers.CommandDistributionRedistributedApplier;
import io.taskite.placementdriver.state.appliers.CommandDistributionStartedApplier;
import io.taskite.placementdriver.state.appliers.PlacementDriverEventAppliers;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionListRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRedistributeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRequestRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.mirror.JobSubscriptionMirrorRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.common.TimerIntent;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.protocol.record.value.placementdriver.commanddistribution.JobSubscriptionCreationType;
import io.taskite.protocol.record.value.placementdriver.commanddistribution.JobSubscriptionDistributionStatus;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;
import io.taskite.streamplatform.api.state.EventAppliers;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.PostCommitActionWriter;
import io.taskite.streamplatform.api.writer.StateWriter;
import io.taskite.streamplatform.api.writer.TypedCommandWriter;
import io.taskite.streamplatform.api.writer.TypedRejectionWriter;
import io.taskite.streamplatform.api.writer.TypedResponseWriter;
import io.taskite.streamplatform.api.writer.Writers;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CommandDistributionStartProcessorTest {
  private static final String VALID_VARIABLES = "{\"amount\":42}";

  @TempDir Path tempDir;

  @Test
  void ordinarySubscriptionCreationAndDeletionNeverCreateTimer() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("timer-deletion"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, "demo", 1);
      final var request = subscription("timer", VALID_VARIABLES, "timer");
      fixture.processor.processRecord(record(request));
      fixture.state.getCommandDistributionState().acknowledge(101, "demo", 1);
      complete(fixture, 101, 0);
      assertTrue(fixture.state.getTimerSubscriptionState().subscriptions().isEmpty());
      fixture.processor.processRecord(record(deletion(101)));
      assertTrue(fixture.state.getTimerSubscriptionState().subscriptions().isEmpty());
      assertEquals(0, fixture.writers.cancelledTimerEventCount);
    }
  }

  @Test
  void ordinarySubscriptionCompletionDoesNotScheduleTimer() {
    try (final PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("timer-validation"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, "demo", 1);
      final var request = subscription("timer", VALID_VARIABLES, "invalid");
      fixture.processor.processRecord(record(request));
      assertFalse(fixture.writers.hasRejection());
      assertEquals(1, fixture.state.getCommandDistributionState().distributions().size());
      assertTrue(fixture.state.getTimerSubscriptionState().subscriptions().isEmpty());
      fixture.state.getCommandDistributionState().acknowledge(101, "demo", 1);
      complete(fixture, 101, 0);
      assertTrue(fixture.state.getTimerSubscriptionState().subscriptions().isEmpty());
    }
  }

  @Test
  void acknowledgementsCompletePlanBeforeCompletionEvent() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("completion"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, "demo", 1);
      registerTopology(fixture.state, "backup", 1);
      fixture
          .state
          .getJobSubscriptionMirrorState()
          .put(
              new JobSubscriptionMirrorRecord()
                  .setMirrorKey(7)
                  .setSourceClusterId("demo")
                  .setTargetClusterId("backup"));
      fixture.processor.processRecord(record(subscription("a", VALID_VARIABLES, "a")));
      fixture.state.getCommandDistributionState().acknowledge(101, "demo", 1);
      complete(fixture, 101, 0);
      assertEquals(RejectionType.INVALID_STATE, fixture.writers.rejectionType);
      assertEquals(0, fixture.writers.completedEventCount);
      assertFalse(fixture.state.getCommandDistributionState().isPlanComplete(101));
      fixture.state.getCommandDistributionState().acknowledge(101, "backup", 1);
      assertTrue(fixture.state.getCommandDistributionState().isPlanComplete(101));
      assertEquals(
          JobSubscriptionDistributionStatus.COMPLETED,
          JobSubscriptionDistributionDetails.view(
                  fixture.state,
                  fixture.state.getCommandDistributionState().distribution(101).orElseThrow(),
                  false)
              .getStatus());
      complete(fixture, 101, 0);
      assertEquals(1, fixture.writers.completedEventCount);
      complete(fixture, 101, 0);
      assertEquals(2, fixture.writers.completedEventCount);
      assertTrue(fixture.state.getCommandDistributionState().isPlanComplete(101));
      assertEquals(0, fixture.writers.completionResponseCount);
    }
  }

  @Test
  void completionRejectsMissingPlansEmptyTargetsAndStaleGenerations() {
    try (final PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("invalid-completion"))) {
      final Fixture fixture = fixture(storage);
      complete(fixture, 0, 0);
      assertEquals(RejectionType.INVALID_ARGUMENT, fixture.writers.rejectionType);
      complete(fixture, 101, -1);
      assertEquals(RejectionType.INVALID_ARGUMENT, fixture.writers.rejectionType);
      complete(fixture, 101, 0);
      assertEquals(RejectionType.NOT_FOUND, fixture.writers.rejectionType);
      fixture
          .state
          .getCommandDistributionState()
          .putDistribution(
              new CommandDistributionRecord<>().setDistributionKey(101).setGeneration(1));
      complete(fixture, 101, 1);
      assertEquals(RejectionType.INVALID_STATE, fixture.writers.rejectionType);
      fixture.state.getCommandDistributionState().putTarget(101, "demo", 1);
      fixture.state.getCommandDistributionState().markPending(101, "demo", 1);
      complete(fixture, 101, 0);
      assertEquals(RejectionType.INVALID_STATE, fixture.writers.rejectionType);
      new CommandDistributionCompletedApplier()
          .applyState(
              101, new CommandDistributionRecord<>().setDistributionKey(101).setGeneration(0));
      assertFalse(fixture.state.getCommandDistributionState().isPlanComplete(101));
      assertEquals(0, fixture.writers.completedEventCount);
    }
  }

  @Test
  void acknowledgedCompletionSurvivesRestartAndExpansionNeedsOnlyNewAck() {
    final Path path = tempDir.resolve("persisted-completion");
    try (final PartitionStorage storage = new PartitionStorage(path)) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, 1);
      fixture.processor.processRecord(record(subscription("a", VALID_VARIABLES, "a")));
      fixture.state.getCommandDistributionState().acknowledge(101, "demo", 1);
      assertTrue(fixture.state.getCommandDistributionState().isPlanComplete(101));
    }
    try (final PartitionStorage storage = new PartitionStorage(path)) {
      final Fixture fixture = fixture(storage);
      assertTrue(fixture.state.getCommandDistributionState().isPlanComplete(101));
      fixture.state.getCommandDistributionState().putTarget(101, "demo", 2);
      fixture.state.getCommandDistributionState().markPending(101, "demo", 2);
      assertFalse(fixture.state.getCommandDistributionState().isPlanComplete(101));
      new CommandDistributionCompletedApplier()
          .applyState(101, new CommandDistributionRecord<>().setDistributionKey(101));
      assertFalse(fixture.state.getCommandDistributionState().isPlanComplete(101));
      fixture.state.getCommandDistributionState().acknowledge(101, "demo", 2);
      assertTrue(fixture.state.getCommandDistributionState().isPlanComplete(101));
      complete(fixture, 101, 0);
      assertTrue(fixture.state.getCommandDistributionState().isPlanComplete(101));
      assertEquals(1, fixture.writers.completedEventCount);
    }
  }

  @Test
  void redistributionResetsAcknowledgementsAndFencesOldCompletionCommands() {
    try (final PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("completion-generation"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, 1);
      fixture.processor.processRecord(record(subscription("a", VALID_VARIABLES, "a")));
      fixture.state.getCommandDistributionState().acknowledge(101, "demo", 1);
      complete(fixture, 101, 0);
      final var context =
          new TestContext(fixture.state, fixture.writers, fixture.keyGenerator::incrementAndGet);
      new CommandDistributionRedistributeProcessor(context)
          .processRecord(typed(redistribute(101, 0)));
      assertFalse(fixture.state.getCommandDistributionState().isPlanComplete(101));
      complete(fixture, 101, 0);
      assertEquals(RejectionType.INVALID_STATE, fixture.writers.rejectionType);
      fixture.state.getCommandDistributionState().acknowledge(101, "demo", 1);
      assertTrue(fixture.state.getCommandDistributionState().isPlanComplete(101));
      complete(fixture, 101, 1);
      assertTrue(fixture.state.getCommandDistributionState().isPlanComplete(101));
      assertEquals(2, fixture.writers.completedEventCount);
    }
  }

  @Test
  void deletionBecomesDeletedAfterAllAcksWithoutCompletionEvent() {
    try (final PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("deletion-completion"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, 1);
      fixture.processor.processRecord(record(subscription("a", VALID_VARIABLES, "a")));
      fixture.state.getCommandDistributionState().acknowledge(101, "demo", 1);
      complete(fixture, 101, 0);
      fixture.processor.processRecord(record(deletion(101)));
      final var distribution =
          fixture.state.getCommandDistributionState().distribution(101).orElseThrow();
      assertEquals(
          JobSubscriptionDistributionStatus.DELETING,
          JobSubscriptionDistributionDetails.view(fixture.state, distribution, false).getStatus());
      fixture.state.getCommandDistributionState().acknowledge(102, "demo", 1);
      assertEquals(
          JobSubscriptionDistributionStatus.DELETED,
          JobSubscriptionDistributionDetails.view(fixture.state, distribution, false).getStatus());
      complete(fixture, 102, 0);
      assertEquals(
          JobSubscriptionDistributionStatus.DELETED,
          JobSubscriptionDistributionDetails.view(fixture.state, distribution, false).getStatus());
    }
  }

  private static void complete(final Fixture fixture, final long key, final long generation) {
    final var processors =
        CommandDistributionProcessors.registerCommandDistributionProcessors(
            TypedRecordProcessors.processors(),
            new TestContext(fixture.state, fixture.writers, fixture.keyGenerator::incrementAndGet));
    ((CommandDistributionCompleteProcessor)
            processors.get(ValueType.COMMAND_DISTRIBUTION, CommandDistributionIntent.COMPLETE, 1))
        .processRecord(
            acknowledgementRecord(
                new CommandDistributionRecord<>()
                    .setDistributionKey(key)
                    .setGeneration(generation)));
  }

  @Test
  void resourceListCombinesExactFiltersAndSeparatesClusterAssetsWithoutResourceContent() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("resource-list"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, "demo", 2);
      registerTopology(fixture.state, "backup", 3);
      fixture
          .state
          .getJobSubscriptionMirrorState()
          .put(
              new JobSubscriptionMirrorRecord()
                  .setMirrorKey(7)
                  .setSourceClusterId("demo")
                  .setTargetClusterId("backup"));
      fixture.processor.processRecord(record(subscription("a", VALID_VARIABLES, "a")));
      fixture.processor.processRecord(record(subscription("b", VALID_VARIABLES, "b")));
      final var context =
          new TestContext(fixture.state, fixture.writers, fixture.keyGenerator::incrementAndGet);
      final var list = new CommandDistributionListProcessor(context);
      list.processRecord(typed(new CommandDistributionListRecord().setClusterId("demo")));
      assertEquals(2, fixture.writers.lastResourceList.getTotal());
      assertEquals(
          102, fixture.writers.lastResourceList.getSubscriptions().getFirst().getDistributionKey());
      assertEquals(
          null, fixture.writers.lastResourceList.getSubscriptions().getFirst().getSubscription());
      assertEquals(2, fixture.writers.lastResourceList.getSubscriptions().size());
      assertEquals(
          JobSubscriptionCreationType.SUBSCRIPTION,
          fixture.writers.lastResourceList.getSubscriptions().getFirst().getCreationType());
      list.processRecord(typed(new CommandDistributionListRecord()));
      assertEquals(4, fixture.writers.lastResourceList.getTotal());
      fixture.state.getCommandDistributionState().acknowledge(101, "demo", 1);
      fixture.state.getCommandDistributionState().acknowledge(101, "demo", 2);
      list.processRecord(
          typed(
              new CommandDistributionListRecord()
                  .setDistributionKey(101)
                  .setJobType("a")
                  .setClusterId("demo")));
      assertEquals(1, fixture.writers.lastResourceList.getTotal());
      assertEquals(
          JobSubscriptionDistributionStatus.DISTRIBUTING,
          fixture.writers.lastResourceList.getSubscriptions().getFirst().getStatus());
      assertEquals(
          1, fixture.writers.lastResourceList.getSubscriptions().getFirst().getTargets().size());
      list.processRecord(typed(new CommandDistributionListRecord().setJobType("pro")));
      assertEquals(0, fixture.writers.lastResourceList.getTotal());
      list.processRecord(typed(new CommandDistributionListRecord().setClusterId("backup")));
      assertEquals(2, fixture.writers.lastResourceList.getTotal());
      final var mirrored = fixture.writers.lastResourceList.getSubscriptions().getFirst();
      assertEquals("backup", mirrored.getClusterId());
      assertEquals("demo", mirrored.getOriginClusterId());
      assertEquals(JobSubscriptionCreationType.MIRROR, mirrored.getCreationType());
      assertEquals(JobSubscriptionDistributionStatus.DISTRIBUTING, mirrored.getStatus());
      assertEquals(1, mirrored.getTargets().size());
      assertEquals(3, mirrored.getTargets().getFirst().getExpectedPartitions());
      list.processRecord(
          typed(
              new CommandDistributionListRecord().setClusterId("backup").setDistributionKey(101)));
      assertEquals(1, fixture.writers.lastResourceList.getTotal());
      assertEquals(
          JobSubscriptionDistributionStatus.DISTRIBUTING,
          fixture.writers.lastResourceList.getSubscriptions().getFirst().getStatus());
      assertEquals(
          0,
          fixture
              .writers
              .lastResourceList
              .getSubscriptions()
              .getFirst()
              .getTargets()
              .getFirst()
              .getAcknowledgedPartitions());
      list.processRecord(typed(new CommandDistributionListRecord().setClusterId("missing")));
      assertEquals(0, fixture.writers.lastResourceList.getTotal());
      list.processRecord(typed(new CommandDistributionListRecord().setDistributionKey(-1)));
      assertEquals(RejectionType.INVALID_ARGUMENT, fixture.writers.rejectionType);
    }
  }

  @Test
  void deletedAssetsRemainVisibleButDeletionCommandsAreNotAssets() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("deleted-asset"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, 1);
      fixture.processor.processRecord(record(subscription("a", VALID_VARIABLES, "a")));
      fixture.processor.processRecord(
          record(
              new CommandDistributionRequestRecord<JobSubscriptionDeletionRecord>()
                  .setValueType(ValueType.JOB_SUBSCRIPTION_DELETION)
                  .setIntent(JobSubscriptionIntent.DELETE)
                  .setValue(
                      new JobSubscriptionDeletionRecord()
                          .setClusterId("demo")
                          .setTenantId("tenant")
                          .setSubscriptionKey(101))));
      final var context =
          new TestContext(fixture.state, fixture.writers, fixture.keyGenerator::incrementAndGet);
      new CommandDistributionListProcessor(context)
          .processRecord(typed(new CommandDistributionListRecord()));
      assertEquals(1, fixture.writers.lastResourceList.getTotal());
      assertEquals(
          JobSubscriptionDistributionStatus.DELETING,
          fixture.writers.lastResourceList.getSubscriptions().getFirst().getStatus());
      assertEquals(
          102,
          fixture
              .writers
              .lastResourceList
              .getSubscriptions()
              .getFirst()
              .getDeletionDistributionKey());
      fixture.state.getCommandDistributionState().acknowledge(102, "demo", 1);
      complete(fixture, 102, 0);
      new CommandDistributionListProcessor(context)
          .processRecord(typed(new CommandDistributionListRecord()));
      assertEquals(
          JobSubscriptionDistributionStatus.DELETED,
          fixture.writers.lastResourceList.getSubscriptions().getFirst().getStatus());
      new CommandDistributionRedistributeProcessor(context)
          .processRecord(typed(redistribute(101, 0)));
      assertEquals(RejectionType.INVALID_STATE, fixture.writers.rejectionType);
      assertEquals(
          0,
          fixture
              .state
              .getCommandDistributionState()
              .distribution(101)
              .orElseThrow()
              .getGeneration());
    }
  }

  @Test
  void redistributingRetainsKeyAndTargetsResetsAcksAndSurvivesReopen() {
    final Path path = tempDir.resolve("redistribute-recovery");
    try (final PartitionStorage storage = new PartitionStorage(path)) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, "demo", 2);
      registerTopology(fixture.state, "backup", 3);
      fixture
          .state
          .getJobSubscriptionMirrorState()
          .put(
              new JobSubscriptionMirrorRecord()
                  .setMirrorKey(7)
                  .setSourceClusterId("demo")
                  .setTargetClusterId("backup"));
      fixture.processor.processRecord(record(subscription("a", VALID_VARIABLES, "a")));
      fixture.state.getCommandDistributionState().acknowledge(101, "demo", 1);
      fixture.state.getCommandDistributionState().acknowledge(101, "backup", 1);
      fixture.state.getJobSubscriptionMirrorState().delete(7);
      registerTopology(fixture.state, "backup", 1);
      new CommandDistributionRedistributeProcessor(
              new TestContext(
                  fixture.state, fixture.writers, fixture.keyGenerator::incrementAndGet))
          .processRecord(typed(redistribute(101, 0)));
      assertFalse(fixture.writers.hasRejection());
      assertEquals(
          List.of(1, 2),
          fixture.state.getCommandDistributionState().pendingPartitions(101, "demo"));
      assertEquals(
          List.of(1), fixture.state.getCommandDistributionState().pendingPartitions(101, "backup"));
      assertEquals(
          1,
          fixture
              .state
              .getCommandDistributionState()
              .distribution(101)
              .orElseThrow()
              .getGeneration());
      assertEquals(100, fixture.keyGenerator.get() - 1);
      // Ordinary deployment retry keeps dedupe semantics and the new generation.
      fixture.processor.processRecord(record(subscription("a", VALID_VARIABLES, "a")));
      assertEquals(
          1,
          fixture
              .state
              .getCommandDistributionState()
              .distribution(101)
              .orElseThrow()
              .getGeneration());
      assertEquals(
          List.of(1, 2),
          fixture.state.getCommandDistributionState().pendingPartitions(101, "demo"));
    }
    try (final PartitionStorage storage = new PartitionStorage(path)) {
      final Fixture recovered = fixture(storage);
      final var distribution =
          recovered.state.getCommandDistributionState().distribution(101).orElseThrow();
      assertEquals(1, distribution.getGeneration());
      assertEquals(1793400000000L, distribution.getLastRedistributedAt());
      assertEquals(
          VALID_VARIABLES, ((JobSubscriptionRecord) distribution.getValue()).getVariables());
      assertEquals(
          List.of(1, 2),
          recovered.state.getCommandDistributionState().pendingPartitions(101, "demo"));
      assertEquals(
          List.of(1),
          recovered.state.getCommandDistributionState().pendingPartitions(101, "backup"));
    }
  }

  @Test
  void concurrentRedistributeAndStaleAckCannotClearNewGeneration() {
    try (final PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("generation-race"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, 1);
      fixture.processor.processRecord(record(subscription("a", VALID_VARIABLES, "a")));
      final var context =
          new TestContext(fixture.state, fixture.writers, fixture.keyGenerator::incrementAndGet);
      final var processor = new CommandDistributionRedistributeProcessor(context);
      processor.processRecord(typed(redistribute(101, 0)));
      processor.processRecord(typed(redistribute(101, 0)));
      assertEquals(RejectionType.INVALID_STATE, fixture.writers.rejectionType);
      assertEquals(
          1,
          fixture
              .state
              .getCommandDistributionState()
              .distribution(101)
              .orElseThrow()
              .getGeneration());
      final var oldAck = acknowledgement(101, "demo", 1);
      new CommandDistributionAcknowledgeProcessor(context)
          .processRecord(acknowledgementRecord(oldAck));
      assertEquals(RejectionType.INVALID_STATE, fixture.writers.rejectionType);
      new CommandDistributionAcknowledgedApplier(fixture.state).applyState(101, oldAck);
      assertTrue(fixture.state.getCommandDistributionState().isPending(101, "demo", 1));
      new CommandDistributionAcknowledgeProcessor(context)
          .processRecord(acknowledgementRecord(oldAck.setGeneration(1)));
      assertFalse(fixture.state.getCommandDistributionState().isPending(101, "demo", 1));
    }
  }

  @Test
  void failedRedistributionValidationRetainsExistingAcks() {
    try (final PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("redistribute-validation"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, 1);
      fixture.processor.processRecord(record(subscription("a", VALID_VARIABLES, "a")));
      fixture.state.getCommandDistributionState().acknowledge(101, "demo", 1);
      final var context =
          new TestContext(fixture.state, fixture.writers, fixture.keyGenerator::incrementAndGet);
      final var processor = new CommandDistributionRedistributeProcessor(context);
      final var original =
          fixture.state.getCommandDistributionState().distribution(101).orElseThrow();
      fixture.state.getCommandDistributionState().putTarget(101, "missing", 1);
      processor.processRecord(typed(redistribute(101, 0)));
      assertEquals(RejectionType.NOT_FOUND, fixture.writers.rejectionType);
      assertEquals(
          0,
          fixture
              .state
              .getCommandDistributionState()
              .distribution(101)
              .orElseThrow()
              .getGeneration());
      assertFalse(fixture.state.getCommandDistributionState().isPending(101, "demo", 1));
    }
  }

  private static CommandDistributionRedistributeRecord redistribute(
      final long key, final long generation) {
    return new CommandDistributionRedistributeRecord()
        .setDistributionKey(key)
        .setExpectedGeneration(generation)
        .setRequestedAtEpochMillis(1793400000000L);
  }

  private static <T extends UnifiedRecordValue> TypedRecord<T> typed(final T value) {
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

  @Test
  void firstDeploymentCreatesDistributionAndPendingForTopology() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, 3);

      fixture.processor.processRecord(
          record(subscription("resource-a", VALID_VARIABLES, "checksum-a")));

      assertFalse(fixture.writers.hasRejection());
      assertEquals(101, fixture.writers.lastEventKey);
      assertTrue(fixture.state.getCommandDistributionState().distribution(101).isPresent());
      assertEquals(
          101,
          fixture
              .state
              .getCommandDistributionState()
              .distributionKeyByDedupeKey(
                  CommandDistributionDedupe.dedupeKey(
                      subscription("resource-a", VALID_VARIABLES, "checksum-a")))
              .orElseThrow());
      assertTrue(fixture.state.getCommandDistributionState().isPending(101, "demo", 1));
      assertTrue(fixture.state.getCommandDistributionState().isPending(101, "demo", 2));
      assertTrue(fixture.state.getCommandDistributionState().isPending(101, "demo", 3));
    }
  }

  @Test
  void createsSubscriptionWithOnlyJobTypeAndDefaultOptions() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("defaults"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, 1);
      fixture.processor.processRecord(
          record(
              new CommandDistributionRequestRecord<JobSubscriptionRecord>()
                  .setValueType(ValueType.JOB_SUBSCRIPTION)
                  .setIntent(JobSubscriptionIntent.CREATE)
                  .setValue(
                      new JobSubscriptionRecord().setClusterId("demo").setJobType("charge"))));
      assertFalse(fixture.writers.hasRejection());
      final var value =
          (JobSubscriptionRecord)
              fixture
                  .state
                  .getCommandDistributionState()
                  .distribution(101)
                  .orElseThrow()
                  .getValue();
      assertEquals("", value.getName());
      assertEquals("{}", value.getVariables());
      assertEquals(3, value.getRetries());
      assertEquals(50, value.getPriority());
    }
  }

  @Test
  void mirroredDeploymentCreatesPendingForSourceAndDirectTarget() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("mirrored"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, "demo", 2);
      registerTopology(fixture.state, "backup", 3);
      fixture
          .state
          .getJobSubscriptionMirrorState()
          .put(
              new JobSubscriptionMirrorRecord()
                  .setMirrorKey(7)
                  .setSourceClusterId("demo")
                  .setTargetClusterId("backup"));

      fixture.processor.processRecord(
          record(subscription("resource-a", VALID_VARIABLES, "checksum-a")));

      assertEquals(
          List.of("demo", "backup"),
          fixture.state.getCommandDistributionState().targets(101).stream()
              .map(target -> target.getClusterId())
              .toList());
      assertTrue(fixture.state.getCommandDistributionState().isPending(101, "demo", 1));
      assertTrue(fixture.state.getCommandDistributionState().isPending(101, "demo", 2));
      assertTrue(fixture.state.getCommandDistributionState().isPending(101, "backup", 1));
      assertTrue(fixture.state.getCommandDistributionState().isPending(101, "backup", 2));
      assertTrue(fixture.state.getCommandDistributionState().isPending(101, "backup", 3));
    }
  }

  @Test
  void mirroredDeploymentDoesNotExpandTransitively() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("single-hop"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, "demo", 1);
      registerTopology(fixture.state, "backup", 1);
      registerTopology(fixture.state, "third", 1);
      fixture
          .state
          .getJobSubscriptionMirrorState()
          .put(
              new JobSubscriptionMirrorRecord()
                  .setMirrorKey(7)
                  .setSourceClusterId("demo")
                  .setTargetClusterId("backup"));
      fixture
          .state
          .getJobSubscriptionMirrorState()
          .put(
              new JobSubscriptionMirrorRecord()
                  .setMirrorKey(8)
                  .setSourceClusterId("backup")
                  .setTargetClusterId("third"));

      fixture.processor.processRecord(
          record(subscription("resource-a", VALID_VARIABLES, "checksum-a")));

      assertEquals(
          List.of("demo", "backup"),
          fixture.state.getCommandDistributionState().targets(101).stream()
              .map(target -> target.getClusterId())
              .toList());
      assertFalse(fixture.state.getCommandDistributionState().isTarget(101, "third"));
    }
  }

  @Test
  void deploymentIncludesAllDirectMirrorTargets() {
    try (final PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("multiple-targets"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, "demo", 1);
      registerTopology(fixture.state, "backup", 1);
      registerTopology(fixture.state, "third", 1);
      fixture
          .state
          .getJobSubscriptionMirrorState()
          .put(
              new JobSubscriptionMirrorRecord()
                  .setMirrorKey(7)
                  .setSourceClusterId("demo")
                  .setTargetClusterId("third"));
      fixture
          .state
          .getJobSubscriptionMirrorState()
          .put(
              new JobSubscriptionMirrorRecord()
                  .setMirrorKey(8)
                  .setSourceClusterId("demo")
                  .setTargetClusterId("backup"));

      fixture.processor.processRecord(
          record(subscription("resource-a", VALID_VARIABLES, "checksum-a")));

      assertEquals(
          List.of("demo", "backup", "third"),
          fixture.state.getCommandDistributionState().targets(101).stream()
              .map(target -> target.getClusterId())
              .toList());
    }
  }

  @Test
  void reciprocalMirrorDoesNotRecurse() {
    try (final PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("reciprocal-targets"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, "demo", 1);
      registerTopology(fixture.state, "backup", 1);
      fixture
          .state
          .getJobSubscriptionMirrorState()
          .put(
              new JobSubscriptionMirrorRecord()
                  .setMirrorKey(7)
                  .setSourceClusterId("demo")
                  .setTargetClusterId("backup"));
      fixture
          .state
          .getJobSubscriptionMirrorState()
          .put(
              new JobSubscriptionMirrorRecord()
                  .setMirrorKey(8)
                  .setSourceClusterId("backup")
                  .setTargetClusterId("demo"));

      fixture.processor.processRecord(
          record(subscription("resource-a", VALID_VARIABLES, "checksum-a")));

      assertEquals(
          List.of("demo", "backup"),
          fixture.state.getCommandDistributionState().targets(101).stream()
              .map(target -> target.getClusterId())
              .toList());
      assertEquals(1, fixture.state.getCommandDistributionState().distributions().size());
    }
  }

  @Test
  void duplicateDeploymentKeepsFrozenTargetsAfterMirrorCreation() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("frozen-targets"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, "demo", 1);
      registerTopology(fixture.state, "backup", 1);
      final CommandDistributionRequestRecord<JobSubscriptionRecord> request =
          subscription("resource-a", VALID_VARIABLES, "checksum-a");
      fixture.processor.processRecord(record(request));
      fixture
          .state
          .getJobSubscriptionMirrorState()
          .put(
              new JobSubscriptionMirrorRecord()
                  .setMirrorKey(7)
                  .setSourceClusterId("demo")
                  .setTargetClusterId("backup"));

      fixture.processor.processRecord(record(request));

      assertEquals(
          List.of("demo"),
          fixture.state.getCommandDistributionState().targets(101).stream()
              .map(target -> target.getClusterId())
              .toList());
      assertFalse(fixture.state.getCommandDistributionState().isTarget(101, "backup"));
    }
  }

  @Test
  void deploymentRejectsUnavailableMirrorTarget() {
    try (final PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("unavailable-mirror-target"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, "demo", 1);
      fixture
          .state
          .getJobSubscriptionMirrorState()
          .put(
              new JobSubscriptionMirrorRecord()
                  .setMirrorKey(7)
                  .setSourceClusterId("demo")
                  .setTargetClusterId("missing"));

      fixture.processor.processRecord(
          record(subscription("resource-a", VALID_VARIABLES, "checksum-a")));

      assertEquals(RejectionType.NOT_FOUND, fixture.writers.rejectionType);
      assertTrue(fixture.writers.rejectionReason.startsWith("CLUSTER_NOT_REGISTERED:"));
      assertTrue(fixture.state.getCommandDistributionState().distributions().isEmpty());
    }
  }

  @Test
  void deletionUsesDeploymentTargetsAfterMirrorIsRemoved() {
    try (final PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("mirrored-delete"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, "demo", 1);
      registerTopology(fixture.state, "backup", 1);
      fixture
          .state
          .getJobSubscriptionMirrorState()
          .put(
              new JobSubscriptionMirrorRecord()
                  .setMirrorKey(7)
                  .setSourceClusterId("demo")
                  .setTargetClusterId("backup"));
      fixture.processor.processRecord(
          record(subscription("resource-a", VALID_VARIABLES, "checksum-a")));
      fixture.state.getJobSubscriptionMirrorState().delete(7);

      fixture.processor.processRecord(record(deletion(101)));

      assertEquals(
          List.of("demo", "backup"),
          fixture.state.getCommandDistributionState().targets(102).stream()
              .map(target -> target.getClusterId())
              .toList());
      assertTrue(fixture.state.getCommandDistributionState().isPending(102, "demo", 1));
      assertTrue(fixture.state.getCommandDistributionState().isPending(102, "backup", 1));
    }
  }

  @Test
  void jobTypeUniquenessSurvivesRestartAndIsScopedToTenantAndCluster() {
    final Path path = tempDir.resolve("unique-job-type");
    try (final PartitionStorage storage = new PartitionStorage(path)) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, "demo", 1);
      registerTopology(fixture.state, "other", 1);
      fixture.processor.processRecord(record(subscription("charge", VALID_VARIABLES, "first")));
      final var anotherTenant = subscription("charge", VALID_VARIABLES, "first");
      anotherTenant.getValue().setTenantId("another");
      fixture.processor.processRecord(record(anotherTenant));
      fixture.processor.processRecord(
          record(subscription("other", "charge", VALID_VARIABLES, "first")));
      fixture.processor.processRecord(record(subscription("notify", VALID_VARIABLES, "first")));
      assertEquals(4, fixture.state.getCommandDistributionState().distributions().size());
    }
    try (final PartitionStorage storage = new PartitionStorage(path)) {
      final Fixture fixture = fixture(storage);
      fixture.processor.processRecord(
          record(subscription("charge", VALID_VARIABLES, "new-request")));
      assertFalse(fixture.writers.hasRejection());
      assertEquals(101, fixture.writers.lastResponseKey);
      assertEquals(0, fixture.writers.lastEventKey);
      assertEquals(100, fixture.keyGenerator.get());
      assertEquals(4, fixture.state.getCommandDistributionState().distributions().size());
    }
  }

  @Test
  void deletionMustFinishBeforeJobTypeCanAllocateANewSubscription() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("recreate"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, 1);
      fixture.processor.processRecord(record(subscription("charge", VALID_VARIABLES, "first")));
      fixture.state.getCommandDistributionState().acknowledge(101, "demo", 1);
      complete(fixture, 101, 0);
      fixture.processor.processRecord(record(deletion(101)));
      fixture.processor.processRecord(record(subscription("charge", VALID_VARIABLES, "second")));
      assertEquals(101, fixture.writers.lastResponseKey);
      assertEquals(102, fixture.keyGenerator.get());
      fixture.state.getCommandDistributionState().acknowledge(102, "demo", 1);
      complete(fixture, 102, 0);
      fixture.processor.processRecord(record(subscription("charge", VALID_VARIABLES, "second")));
      assertEquals(103, fixture.writers.lastResponseKey);
      assertEquals(103, fixture.keyGenerator.get());
      fixture.state.getCommandDistributionState().acknowledge(103, "demo", 1);
      complete(fixture, 103, 0);
      assertTrue(fixture.state.getJobSubscriptionState().get(101).isEmpty());
      assertTrue(fixture.state.getJobSubscriptionState().get(103).isPresent());
      fixture.processor.processRecord(record(deletion(101)));
      assertEquals(102, fixture.writers.lastResponseKey);
      assertTrue(fixture.state.getJobSubscriptionState().get(103).isPresent());
    }
  }

  @Test
  void duplicateDeploymentReturnsSameDistributionKey() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, 1);
      final CommandDistributionRequestRecord<JobSubscriptionRecord> request =
          subscription("resource-a", VALID_VARIABLES, "checksum-a");

      fixture.processor.processRecord(record(request));
      fixture.processor.processRecord(record(request));

      assertFalse(fixture.writers.hasRejection());
      assertEquals(101, fixture.writers.lastEventKey);
      assertEquals(101, fixture.keyGenerator.get());
    }
  }

  @Test
  void duplicateDeploymentDoesNotRecreateAcknowledgedPendingState() {
    try (final PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("duplicate-acked"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, 1);
      final CommandDistributionRequestRecord<JobSubscriptionRecord> request =
          subscription("resource-a", VALID_VARIABLES, "checksum-a");

      fixture.processor.processRecord(record(request));
      fixture.state.getCommandDistributionState().acknowledge(101, "demo", 1);
      fixture.processor.processRecord(record(request));

      assertFalse(fixture.writers.hasRejection());
      assertEquals(101, fixture.writers.lastEventKey);
      assertEquals(101, fixture.keyGenerator.get());
      assertFalse(fixture.state.getCommandDistributionState().isPending(101, "demo", 1));
    }
  }

  @Test
  void differentRequestAndDefinitionForSameJobTypeReturnOriginalSubscription() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, 1);

      fixture.processor.processRecord(
          record(subscription("resource-a", VALID_VARIABLES, "checksum-a")));
      final var duplicate = subscription("resource-b", "{\"amount\":99}", "another-request");
      duplicate.getValue().setJobType("resource-a");
      fixture.processor.processRecord(record(duplicate));

      assertFalse(fixture.writers.hasRejection());
      assertEquals(101, fixture.writers.lastEventKey);
      assertEquals(101, fixture.keyGenerator.get());
      assertEquals(
          VALID_VARIABLES,
          ((JobSubscriptionRecord)
                  fixture
                      .state
                      .getCommandDistributionState()
                      .distribution(101)
                      .orElseThrow()
                      .getValue())
              .getVariables());
    }
  }

  @Test
  void deleteMissingResourceIsRejected() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final Fixture fixture = fixture(storage);

      fixture.processor.processRecord(record(deletion(42)));

      assertEquals(RejectionType.NOT_FOUND, fixture.writers.rejectionType);
      assertEquals(RejectionType.NOT_FOUND, fixture.writers.responseRejectionType);
    }
  }

  @Test
  void duplicateDeletionReturnsSameDistribution() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, 1);
      fixture.processor.processRecord(
          record(subscription("resource-a", VALID_VARIABLES, "checksum-a")));

      final CommandDistributionRequestRecord<JobSubscriptionDeletionRecord> deletion =
          deletion(101);
      fixture.processor.processRecord(record(deletion));
      final long deleteKey = fixture.writers.lastEventKey;
      assertFalse(fixture.writers.hasRejection());
      assertEquals(102, deleteKey);

      fixture.processor.processRecord(record(deletion));

      assertFalse(fixture.writers.hasRejection());
      assertEquals(deleteKey, fixture.writers.lastEventKey);
      assertEquals(deleteKey, fixture.keyGenerator.get());
    }
  }

  @Test
  void firstDeletionCreatesDistributionAndPendingForTopology() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("first-deletion"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, 3);
      fixture.processor.processRecord(
          record(subscription("resource-a", VALID_VARIABLES, "checksum-a")));

      fixture.processor.processRecord(record(deletion(101)));

      assertFalse(fixture.writers.hasRejection());
      assertEquals(102, fixture.writers.lastEventKey);
      final CommandDistributionRecord<?> stored =
          fixture.state.getCommandDistributionState().distribution(102).orElseThrow();
      assertEquals(ValueType.JOB_SUBSCRIPTION_DELETION, stored.getValueType());
      assertEquals(101, ((JobSubscriptionDeletionRecord) stored.getValue()).getSubscriptionKey());
      assertTrue(fixture.state.getCommandDistributionState().isPending(102, "demo", 1));
      assertTrue(fixture.state.getCommandDistributionState().isPending(102, "demo", 2));
      assertTrue(fixture.state.getCommandDistributionState().isPending(102, "demo", 3));
    }
  }

  @Test
  void identicalDeploymentsInDifferentClustersHaveIndependentDistributionKeys() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("cluster-dedupe"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, "demo", 1);
      registerTopology(fixture.state, "other", 1);

      fixture.processor.processRecord(
          record(subscription("demo", "resource-a", VALID_VARIABLES, "checksum-a")));
      final long demoKey = fixture.writers.lastEventKey;
      fixture.processor.processRecord(
          record(subscription("other", "resource-a", VALID_VARIABLES, "checksum-a")));
      final long otherKey = fixture.writers.lastEventKey;

      assertEquals(101, demoKey);
      assertEquals(102, otherKey);
      assertTrue(fixture.state.getCommandDistributionState().isPending(demoKey, "demo", 1));
      assertTrue(fixture.state.getCommandDistributionState().isPending(otherKey, "other", 1));
    }
  }

  @Test
  void resourceCannotBeDeletedFromAnotherCluster() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("cluster-delete"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, "demo", 1);
      registerTopology(fixture.state, "other", 1);
      fixture.processor.processRecord(
          record(subscription("resource-a", VALID_VARIABLES, "checksum-a")));

      fixture.processor.processRecord(record(deletion("other", 101)));

      assertEquals(RejectionType.NOT_FOUND, fixture.writers.rejectionType);
      assertEquals(101, fixture.keyGenerator.get());
    }
  }

  @Test
  void deploymentForUnknownClusterIsRejected() {
    try (final PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("unknown-cluster"))) {
      final Fixture fixture = fixture(storage);

      fixture.processor.processRecord(
          record(subscription("unknown", "resource-a", VALID_VARIABLES, "checksum-a")));

      assertEquals(RejectionType.NOT_FOUND, fixture.writers.rejectionType);
      assertEquals(100, fixture.keyGenerator.get());
    }
  }

  @Test
  void acknowledgementFromAnotherClusterIsRejected() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("cluster-ack"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, 1);
      fixture.processor.processRecord(
          record(subscription("resource-a", VALID_VARIABLES, "checksum-a")));
      final CommandDistributionRecord<?> acknowledgement =
          fixture
              .state
              .getCommandDistributionState()
              .distribution(101)
              .orElseThrow()
              .setClusterId("other")
              .setPartitionId(1);

      new CommandDistributionAcknowledgeProcessor(
              new TestContext(
                  fixture.state, fixture.writers, fixture.keyGenerator::incrementAndGet))
          .processRecord(acknowledgementRecord(acknowledgement));

      assertEquals(RejectionType.INVALID_ARGUMENT, fixture.writers.rejectionType);
      assertTrue(fixture.state.getCommandDistributionState().isPending(101, "demo", 1));
    }
  }

  @Test
  void mirrorTargetAcknowledgementIsIdempotent() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("mirror-ack"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, "demo", 1);
      registerTopology(fixture.state, "backup", 1);
      fixture
          .state
          .getJobSubscriptionMirrorState()
          .put(
              new JobSubscriptionMirrorRecord()
                  .setMirrorKey(7)
                  .setSourceClusterId("demo")
                  .setTargetClusterId("backup"));
      fixture.processor.processRecord(
          record(subscription("resource-a", VALID_VARIABLES, "checksum-a")));
      final CommandDistributionAcknowledgeProcessor acknowledgements =
          new CommandDistributionAcknowledgeProcessor(
              new TestContext(
                  fixture.state, fixture.writers, fixture.keyGenerator::incrementAndGet));
      final CommandDistributionRecord<?> acknowledgement = acknowledgement(101, "backup", 1);

      acknowledgements.processRecord(acknowledgementRecord(acknowledgement));
      acknowledgements.processRecord(acknowledgementRecord(acknowledgement));

      assertFalse(fixture.writers.hasRejection());
      assertFalse(fixture.state.getCommandDistributionState().isPending(101, "backup", 1));
      assertEquals(1, fixture.writers.acknowledgedEventCount);
      assertEquals(0, fixture.writers.acknowledgementResponseCount);
      assertTrue(fixture.writers.completionCommands.isEmpty());
      assertEquals(
          1,
          fixture.state.getCommandDistributionState().targets(101).stream()
              .filter(target -> target.getClusterId().equals("backup"))
              .findFirst()
              .orElseThrow()
              .getAcknowledgedPartitionCount());
    }
  }

  @Test
  void lastAcknowledgementProducesCompletionWithoutWritingResponses() {
    try (final PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("ack-complete-chain"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, "demo", 1);
      registerTopology(fixture.state, "backup", 1);
      fixture
          .state
          .getJobSubscriptionMirrorState()
          .put(
              new JobSubscriptionMirrorRecord()
                  .setMirrorKey(7)
                  .setSourceClusterId("demo")
                  .setTargetClusterId("backup"));
      fixture.processor.processRecord(
          record(subscription("resource-a", VALID_VARIABLES, "checksum-a")));
      final var acknowledgements =
          new CommandDistributionAcknowledgeProcessor(
              new TestContext(
                  fixture.state, fixture.writers, fixture.keyGenerator::incrementAndGet));
      acknowledgements.processRecord(acknowledgementRecord(acknowledgement(101, "demo", 1)));
      assertTrue(fixture.writers.completionCommands.isEmpty());
      acknowledgements.processRecord(acknowledgementRecord(acknowledgement(101, "backup", 1)));
      assertEquals(1, fixture.writers.completionCommands.size());
      final var completion = fixture.writers.completionCommands.getFirst();
      assertEquals(101, completion.getDistributionKey());
      assertEquals(0, completion.getGeneration());
      assertEquals("demo", completion.getClusterId());
      assertTrue(fixture.state.getCommandDistributionState().isPlanComplete(101));

      // Repeated ACKs do not produce additional completion commands.
      acknowledgements.processRecord(acknowledgementRecord(acknowledgement(101, "backup", 1)));
      assertEquals(1, fixture.writers.completionCommands.size());
      complete(fixture, completion.getDistributionKey(), completion.getGeneration());
      acknowledgements.processRecord(acknowledgementRecord(acknowledgement(101, "backup", 1)));
      complete(fixture, completion.getDistributionKey(), completion.getGeneration());
      assertEquals(1, fixture.writers.completionCommands.size());
      assertEquals(2, fixture.writers.acknowledgedEventCount);
      assertEquals(2, fixture.writers.completedEventCount);
      assertEquals(0, fixture.writers.acknowledgementResponseCount);
      assertEquals(0, fixture.writers.completionResponseCount);
    }
  }

  @Test
  void acknowledgementOutsideTargetPartitionRangeIsRejected() {
    try (final PartitionStorage storage = new PartitionStorage(tempDir.resolve("partition-ack"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, 1);
      fixture.processor.processRecord(
          record(subscription("resource-a", VALID_VARIABLES, "checksum-a")));

      new CommandDistributionAcknowledgeProcessor(
              new TestContext(
                  fixture.state, fixture.writers, fixture.keyGenerator::incrementAndGet))
          .processRecord(acknowledgementRecord(acknowledgement(101, "demo", 2)));

      assertEquals(RejectionType.INVALID_ARGUMENT, fixture.writers.rejectionType);
      assertTrue(fixture.state.getCommandDistributionState().isPending(101, "demo", 1));
    }
  }

  @Test
  void distributionQueryReturnsTargetProgress() {
    try (final PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("distribution-query"))) {
      final Fixture fixture = fixture(storage);
      registerTopology(fixture.state, "demo", 1);
      registerTopology(fixture.state, "backup", 2);
      fixture
          .state
          .getJobSubscriptionMirrorState()
          .put(
              new JobSubscriptionMirrorRecord()
                  .setMirrorKey(7)
                  .setSourceClusterId("demo")
                  .setTargetClusterId("backup"));
      fixture.processor.processRecord(
          record(subscription("resource-a", VALID_VARIABLES, "checksum-a")));
      fixture.state.getCommandDistributionState().acknowledge(101, "demo", 1);
      fixture.state.getCommandDistributionState().acknowledge(101, "backup", 1);

      new CommandDistributionQueryProcessor(
              new TestContext(
                  fixture.state, fixture.writers, fixture.keyGenerator::incrementAndGet))
          .processRecord(
              distributionQueryRecord(
                  new CommandDistributionQueryRecord().setDistributionKey(101)));

      final CommandDistributionQueryRecord query = fixture.writers.lastDistributionQuery;
      assertEquals("demo", query.getOriginClusterId());
      assertEquals("JOB_SUBSCRIPTION", query.getValueType());
      assertEquals(JobSubscriptionDistributionStatus.DISTRIBUTING, query.getStatus());
      assertEquals(2, query.getTargets().size());
      assertEquals(
          JobSubscriptionDistributionStatus.COMPLETED, query.getTargets().get(0).getStatus());
      assertEquals("backup", query.getTargets().get(1).getClusterId());
      assertEquals(2, query.getTargets().get(1).getExpectedPartitions());
      assertEquals(1, query.getTargets().get(1).getAcknowledgedPartitions());
      assertEquals(List.of(2), query.getTargets().get(1).getPendingPartitions());
      assertEquals(List.of(1), query.getTargets().get(1).getConfirmedPartitionIds());
      assertEquals(7, query.getTargets().get(1).getMirrorKey());
      assertEquals(VALID_VARIABLES, query.getSubscription().getVariables());
      assertEquals("resource-a", query.getJobType());

      // Deletion retains subscription metadata but owns a separate ACK plan.
      fixture.state.getJobSubscriptionMirrorState().delete(7);
      fixture.processor.processRecord(record(deletion(101)));
      final var processor =
          new CommandDistributionQueryProcessor(
              new TestContext(
                  fixture.state, fixture.writers, fixture.keyGenerator::incrementAndGet));
      processor.processRecord(
          distributionQueryRecord(new CommandDistributionQueryRecord().setDistributionKey(102)));
      final var removal = fixture.writers.lastDistributionQuery;
      assertEquals("JOB_SUBSCRIPTION_DELETION", removal.getValueType());
      assertEquals(101, removal.getSubscriptionKey());
      assertEquals(0, removal.getDeletionDistributionKey());
      assertEquals("resource-a", removal.getName());
      assertEquals("resource-a", removal.getJobType());
      assertEquals("tenant", removal.getTenantId());
      assertEquals(VALID_VARIABLES, removal.getSubscription().getVariables());
      assertEquals(JobSubscriptionDistributionStatus.DISTRIBUTING, removal.getStatus());
      assertEquals(2, removal.getTargets().size());
      assertEquals(0, removal.getTargets().getFirst().getAcknowledgedPartitions());
      assertEquals(List.of(1, 2), removal.getTargets().get(1).getPendingPartitions());

      processor.processRecord(
          distributionQueryRecord(new CommandDistributionQueryRecord().setDistributionKey(101)));
      final var original = fixture.writers.lastDistributionQuery;
      assertEquals(102, original.getDeletionDistributionKey());
      assertEquals(JobSubscriptionDistributionStatus.DELETING, original.getStatus());
      assertEquals(1, original.getTargets().getFirst().getAcknowledgedPartitions());
      assertEquals(List.of(2), original.getTargets().get(1).getPendingPartitions());

      final var context =
          new TestContext(fixture.state, fixture.writers, fixture.keyGenerator::incrementAndGet);
      new CommandDistributionListProcessor(context)
          .processRecord(typed(new CommandDistributionListRecord().setClusterId("backup")));
      assertEquals(
          102,
          fixture
              .writers
              .lastResourceList
              .getSubscriptions()
              .getFirst()
              .getDeletionDistributionKey());

      fixture.state.getCommandDistributionState().acknowledge(102, "demo", 1);
      fixture.state.getCommandDistributionState().acknowledge(102, "backup", 1);
      processor.processRecord(
          distributionQueryRecord(new CommandDistributionQueryRecord().setDistributionKey(102)));
      assertEquals(
          JobSubscriptionDistributionStatus.DISTRIBUTING,
          fixture.writers.lastDistributionQuery.getStatus());
      assertEquals(
          List.of(2),
          fixture.writers.lastDistributionQuery.getTargets().get(1).getPendingPartitions());
      fixture.state.getCommandDistributionState().acknowledge(102, "backup", 2);
      complete(fixture, 102, 0);
      processor.processRecord(
          distributionQueryRecord(new CommandDistributionQueryRecord().setDistributionKey(102)));
      assertEquals(
          JobSubscriptionDistributionStatus.COMPLETED,
          fixture.writers.lastDistributionQuery.getStatus());
      processor.processRecord(
          distributionQueryRecord(new CommandDistributionQueryRecord().setDistributionKey(101)));
      assertEquals(
          JobSubscriptionDistributionStatus.DELETED,
          fixture.writers.lastDistributionQuery.getStatus());
      assertEquals(102, fixture.writers.lastDistributionQuery.getDeletionDistributionKey());
      assertEquals(
          List.of(2),
          fixture.writers.lastDistributionQuery.getTargets().get(1).getPendingPartitions());
    }
  }

  @Test
  void distributionQueryRejectsMissingPlan() {
    try (final PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("missing-distribution-query"))) {
      final Fixture fixture = fixture(storage);

      new CommandDistributionQueryProcessor(
              new TestContext(
                  fixture.state, fixture.writers, fixture.keyGenerator::incrementAndGet))
          .processRecord(
              distributionQueryRecord(
                  new CommandDistributionQueryRecord().setDistributionKey(404)));

      assertEquals(RejectionType.NOT_FOUND, fixture.writers.rejectionType);
      assertTrue(
          fixture.writers.rejectionReason.startsWith("JOB_SUBSCRIPTION_DISTRIBUTION_NOT_FOUND:"));
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidDeployments")
  void invalidDeploymentIsRejected(
      final String caseName, final CommandDistributionRequestRecord<?> request) {
    try (final PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("invalid-" + caseName))) {
      final Fixture fixture = fixture(storage);

      fixture.processor.processRecord(record(request));

      assertEquals(RejectionType.INVALID_ARGUMENT, fixture.writers.rejectionType);
      assertEquals(RejectionType.INVALID_ARGUMENT, fixture.writers.responseRejectionType);
      assertEquals(100, fixture.keyGenerator.get());
      assertEquals(0, fixture.writers.lastEventKey);
    }
  }

  @Test
  void deletionWithNonPositiveResourceKeyIsRejected() {
    try (final PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("invalid-delete-key"))) {
      final Fixture fixture = fixture(storage);

      fixture.processor.processRecord(record(deletion(0)));

      assertEquals(RejectionType.INVALID_ARGUMENT, fixture.writers.rejectionType);
      assertEquals(RejectionType.INVALID_ARGUMENT, fixture.writers.responseRejectionType);
      assertEquals(100, fixture.keyGenerator.get());
      assertEquals(0, fixture.writers.lastEventKey);
    }
  }

  private Fixture fixture(final PartitionStorage storage) {
    final MutableProcessingState state =
        new MutableProcessingState(new StateStoreProcessingStore(storage));
    final CapturingWriters writers = new CapturingWriters(state);
    final AtomicLong keys = new AtomicLong(100);
    final CommandDistributionStartProcessor processor =
        new CommandDistributionStartProcessor(
            new TestContext(state, writers, keys::incrementAndGet));
    return new Fixture(state, writers, keys, processor);
  }

  private static CommandDistributionRequestRecord<JobSubscriptionRecord> subscription(
      final String resourceName, final String content, final String checksum) {
    return subscription("demo", resourceName, content, checksum);
  }

  private static CommandDistributionRequestRecord<JobSubscriptionRecord> subscription(
      final String clusterId,
      final String resourceName,
      final String content,
      final String checksum) {
    final JobSubscriptionRecord resource =
        template(resourceName, content, checksum).setClusterId(clusterId);
    return new CommandDistributionRequestRecord<JobSubscriptionRecord>()
        .setValueType(ValueType.JOB_SUBSCRIPTION)
        .setIntent(JobSubscriptionIntent.CREATE)
        .setValue(resource);
  }

  private static JobSubscriptionRecord template(
      final String resourceName, final String content, final String checksum) {
    return new JobSubscriptionRecord()
        .setClusterId("demo")
        .setTenantId("tenant")
        .setName(resourceName)
        .setJobType(resourceName)
        .setVariables(content);
  }

  private static Stream<Arguments> invalidDeployments() {
    return Stream.of(
        Arguments.of(
            "unsupported-value-type",
            new CommandDistributionRequestRecord<JobSubscriptionRecord>()
                .setValueType(ValueType.JOB)
                .setIntent(JobSubscriptionIntent.CREATE)
                .setValue(template("resource-a", VALID_VARIABLES, "checksum-a"))),
        Arguments.of(
            "missing-value",
            new CommandDistributionRequestRecord<JobSubscriptionRecord>()
                .setValueType(ValueType.JOB_SUBSCRIPTION)
                .setIntent(JobSubscriptionIntent.CREATE)),
        Arguments.of(
            "wrong-intent",
            new CommandDistributionRequestRecord<JobSubscriptionRecord>()
                .setValueType(ValueType.JOB_SUBSCRIPTION)
                .setIntent(JobSubscriptionIntent.DELETE)
                .setValue(template("resource-a", VALID_VARIABLES, "checksum-a"))),
        Arguments.of(
            "missing-cluster",
            subscription("resource-a", VALID_VARIABLES, "checksum-a")
                .setValue(template("resource-a", VALID_VARIABLES, "checksum-a").setClusterId(""))),
        Arguments.of(
            "missing-job-type",
            subscription("resource-a", VALID_VARIABLES, "checksum-a")
                .setValue(template("resource-a", VALID_VARIABLES, "checksum-a").setJobType(""))),
        Arguments.of(
            "invalid-priority",
            subscription("resource-a", VALID_VARIABLES, "checksum-a")
                .setValue(template("resource-a", VALID_VARIABLES, "checksum-a").setPriority(0))),
        Arguments.of(
            "malformed-variables",
            subscription("resource-a", VALID_VARIABLES, "checksum-a")
                .setValue(template("resource-a", "{", "checksum-a"))));
  }

  private static CommandDistributionRequestRecord<JobSubscriptionDeletionRecord> deletion(
      final long subscriptionKey) {
    return deletion("demo", subscriptionKey);
  }

  private static CommandDistributionRequestRecord<JobSubscriptionDeletionRecord> deletion(
      final String clusterId, final long subscriptionKey) {
    final JobSubscriptionDeletionRecord resource =
        new JobSubscriptionDeletionRecord()
            .setClusterId(clusterId)
            .setTenantId("tenant")
            .setSubscriptionKey(subscriptionKey);
    return new CommandDistributionRequestRecord<JobSubscriptionDeletionRecord>()
        .setValueType(ValueType.JOB_SUBSCRIPTION_DELETION)
        .setIntent(JobSubscriptionIntent.DELETE)
        .setValue(resource);
  }

  private static CommandDistributionRecord<?> acknowledgement(
      final long distributionKey, final String clusterId, final int partitionId) {
    return new CommandDistributionRecord<>()
        .setDistributionKey(distributionKey)
        .setClusterId(clusterId)
        .setPartitionId(partitionId);
  }

  private static TypedRecord<CommandDistributionRequestRecord<?>> record(
      final CommandDistributionRequestRecord<?> value) {
    return new TypedRecord<>() {
      @Override
      public long getKey() {
        return 1;
      }

      @Override
      public CommandDistributionRequestRecord<?> getValue() {
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

  private static TypedRecord<CommandDistributionRecord<?>> acknowledgementRecord(
      final CommandDistributionRecord<?> value) {
    return new TypedRecord<>() {
      @Override
      public long getKey() {
        return value.getDistributionKey();
      }

      @Override
      public CommandDistributionRecord<?> getValue() {
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

  private static TypedRecord<CommandDistributionQueryRecord> distributionQueryRecord(
      final CommandDistributionQueryRecord value) {
    return new TypedRecord<>() {
      @Override
      public long getKey() {
        return value.getDistributionKey();
      }

      @Override
      public CommandDistributionQueryRecord getValue() {
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

  private static void registerTopology(
      final MutableProcessingState state, final int partitionSize) {
    registerTopology(state, "demo", partitionSize);
  }

  private static void registerTopology(
      final MutableProcessingState state, final String clusterId, final int partitionSize) {
    new BrokerClusterCreatedApplier(state)
        .applyState(
            1,
            new ClusterConfigRecord()
                .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                .setClusterId(clusterId)
                .setClusterSize(1)
                .setPartitionSize(partitionSize)
                .setReplicationFactor(1));
  }

  private record Fixture(
      MutableProcessingState state,
      CapturingWriters writers,
      AtomicLong keyGenerator,
      CommandDistributionStartProcessor processor) {}

  private static final class TestContext implements TypedRecordProcessorContext {
    private final MutableProcessingState state;
    private final CapturingWriters writers;
    private final KeyGenerator keyGenerator;

    private TestContext(
        final MutableProcessingState state,
        final CapturingWriters writers,
        final KeyGenerator keyGenerator) {
      this.state = state;
      this.writers = writers;
      this.keyGenerator = keyGenerator;
    }

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
    private final CapturingStateWriter stateWriter = new CapturingStateWriter();
    private final CapturingRejectionWriter rejectionWriter = new CapturingRejectionWriter();
    private long lastEventKey;
    private long lastResponseKey;
    private RejectionType rejectionType;
    private String rejectionReason;
    private RejectionType responseRejectionType;
    private int acknowledgedEventCount;
    private int acknowledgementResponseCount;
    private int completedEventCount;
    private int completionResponseCount;
    private int cancelledTimerEventCount;
    private final List<CommandDistributionRecord<?>> completionCommands = new ArrayList<>();
    private CommandDistributionQueryRecord lastDistributionQuery;
    private CommandDistributionListRecord lastResourceList;

    private CapturingWriters(final MutableProcessingState state) {
      this.state = state;
    }

    boolean hasRejection() {
      return rejectionType != null;
    }

    @Override
    public TypedCommandWriter command() {
      return (key, intent, value) -> {
        assertEquals(CommandDistributionIntent.COMPLETE, intent);
        final var distribution = (CommandDistributionRecord<?>) value;
        assertEquals(key, distribution.getDistributionKey());
        completionCommands.add(distribution);
      };
    }

    @Override
    public StateWriter state() {
      return stateWriter;
    }

    @Override
    public TypedRejectionWriter rejection() {
      return rejectionWriter;
    }

    @Override
    public TypedResponseWriter response() {
      return new TypedResponseWriter() {
        @Override
        public void writeEventOnCommand(
            final long eventKey,
            final Intent eventIntent,
            final UnifiedRecordValue eventValue,
            final TypedRecord<?> sourceCommand) {
          lastResponseKey = eventKey;
          if (eventIntent == CommandDistributionIntent.ACKNOWLEDGED) {
            acknowledgementResponseCount++;
          } else if (eventIntent == CommandDistributionIntent.COMPLETED) {
            completionResponseCount++;
          }
        }

        @Override
        public void writeRejectionOnCommand(
            final TypedRecord<?> sourceCommand, final RejectionType type, final String reason) {
          responseRejectionType = type;
        }
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
        lastEventKey = key;
        if (intent == CommandDistributionIntent.STARTED) {
          new CommandDistributionStartedApplier(state)
              .applyState(key, (CommandDistributionRecord<?>) value);
        } else if (intent == CommandDistributionIntent.ACKNOWLEDGED) {
          acknowledgedEventCount++;
          new CommandDistributionAcknowledgedApplier(state)
              .applyState(key, (CommandDistributionRecord<?>) value);
        } else if (intent == CommandDistributionIntent.QUERIED) {
          lastDistributionQuery = (CommandDistributionQueryRecord) value;
        } else if (intent == CommandDistributionIntent.LISTED) {
          lastResourceList = (CommandDistributionListRecord) value;
        } else if (intent == CommandDistributionIntent.REDISTRIBUTED) {
          new CommandDistributionRedistributedApplier(state)
              .applyState(key, (CommandDistributionRecord<?>) value);
        } else if (intent == CommandDistributionIntent.COMPLETED) {
          completedEventCount++;
          PlacementDriverEventAppliers.create(state).applyState(key, intent, value, recordVersion);
        } else if (intent == TimerIntent.CREATED) {
          PlacementDriverEventAppliers.create(state).applyState(key, intent, value, recordVersion);
        } else if (intent == JobSubscriptionIntent.CREATED
            || intent == JobSubscriptionIntent.DELETED) {
          PlacementDriverEventAppliers.create(state).applyState(key, intent, value, recordVersion);
        } else if (intent == TimerIntent.CANCELED) {
          cancelledTimerEventCount++;
          PlacementDriverEventAppliers.create(state).applyState(key, intent, value, recordVersion);
        }
      }

      @Override
      public void appendFollowUpEvent(
          final long key,
          final ValueType valueType,
          final Intent intent,
          final UnifiedRecordValue value) {
        appendFollowUpEvent(key, valueType, intent, value, 1);
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

    private final class CapturingRejectionWriter implements TypedRejectionWriter {
      @Override
      public void appendRejection(final RejectionType type, final String reason) {
        rejectionType = type;
        rejectionReason = reason;
      }
    }
  }
}
