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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.QueryType;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.streamplatform.api.exception.ExceededBatchRecordSizeException;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.api.state.EventAppliers;
import io.taskite.streamplatform.logstream.LogAppendEntry;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.streamplatform.record.TypedRecordImpl;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class DirectWritersTest {
  @Test
  void appendRejectionPreservesCommandIntent() {
    final AtomicReference<List<LogAppendEntry>> appended = new AtomicReference<>();
    final DirectWriters writers =
        new DirectWriters(
            (entries, sourcePosition) -> {
              appended.set(entries);
              return CompletableActorFuture.completed(2L);
            },
            CommandFailureObserver.noop(),
            new EventAppliers(),
            () -> 1L,
            Integer.MAX_VALUE,
            TypedEventRegistry.builder()
                .register(ValueType.TOPOLOGY, TopologyQueryRecord::new)
                .build(),
            Runnable::run);
    final TopologyQueryRecord value =
        new TopologyQueryRecord()
            .setClusterId("demo")
            .setQueryType(QueryType.BROKER_CLUSTER_CONFIGURATION);
    final RecordMetadata commandMetadata =
        new RecordMetadata()
            .requestId("request-1")
            .recordType(RecordType.COMMAND)
            .valueType(ValueType.TOPOLOGY)
            .intent(TopologyIntent.QUERY);
    final TypedRecordImpl command = new TypedRecordImpl(1);
    command.wrap(LoggedEvent.ofRecord(1L, commandMetadata, value), commandMetadata, value);

    writers.beginBatch(command);
    writers.rejection().appendRejection(RejectionType.NOT_FOUND, "CLUSTER_NOT_REGISTERED");
    assertNull(appended.get());
    writers.awaitAppends().join();

    final RecordMetadata rejectionMetadata = appended.get().getFirst().recordMetadata();
    assertEquals(RecordType.COMMAND_REJECTION, rejectionMetadata.getRecordType());
    assertEquals(ValueType.TOPOLOGY, rejectionMetadata.getValueType());
    assertEquals(TopologyIntent.QUERY, rejectionMetadata.getIntent());
    assertEquals(RejectionType.NOT_FOUND, rejectionMetadata.getRejectionType());
    assertEquals("CLUSTER_NOT_REGISTERED", rejectionMetadata.getRejectionReason());
    assertEquals(0, appended.get().getFirst().recordValue().getLength());
  }

  @Test
  void rejectsRecordBeforeAddingItToAnOversizedBatch() {
    final DirectWriters writers =
        new DirectWriters(
            (entries, sourcePosition) -> CompletableActorFuture.completed(2L),
            CommandFailureObserver.noop(),
            new EventAppliers(),
            () -> 1L,
            128,
            Runnable::run);
    final TypedRecordImpl command = command(1L, "root");
    final TopologyQueryRecord oversized =
        new TopologyQueryRecord()
            .setClusterId("x".repeat(512))
            .setQueryType(QueryType.BROKER_CLUSTER_CONFIGURATION);

    writers.beginBatch(command);

    assertThrows(
        ExceededBatchRecordSizeException.class,
        () -> writers.command().appendFollowUpCommand(2L, TopologyIntent.QUERY, oversized));
    assertEquals(0, writers.recordCount());
    assertEquals(Integer.BYTES, writers.payloadBytes());
  }

  @Test
  void batchesAndFreezesFollowUpsFromOneCommand() {
    final AtomicInteger appendCalls = new AtomicInteger();
    final AtomicReference<List<LogAppendEntry>> appended = new AtomicReference<>();
    final DirectWriters writers =
        new DirectWriters(
            (entries, sourcePosition) -> {
              appendCalls.incrementAndGet();
              appended.set(entries);
              return CompletableActorFuture.completed(3L);
            },
            CommandFailureObserver.noop(),
            new EventAppliers(),
            () -> 1L,
            Integer.MAX_VALUE,
            TypedEventRegistry.builder()
                .register(ValueType.TOPOLOGY, TopologyQueryRecord::new)
                .build(),
            Runnable::run);
    final TopologyQueryRecord value =
        new TopologyQueryRecord()
            .setClusterId("before")
            .setQueryType(QueryType.BROKER_CLUSTER_CONFIGURATION);
    final RecordMetadata commandMetadata =
        new RecordMetadata()
            .requestId("request-1")
            .recordType(RecordType.COMMAND)
            .valueType(ValueType.TOPOLOGY)
            .intent(TopologyIntent.QUERY);
    final TypedRecordImpl command = new TypedRecordImpl(1);
    command.wrap(LoggedEvent.ofRecord(1L, commandMetadata, value), commandMetadata, value);

    writers.beginBatch(command);
    writers.command().appendFollowUpCommand(2L, TopologyIntent.QUERY, value);
    value.setClusterId("after");
    writers.command().appendFollowUpCommand(3L, TopologyIntent.QUERY, value);
    writers.awaitAppends().join();

    assertEquals(1, appendCalls.get());
    assertEquals(2, appended.get().size());
    assertEquals("before", readTopology(appended.get().get(0)).getClusterId());
    assertEquals("after", readTopology(appended.get().get(1)).getClusterId());
    assertEquals(3L, writers.highestWrittenPosition());
  }

  @Test
  void retainsRecordsAndRootPositionWhenSwitchingCommands() {
    final AtomicReference<List<LogAppendEntry>> appended = new AtomicReference<>();
    final AtomicLong sourcePosition = new AtomicLong();
    final DirectWriters writers =
        new DirectWriters(
            (entries, source) -> {
              appended.set(entries);
              sourcePosition.set(source);
              return CompletableActorFuture.completed(9L);
            },
            CommandFailureObserver.noop(),
            new EventAppliers(),
            () -> 1L,
            Runnable::run);
    final TypedRecordImpl root = command(7L, "root");
    final TypedRecordImpl followUp = command(8L, "follow-up");

    writers.beginBatch(root);
    writers.command().appendFollowUpCommand(2L, TopologyIntent.QUERY, root.getValue());
    writers.beginCommand(followUp);
    writers.command().appendFollowUpCommand(3L, TopologyIntent.QUERY, followUp.getValue());
    writers.awaitAppends().join();

    assertEquals(2, appended.get().size());
    assertEquals(7L, sourcePosition.get());
  }

  @Test
  void restoresTypedValuesForWrittenEvents() {
    final EventAppliers eventAppliers =
        new EventAppliers().register(JobIntent.CREATED, (key, value) -> {});
    final DirectWriters writers =
        new DirectWriters(
            (entries, sourcePosition) -> CompletableActorFuture.completed(2L),
            CommandFailureObserver.noop(),
            eventAppliers,
            () -> 1L,
            Integer.MAX_VALUE,
            TypedEventRegistry.builder().register(ValueType.JOB, JobRecord::new).build(),
            Runnable::run);
    final JobRecord value = new JobRecord().setJobType("service-task");

    writers.beginBatch(command(1L, "root"));
    writers.state().appendFollowUpEvent(2L, ValueType.JOB, JobIntent.CREATED, value);

    final LoggedEvent written = writers.writtenRecords().getFirst();
    final JobRecord restored = assertInstanceOf(JobRecord.class, written.value());
    assertEquals("service-task", restored.getType());
  }

  @Test
  void accumulatesFrozenCommandBoundResponsesAndKeepsTheLastResponsePerRequest() {
    final DirectWriters writers =
        new DirectWriters(
            (entries, sourcePosition) -> CompletableActorFuture.completed(2L),
            CommandFailureObserver.noop(),
            new EventAppliers(),
            () -> 1L,
            Integer.MAX_VALUE,
            TypedEventRegistry.builder()
                .register(ValueType.TOPOLOGY, TopologyQueryRecord::new)
                .build(),
            Runnable::run);
    final TypedRecordImpl root = command(1L, "first");
    final TypedRecordImpl followUp = command(2L, "second");

    writers.beginBatch(root);
    writers.response().writeEventOnCommand(11L, TopologyIntent.QUERY, root.getValue(), root);
    ((TopologyQueryRecord) root.getValue()).setClusterId("mutated");
    writers.response().writeEventOnCommand(12L, TopologyIntent.QUERY, root.getValue(), root);
    writers
        .response()
        .writeEventOnCommand(22L, TopologyIntent.QUERY, followUp.getValue(), followUp);

    assertFalse(writers.isEmpty());
    assertEquals(2, writers.responses().size());
    assertEquals("request-1", writers.responses().get(0).metadata().getRequestId());
    assertEquals(12L, writers.responses().get(0).key());
    assertTrue(writers.responses().get(0).value() instanceof TopologyQueryRecord);
    assertEquals("mutated", readTopology(writers.responses().get(0)).getClusterId());
    assertEquals("request-2", writers.responses().get(1).metadata().getRequestId());
    assertEquals("second", readTopology(writers.responses().get(1)).getClusterId());
  }

  @Test
  void resetsResponsesAndPostCommitActionsWithTheBatch() {
    final DirectWriters writers =
        new DirectWriters(
            (entries, sourcePosition) -> CompletableActorFuture.completed(2L),
            CommandFailureObserver.noop(),
            new EventAppliers(),
            () -> 1L,
            Runnable::run);
    final TypedRecordImpl first = command(1L, "first");

    writers.beginBatch(first);
    writers.response().writeEventOnCommand(11L, TopologyIntent.QUERY, first.getValue(), first);
    writers.postCommit().append(() -> {});
    assertFalse(writers.isEmpty());
    assertEquals(1, writers.responses().size());
    assertEquals(1, writers.postCommitActions().size());

    writers.beginBatch(command(2L, "second"));

    assertTrue(writers.isEmpty());
    assertTrue(writers.responses().isEmpty());
    assertTrue(writers.postCommitActions().isEmpty());
  }

  private static TypedRecordImpl command(final long position, final String clusterId) {
    final TopologyQueryRecord value =
        new TopologyQueryRecord()
            .setClusterId(clusterId)
            .setQueryType(QueryType.BROKER_CLUSTER_CONFIGURATION);
    final RecordMetadata metadata =
        new RecordMetadata()
            .requestId("request-" + position)
            .recordType(RecordType.COMMAND)
            .valueType(ValueType.TOPOLOGY)
            .intent(TopologyIntent.QUERY);
    final TypedRecordImpl command = new TypedRecordImpl(1);
    command.wrap(
        LoggedEvent.of(LogAppendEntry.of(position, metadata, value), position, -1L, 0L),
        metadata,
        value);
    return command;
  }

  private static TopologyQueryRecord readTopology(final LogAppendEntry entry) {
    final byte[] bytes = new byte[entry.recordValue().getLength()];
    entry.recordValue().write(new UnsafeBuffer(bytes), 0);
    final TopologyQueryRecord record = new TopologyQueryRecord();
    record.wrap(new UnsafeBuffer(bytes));
    return record;
  }

  private static TopologyQueryRecord readTopology(final LoggedEvent event) {
    final byte[] bytes = new byte[event.value().getLength()];
    event.value().write(new UnsafeBuffer(bytes), 0);
    final TopologyQueryRecord record = new TopologyQueryRecord();
    record.wrap(new UnsafeBuffer(bytes));
    return record;
  }
}
