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
package io.taskite.engine.processing.job;

import static org.assertj.core.api.Assertions.assertThat;

import io.taskite.engine.state.appliers.BrokerEventAppliers;
import io.taskite.engine.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.command.CommandFailureObserver;
import io.taskite.streamplatform.command.DirectWriters;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.streamplatform.record.TypedRecordImpl;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class JobCreateProcessorTest {
  @TempDir Path directory;
  private PartitionStorage storage;
  private StateStoreProcessingStore store;
  private MutableProcessingState state;
  private final AtomicLong keys = new AtomicLong(100);
  private DirectWriters writers;
  private JobCreateProcessor processor;

  @BeforeEach
  void setUp() {
    open();
    state
        .getJobSubscriptionState()
        .create(42, new JobSubscriptionRecord().setJobType("charge").setTenantId("tenant"));
  }

  void open() {
    storage = new PartitionStorage(directory);
    store = new StateStoreProcessingStore(storage);
    state = new MutableProcessingState(store);
    store.getCurrentTransaction();
    writers =
        new DirectWriters(
            (records, position) -> CompletableActorFuture.completed(1L),
            CommandFailureObserver.noop(),
            BrokerEventAppliers.create(state),
            keys::incrementAndGet,
            Runnable::run);
    processor = new JobCreateProcessor(state, writers, keys::incrementAndGet);
  }

  @AfterEach
  void close() throws Exception {
    store.getCurrentTransaction().rollback();
    storage.close();
  }

  private JobRecord request() {
    return new JobRecord()
        .setJobType("charge")
        .setTenantId("tenant")
        .setVariablesMap(Map.of("amount", 7));
  }

  private LoggedEvent create(final JobRecord value) {
    final var metadata =
        new RecordMetadata()
            .valueType(ValueType.JOB)
            .intent(JobIntent.CREATE)
            .recordType(RecordType.COMMAND)
            .requestId("rpc-" + keys.get())
            .requestStreamId("client");
    final var command = new TypedRecordImpl(1);
    command.wrap(LoggedEvent.ofRecord(-1, metadata, value), metadata, value);
    writers.beginBatch(command);
    processor.processRecord((TypedRecord<JobRecord>) (TypedRecord<?>) command);
    return writers.responses().getFirst();
  }

  @Test
  void createsJobWithMatchingSubscriptionAndVariables() {
    final var response = create(request());
    assertThat(response.metadata().getIntent()).isEqualTo(JobIntent.CREATED);
    assertThat(response.getKey()).isEqualTo(101);
    assertThat(state.getJobState().getJob(101).getType()).isEqualTo("charge");
    assertThat(state.getJobState().getJob(101).getSubscriptionKey()).isEqualTo(42);
    assertThat(state.getVariableState().getVariables(101)).containsEntry("amount", 7);
  }

  @Test
  void associatesJobsByTypeAndTenantWithoutASubscriptionKeyInTheRequest() throws Exception {
    state
        .getJobSubscriptionState()
        .create(42, new JobSubscriptionRecord().setJobType("charge").setTenantId("tenant"));
    state
        .getJobSubscriptionState()
        .create(43, new JobSubscriptionRecord().setJobType("charge").setTenantId("another"));
    state
        .getJobSubscriptionState()
        .create(44, new JobSubscriptionRecord().setJobType("refund").setTenantId("tenant"));
    final long first = create(request()).getKey();
    final long another = create(request().setTenantId("another")).getKey();
    final long refund = create(request().setJobType("refund")).getKey();
    assertThat(state.getJobState().getJob(first).getSubscriptionKey()).isEqualTo(42);
    assertThat(state.getJobState().getJob(another).getSubscriptionKey()).isEqualTo(43);
    assertThat(state.getJobState().getJob(refund).getSubscriptionKey()).isEqualTo(44);
    assertThat(subscriptionJobs(42)).containsExactly(first);
    assertThat(subscriptionJobs(43)).containsExactly(another);
    assertThat(subscriptionJobs(44)).containsExactly(refund);
    store.getCurrentTransaction().commit();
    storage.close();
    open();
    final long next = create(request()).getKey();
    assertThat(state.getJobState().getJob(next).getSubscriptionKey()).isEqualTo(42);
    assertThat(subscriptionJobs(42)).containsExactly(first, next);
  }

  @Test
  void rejectsCreationUntilMatchingSubscriptionExists() {
    state.getJobSubscriptionState().delete(42);
    final var rejected = create(request());
    assertThat(rejected.metadata().getRejectionType()).isEqualTo(RejectionType.NOT_FOUND);
    assertThat(rejected.metadata().getRejectionReason())
        .isEqualTo("No active job subscription for job type 'charge' and tenant 'tenant'");
    assertThat(keys.get()).isEqualTo(100);
    assertThat(state.getJobState().getJob(101)).isNull();
    assertThat(state.getVariableState().getVariables(101)).isEmpty();
    assertThat(writers.writtenRecords())
        .filteredOn(record -> record.metadata().getRecordType() == RecordType.EVENT)
        .isEmpty();
    state
        .getJobSubscriptionState()
        .create(42, new JobSubscriptionRecord().setJobType("charge").setTenantId("tenant"));
    final long associated = create(request()).getKey();
    assertThat(associated).isEqualTo(101);
    assertThat(state.getJobState().getJob(associated).getSubscriptionKey()).isEqualTo(42);
    assertThat(subscriptionJobs(42)).containsExactly(associated);
  }

  @ParameterizedTest
  @ValueSource(strings = {"type", "tenant"})
  void rejectsCreationWithoutMatchingTypeAndTenant(final String selector) {
    final var value = request();
    if (selector.equals("type")) {
      value.setJobType("missing");
    } else {
      value.setTenantId("another");
    }
    final var rejected = create(value);
    assertThat(rejected.metadata().getRejectionType()).isEqualTo(RejectionType.NOT_FOUND);
    assertThat(rejected.metadata().getRejectionReason())
        .contains(value.getType(), value.getTenantId());
    assertThat(keys.get()).isEqualTo(100);
    assertThat(state.getJobState().getJob(101)).isNull();
    assertThat(state.getVariableState().getVariables(101)).isEmpty();
    assertThat(writers.writtenRecords())
        .filteredOn(record -> record.metadata().getRecordType() == RecordType.EVENT)
        .isEmpty();
  }

  @Test
  void rejectsCreationByTypeAfterSubscriptionDeletionAndRecovery() throws Exception {
    state.getJobSubscriptionState().delete(42);
    store.getCurrentTransaction().commit();
    storage.close();
    open();

    final var rejected = create(request());
    assertThat(rejected.metadata().getRejectionType()).isEqualTo(RejectionType.NOT_FOUND);
    assertThat(keys.get()).isEqualTo(100);
    assertThat(state.getJobState().getJob(101)).isNull();
    assertThat(state.getVariableState().getVariables(101)).isEmpty();
    assertThat(writers.writtenRecords())
        .filteredOn(record -> record.metadata().getRecordType() == RecordType.EVENT)
        .isEmpty();
  }

  private List<Long> subscriptionJobs(final long subscriptionKey) {
    final List<Long> jobs = new ArrayList<>();
    state
        .getJobState()
        .forEachJobs(
            subscriptionKey,
            (key, ignored) -> {
              jobs.add(key);
              return true;
            });
    return jobs;
  }

  @Test
  void repeatedCreationCreatesIndependentJobsAndVariables() {
    final long first = create(request()).getKey();
    final long second = create(request()).getKey();
    assertThat(second).isGreaterThan(first);
    assertThat(state.getJobState().getJob(first)).isNotNull();
    assertThat(state.getJobState().getJob(second)).isNotNull();
    assertThat(state.getVariableState().getVariables(first)).containsEntry("amount", 7);
    assertThat(state.getVariableState().getVariables(second)).containsEntry("amount", 7);
  }

  @Test
  void mergesTemplateVariablesWithRequestKeysTakingPrecedenceAcrossRecovery() throws Exception {
    final String template =
        "{\"amount\":42,\"currency\":\"USD\",\"nested\":{\"template\":true},\"nullable\":\"default\"}";
    state
        .getJobSubscriptionState()
        .create(
            42,
            new JobSubscriptionRecord()
                .setJobType("charge")
                .setTenantId("tenant")
                .setVariables(template));
    final var value =
        request()
            .setVariablesJson("{\"amount\":7,\"nested\":{\"request\":true},\"nullable\":null}");
    final long key = create(value).getKey();
    final var expected = new java.util.LinkedHashMap<String, Object>();
    expected.put("amount", 7);
    expected.put("currency", "USD");
    expected.put("nested", Map.of("request", true));
    expected.put("nullable", null);
    assertThat(state.getJobState().getJob(key).getVariables()).isEqualTo(expected);
    assertThat(state.getVariableState().getVariables(key)).isEqualTo(expected);
    assertThat(state.getJobSubscriptionState().get(42).getVariables()).isEqualTo(template);
    assertThat(value.getVariables()).doesNotContainKey("currency");
    store.getCurrentTransaction().commit();
    storage.close();
    open();
    assertThat(state.getJobState().getJob(key).getVariables()).isEqualTo(expected);
    assertThat(state.getVariableState().getVariables(key)).isEqualTo(expected);
    final long inherited = create(request().setVariablesJson(null)).getKey();
    assertThat(state.getJobState().getJob(inherited).getVariables())
        .isEqualTo(new JobRecord().setVariablesJson(template).getVariables());
    assertThat(state.getVariableState().getVariables(inherited))
        .isEqualTo(state.getJobState().getJob(inherited).getVariables());
  }

  @Test
  void repeatedRequestsCreateIndependentJobsIncludingAfterRecovery() throws Exception {
    final var ordinary = request();
    final long first = create(ordinary).getKey();
    final long second = create(ordinary).getKey();
    assertThat(second).isGreaterThan(first);
    store.getCurrentTransaction().commit();
    storage.close();
    open();
    final long third = create(ordinary.setVariablesMap(Map.of("amount", 8))).getKey();
    assertThat(third).isGreaterThan(second);
    assertThat(state.getVariableState().getVariables(third)).containsEntry("amount", 8);
  }

  @ParameterizedTest
  @ValueSource(strings = {"type", "variables", "retries", "priority"})
  void subsequentCreationCanChangeJobParameters(final String field) {
    state
        .getJobSubscriptionState()
        .create(44, new JobSubscriptionRecord().setJobType("refund").setTenantId("tenant"));
    final long first = create(request()).getKey();
    final var other = request();
    switch (field) {
      case "type" -> other.setJobType("refund");
      case "variables" -> other.setVariablesMap(Map.of("amount", 8));
      case "retries" -> other.setRetries(5);
      case "priority" -> other.setPriority(1);
      default -> throw new AssertionError(field);
    }
    final var response = create(other);
    final long second = response.getKey();
    assertThat(response.metadata().getIntent()).isEqualTo(JobIntent.CREATED);
    assertThat(second).isGreaterThan(first);
    final var created = state.getJobState().getJob(second);
    assertThat(created.getType()).isEqualTo(other.getType());
    assertThat(created.getRetries()).isEqualTo(other.getRetries());
    assertThat(created.getPriority()).isEqualTo(other.getPriority());
    assertThat(state.getVariableState().getVariables(second)).isEqualTo(other.getVariables());
    assertThat(state.getVariableState().getVariables(first)).containsEntry("amount", 7);
  }

  @Test
  void createsJobsForDifferentTenants() {
    state
        .getJobSubscriptionState()
        .create(43, new JobSubscriptionRecord().setJobType("charge").setTenantId("another"));
    assertThat(create(request()).getKey()).isEqualTo(101);
    assertThat(create(request().setTenantId("another")).getKey()).isGreaterThan(101);
  }

  @Test
  void createsNewJobAfterCompletionAndRecovery() throws Exception {
    final long first = create(request()).getKey();
    BrokerEventAppliers.create(state)
        .applyState(first, JobIntent.COMPLETED, state.getJobState().getJob(first), 1);
    store.getCurrentTransaction().commit();
    storage.close();
    open();
    final long second = create(request()).getKey();
    assertThat(second).isGreaterThan(first);
    assertThat(state.getJobState().getJob(first)).isNull();
    assertThat(state.getJobState().getJob(second)).isNotNull();
    assertThat(state.getVariableState().getVariables(second)).containsEntry("amount", 7);
  }

  @Test
  void rejectsCreationWhenSubscriptionWasDeletedAfterRecovery() throws Exception {
    state
        .getJobSubscriptionState()
        .create(42, new JobSubscriptionRecord().setJobType("charge").setTenantId("tenant"));
    final var value = request();
    final long created = create(value).getKey();
    BrokerEventAppliers.create(state)
        .applyState(created, JobIntent.COMPLETED, state.getJobState().getJob(created), 1);
    state.getJobSubscriptionState().delete(42);
    store.getCurrentTransaction().commit();
    storage.close();
    open();
    final long highWater = keys.get();
    assertThat(create(value).metadata().getRejectionType()).isEqualTo(RejectionType.NOT_FOUND);
    assertThat(keys.get()).isEqualTo(highWater);
    assertThat(state.getJobState().getJob(created)).isNull();
    assertThat(state.getJobSubscriptionState().get(42)).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = {"[]", "null", "{broken", "{} {}"})
  void rejectsInvalidVariablesWithoutCreatingAJob(final String json) {
    assertThat(create(request().setVariablesJson(json)).metadata().getRejectionType())
        .isEqualTo(RejectionType.INVALID_ARGUMENT);
    assertThat(state.getJobState().getJob(101)).isNull();
    assertThat(state.getVariableState().getVariables(101)).isEmpty();
    assertThat(keys.get()).isEqualTo(100);
  }

  @Test
  void rollsBackJobAndVariablesTogether() throws Exception {
    store.getCurrentTransaction().commit();
    store.getCurrentTransaction();
    create(request());
    store.getCurrentTransaction().rollback();
    store.getCurrentTransaction();
    assertThat(state.getJobState().getJob(101)).isNull();
    assertThat(state.getVariableState().getVariables(101)).isEmpty();
    assertThat(create(request()).metadata().getIntent()).isEqualTo(JobIntent.CREATED);
  }
}
