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
package io.taskite.engine.state;

import static org.assertj.core.api.Assertions.assertThat;

import io.taskite.engine.state.job.DbJobSubscriptionState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class DbJobSubscriptionStateTest {
  @TempDir Path directory;
  private PartitionStorage storage;
  private StateStoreProcessingStore store;
  private DbJobSubscriptionState subscriptions;

  @BeforeEach
  void open() {
    storage = new PartitionStorage(directory);
    store = new StateStoreProcessingStore(storage);
    subscriptions = new DbJobSubscriptionState(store);
    store.getCurrentTransaction();
  }

  @AfterEach
  void close() throws Exception {
    store.getCurrentTransaction().rollback();
    storage.close();
  }

  @Test
  void looksUpOnlyTheExactTypeAndTenantPrefix() {
    subscriptions.create(42, subscription("task", "a"));
    subscriptions.create(43, subscription("task", "aa"));
    subscriptions.create(44, subscription("tasks", "a"));
    assertThat(subscriptions.findSubscriptionKey("task", "a")).isEqualTo(42);
    assertThat(subscriptions.findSubscriptionKey("task", "aa")).isEqualTo(43);
    assertThat(subscriptions.findSubscriptionKey("tasks", "a")).isEqualTo(44);
    assertThat(subscriptions.findSubscriptionKey("task", "b")).isZero();
    assertThat(subscriptions.findSubscriptionKey("missing", "a")).isZero();
    assertThat(subscriptions.get(42).getJobType()).isEqualTo("task");
  }

  @Test
  void replayUpdatesAndDeletionMaintainTheIndexAndPermitRecreation() {
    subscriptions.create(42, subscription("task", "a"));
    subscriptions.create(42, subscription("task", "a"));
    subscriptions.create(43, subscription("task", "aa"));
    subscriptions.create(42, subscription("other", "b"));
    assertThat(subscriptions.findSubscriptionKey("task", "a")).isZero();
    assertThat(subscriptions.findSubscriptionKey("other", "b")).isEqualTo(42);
    assertThat(subscriptions.findSubscriptionKey("task", "aa")).isEqualTo(43);
    subscriptions.delete(42);
    subscriptions.delete(42);
    assertThat(subscriptions.get(42)).isNull();
    assertThat(subscriptions.findSubscriptionKey("other", "b")).isZero();
    assertThat(subscriptions.findSubscriptionKey("task", "aa")).isEqualTo(43);
    subscriptions.create(42, subscription("task", "a"));
    assertThat(subscriptions.findSubscriptionKey("task", "a")).isEqualTo(42);
  }

  @Test
  void recordAndLookupIndexRollBackAndRecoverTogether() throws Exception {
    subscriptions.create(42, subscription("task", "a"));
    subscriptions.create(43, subscription("task", "b"));
    store.getCurrentTransaction().commit();
    store.getCurrentTransaction();
    subscriptions.create(42, subscription("other", "c"));
    subscriptions.delete(43);
    assertThat(subscriptions.findSubscriptionKey("task", "a")).isZero();
    assertThat(subscriptions.findSubscriptionKey("other", "c")).isEqualTo(42);
    assertThat(subscriptions.findSubscriptionKey("task", "b")).isZero();
    store.getCurrentTransaction().rollback();
    storage.close();
    open();
    assertThat(subscriptions.get(42).getJobType()).isEqualTo("task");
    assertThat(subscriptions.get(43).getTenantId()).isEqualTo("b");
    assertThat(subscriptions.findSubscriptionKey("task", "a")).isEqualTo(42);
    assertThat(subscriptions.findSubscriptionKey("task", "b")).isEqualTo(43);
    assertThat(subscriptions.findSubscriptionKey("other", "c")).isZero();
    subscriptions.delete(42);
    store.getCurrentTransaction().commit();
    storage.close();
    open();
    assertThat(subscriptions.get(42)).isNull();
    assertThat(subscriptions.findSubscriptionKey("task", "a")).isZero();
    assertThat(subscriptions.findSubscriptionKey("task", "b")).isEqualTo(43);
  }

  private static JobSubscriptionRecord subscription(final String type, final String tenant) {
    return new JobSubscriptionRecord().setJobType(type).setTenantId(tenant);
  }
}
