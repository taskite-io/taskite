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
package io.taskite.protocol.impl.record.value.placementdriver.partitionchange;

import io.taskite.protocol.impl.record.value.broker.BrokerPartitionConfigurationRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.topology.BrokerEndpoint;

/** Concrete internal RPC result and persisted execution observation. */
public final class PartitionExecutionRecord extends UnifiedRecordValue {
  public enum Phase {
    READY,
    APPLY,
    BOOTSTRAP,
    READ,
    INSTALL
  }

  public record Plan(
      String clusterId,
      int partitionId,
      String changeId,
      String status,
      int operationIndex,
      long generation,
      long authorityIndex,
      String successor) {}

  public record Schedule(
      Plan plan,
      Phase phase,
      long attempt,
      long dueAt,
      long deadline,
      BrokerEndpoint endpoint,
      BrokerPartitionConfigurationRecord source) {
    public Schedule ready(final long due) {
      return new Schedule(plan, Phase.READY, attempt, due, 0, null, null);
    }

    public Schedule observe(final long due) {
      return new Schedule(plan, phase, attempt, due, deadline, endpoint, source);
    }
  }

  private Schedule schedule;
  private boolean success;
  private PlanBlockedReason failure = PlanBlockedReason.NONE;
  private BrokerPartitionConfigurationRecord configuration;

  public Schedule getSchedule() {
    return schedule;
  }

  public PartitionExecutionRecord setSchedule(final Schedule value) {
    schedule = value;
    return this;
  }

  public boolean isSuccess() {
    return success;
  }

  public PartitionExecutionRecord setSuccess(final boolean value) {
    success = value;
    return this;
  }

  public PlanBlockedReason getFailure() {
    return failure;
  }

  public PartitionExecutionRecord setFailure(final PlanBlockedReason value) {
    failure = value;
    return this;
  }

  public BrokerPartitionConfigurationRecord getConfiguration() {
    return configuration;
  }

  public PartitionExecutionRecord setConfiguration(final BrokerPartitionConfigurationRecord value) {
    configuration = value;
    return this;
  }
}
