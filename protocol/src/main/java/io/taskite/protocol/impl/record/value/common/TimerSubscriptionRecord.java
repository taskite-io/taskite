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
package io.taskite.protocol.impl.record.value.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.broker.TenantOwned;
import java.util.List;

/** Independent durable timer definition and the progress of its current occurrence. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class TimerSubscriptionRecord extends UnifiedRecordValue implements TenantOwned {
  private long dueDate;
  private int repetitions = 1;
  private String clusterId = "";
  private String tenantId = TenantOwned.DEFAULT_TENANT_IDENTIFIER;
  private String name = "";
  private TimerType type = TimerType.UNSPECIFIED;
  private String expression = "";
  private List<String> jobTypes = List.of();
  private String variables = "{}";
  private String timeZone = "";
  private long occurrence;
  // Persist the selected partition before sending an RPC; retries must keep the same destination.
  private int partitionId;

  public long getDueDate() {
    return dueDate;
  }

  public TimerSubscriptionRecord setDueDate(final long value) {
    dueDate = value;
    return this;
  }

  public String getName() {
    return name;
  }

  public TimerSubscriptionRecord setName(final String value) {
    name = value == null ? "" : value;
    return this;
  }

  public TimerType getType() {
    return type;
  }

  public TimerSubscriptionRecord setType(final TimerType value) {
    type = value == null ? TimerType.UNSPECIFIED : value;
    return this;
  }

  public String getExpression() {
    return expression;
  }

  public TimerSubscriptionRecord setExpression(final String value) {
    expression = value == null ? "" : value;
    return this;
  }

  public List<String> getJobTypes() {
    return jobTypes;
  }

  public TimerSubscriptionRecord setJobTypes(final List<String> value) {
    jobTypes = value == null ? List.of() : List.copyOf(value);
    return this;
  }

  public String getVariables() {
    return variables;
  }

  public TimerSubscriptionRecord setVariables(final String value) {
    variables = value == null || value.isEmpty() ? "{}" : value;
    return this;
  }

  public String getTimeZone() {
    return timeZone;
  }

  public TimerSubscriptionRecord setTimeZone(final String value) {
    timeZone = value == null ? "" : value;
    return this;
  }

  public long getOccurrence() {
    return occurrence;
  }

  public TimerSubscriptionRecord setOccurrence(final long value) {
    occurrence = value;
    return this;
  }

  public int getRepetitions() {
    return repetitions;
  }

  public TimerSubscriptionRecord setRepetitions(final int value) {
    repetitions = value;
    return this;
  }

  public String getClusterId() {
    return clusterId;
  }

  public TimerSubscriptionRecord setClusterId(final String value) {
    clusterId = value == null ? "" : value;
    return this;
  }

  @Override
  public String getTenantId() {
    return tenantId;
  }

  public TimerSubscriptionRecord setTenantId(final String value) {
    tenantId = value == null || value.isBlank() ? TenantOwned.DEFAULT_TENANT_IDENTIFIER : value;
    return this;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public TimerSubscriptionRecord setPartitionId(final int value) {
    partitionId = value;
    return this;
  }
}
