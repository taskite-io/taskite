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
package io.taskite.protocol.impl.record.value.placementdriver.commanddistribution;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.broker.TenantOwned;

/** A task template distributed to all target partitions. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class JobSubscriptionRecord extends UnifiedRecordValue implements TenantOwned {
  private String clusterId = "";
  private String tenantId = TenantOwned.DEFAULT_TENANT_IDENTIFIER;
  private String name = "";
  private String jobType = "";
  private String variables = "{}";
  private int retries = 3;
  private int priority = 50;

  public String getClusterId() {
    return clusterId;
  }

  public JobSubscriptionRecord setClusterId(final String value) {
    clusterId = value == null ? "" : value;
    return this;
  }

  @Override
  public String getTenantId() {
    return tenantId;
  }

  public JobSubscriptionRecord setTenantId(final String value) {
    tenantId = value == null || value.isBlank() ? TenantOwned.DEFAULT_TENANT_IDENTIFIER : value;
    return this;
  }

  public String getName() {
    return name;
  }

  public JobSubscriptionRecord setName(final String value) {
    name = value == null ? "" : value;
    return this;
  }

  public String getJobType() {
    return jobType;
  }

  public JobSubscriptionRecord setJobType(final String value) {
    jobType = value == null ? "" : value;
    return this;
  }

  public String getVariables() {
    return variables;
  }

  public JobSubscriptionRecord setVariables(final String value) {
    variables = value == null || value.isBlank() ? "{}" : value;
    return this;
  }

  public int getRetries() {
    return retries;
  }

  public JobSubscriptionRecord setRetries(final int value) {
    retries = value;
    return this;
  }

  public int getPriority() {
    return priority;
  }

  public JobSubscriptionRecord setPriority(final int value) {
    priority = value;
    return this;
  }
}
