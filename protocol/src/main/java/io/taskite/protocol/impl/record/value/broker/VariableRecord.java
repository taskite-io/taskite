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
package io.taskite.protocol.impl.record.value.broker;

import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.broker.TenantOwned;

/** A single variable value owned by one job. */
public final class VariableRecord extends UnifiedRecordValue implements TenantOwned {

  private long jobKey = -1;
  private String tenantId = DEFAULT_TENANT_IDENTIFIER;
  private String name = "";
  private Object value;

  public long getJobKey() {
    return jobKey;
  }

  public VariableRecord setJobKey(final long jobKey) {
    this.jobKey = jobKey;
    return this;
  }

  @Override
  public String getTenantId() {
    return tenantId;
  }

  public VariableRecord setTenantId(final String tenantId) {
    this.tenantId = tenantId;
    return this;
  }

  public String getName() {
    return name;
  }

  public VariableRecord setName(final String name) {
    this.name = name;
    return this;
  }

  public Object getValue() {
    return value;
  }

  public VariableRecord setValue(final Object value) {
    this.value = value;
    return this;
  }
}
