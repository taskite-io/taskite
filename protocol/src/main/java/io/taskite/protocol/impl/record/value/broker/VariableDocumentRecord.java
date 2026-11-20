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

import io.taskite.protocol.impl.encoding.MsgPackConverter;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.broker.TenantOwned;
import io.taskite.protocol.record.value.broker.VariableDocumentRecordValue;
import java.util.LinkedHashMap;
import java.util.Map;

/** Variables submitted as one atomic update for a job. */
public final class VariableDocumentRecord extends UnifiedRecordValue
    implements VariableDocumentRecordValue {

  private long jobKey = -1;
  private Map<String, Object> variables = new LinkedHashMap<>();
  private String tenantId = TenantOwned.DEFAULT_TENANT_IDENTIFIER;

  @Override
  public long getJobKey() {
    return jobKey;
  }

  public VariableDocumentRecord setJobKey(final long jobKey) {
    this.jobKey = jobKey;
    return this;
  }

  @Override
  public Map<String, Object> getVariables() {
    return variables;
  }

  public VariableDocumentRecord setVariables(final Map<String, Object> variables) {
    this.variables = variables == null ? new LinkedHashMap<>() : new LinkedHashMap<>(variables);
    return this;
  }

  @Override
  public String getTenantId() {
    return tenantId;
  }

  public VariableDocumentRecord setTenantId(final String tenantId) {
    this.tenantId =
        tenantId == null || tenantId.isBlank() ? TenantOwned.DEFAULT_TENANT_IDENTIFIER : tenantId;
    return this;
  }

  public VariableDocumentRecord copy() {
    return MsgPackConverter.copy(this);
  }
}
