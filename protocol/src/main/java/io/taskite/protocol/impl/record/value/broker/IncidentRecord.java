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
import io.taskite.protocol.record.value.broker.ErrorType;
import io.taskite.protocol.record.value.broker.TenantOwned;

/** Persistent recovery point for a job with exhausted retries. */
public final class IncidentRecord extends UnifiedRecordValue implements TenantOwned {
  private String tenantId = DEFAULT_TENANT_IDENTIFIER;
  private long jobKey = -1;
  private ErrorType errorType;
  private String errorMessage = "";

  @Override
  public String getTenantId() {
    return tenantId;
  }

  public IncidentRecord setTenantId(final String value) {
    tenantId = value;
    return this;
  }

  public long getJobKey() {
    return jobKey;
  }

  public IncidentRecord setJobKey(final long value) {
    jobKey = value;
    return this;
  }

  public ErrorType getErrorType() {
    return errorType;
  }

  public IncidentRecord setErrorType(final ErrorType value) {
    errorType = value;
    return this;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public IncidentRecord setErrorMessage(final String value) {
    errorMessage = value;
    return this;
  }
}
