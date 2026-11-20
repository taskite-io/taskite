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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.taskite.protocol.impl.encoding.MsgPackConverter;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.broker.JobRecordValue;
import io.taskite.util.BufferUtil;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JobRecord extends UnifiedRecordValue implements JobRecordValue {
  private long subscriptionKey;

  public long getSubscriptionKey() {
    return subscriptionKey;
  }

  public JobRecord setSubscriptionKey(final long value) {
    subscriptionKey = value;
    return this;
  }

  private String jobType = "";
  private String worker = "";
  private String workerId = "";
  private long workerLeaseToken;

  @Override
  public String getWorkerId() {
    return workerId;
  }

  public JobRecord setWorkerId(String value) {
    workerId = value == null ? "" : value;
    return this;
  }

  @Override
  public long getWorkerLeaseToken() {
    return workerLeaseToken;
  }

  public JobRecord setWorkerLeaseToken(long value) {
    workerLeaseToken = value;
    return this;
  }

  private int retries = 3;
  private int priority = 50;
  private long retryBackoff;
  private long recurringTime;
  private long deadline;
  private long timeout;
  private String errorMessage = "";
  private String errorCode = "";
  private DirectBuffer variables;
  private String tenantId = DEFAULT_TENANT_IDENTIFIER;

  @Override
  @JsonProperty("jobType")
  public String getType() {
    return jobType;
  }

  @Override
  public String getWorker() {
    return worker;
  }

  @Override
  public int getRetries() {
    return retries;
  }

  @Override
  public int getPriority() {
    return priority;
  }

  @Override
  public long getRetryBackoff() {
    return retryBackoff;
  }

  @Override
  public long getRecurringTime() {
    return recurringTime;
  }

  @Override
  public long getDeadline() {
    return deadline;
  }

  @Override
  public long getTimeout() {
    return timeout;
  }

  @Override
  public String getErrorMessage() {
    return errorMessage;
  }

  @Override
  public String getErrorCode() {
    return errorCode;
  }

  @Override
  @JsonIgnore
  public Map<String, Object> getVariables() {
    return MsgPackConverter.convertJsonToMap(getVariablesJson());
  }

  @Override
  public String getTenantId() {
    return tenantId;
  }

  public JobRecord setJobType(String jobType) {
    this.jobType = jobType;
    return this;
  }

  @JsonProperty("jobType")
  public JobRecord setJsonJobType(final String jobType) {
    this.jobType = jobType;
    return this;
  }

  public JobRecord setWorker(String worker) {
    this.worker = worker;
    return this;
  }

  public JobRecord setRetries(int retries) {
    this.retries = retries;
    return this;
  }

  public JobRecord setPriority(int priority) {
    this.priority = priority;
    return this;
  }

  public JobRecord setRetryBackoff(long retryBackoff) {
    this.retryBackoff = retryBackoff;
    return this;
  }

  public JobRecord setRecurringTime(long recurringTime) {
    this.recurringTime = recurringTime;
    return this;
  }

  public JobRecord setDeadline(long deadline) {
    this.deadline = deadline;
    return this;
  }

  public JobRecord setTimeout(long timeout) {
    this.timeout = timeout;
    return this;
  }

  public JobRecord setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
    return this;
  }

  public JobRecord setErrorCode(String errorCode) {
    this.errorCode = errorCode;
    return this;
  }

  public JobRecord setVariables(DirectBuffer variables) {
    this.variables = variables;
    return this;
  }

  public JobRecord setVariablesMap(Map<String, Object> variables) {
    return setVariablesJson(MsgPackConverter.convertObjectToJson(variables));
  }

  @JsonProperty("variables")
  public String getVariablesJson() {
    if (variables == null || variables.capacity() == 0) {
      return "{}";
    }

    return BufferUtil.bufferAsString(variables);
  }

  @JsonProperty("variables")
  public JobRecord setVariablesJson(final String variablesJson) {
    final String normalized =
        variablesJson == null || variablesJson.isBlank() ? "{}" : variablesJson;
    variables = new UnsafeBuffer(normalized.getBytes(StandardCharsets.UTF_8));
    return this;
  }

  public JobRecord setTenantId(String tenantId) {
    this.tenantId = tenantId;
    return this;
  }

  public JobRecord copy() {
    return MsgPackConverter.copy(this);
  }
}
