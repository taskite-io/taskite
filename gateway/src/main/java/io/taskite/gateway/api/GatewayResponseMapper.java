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
package io.taskite.gateway.api;

import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.gateway.protocol.ActivateJobsResponse;
import io.taskite.gateway.protocol.ActivatedJob;
import io.taskite.gateway.protocol.CancelJobResponse;
import io.taskite.gateway.protocol.CompleteJobResponse;
import io.taskite.gateway.protocol.CreateJobResponse;
import io.taskite.gateway.protocol.CreateJobSubscriptionResponse;
import io.taskite.gateway.protocol.CreateTimerSubscriptionResponse;
import io.taskite.gateway.protocol.DeleteJobSubscriptionResponse;
import io.taskite.gateway.protocol.DeleteTimerSubscriptionResponse;
import io.taskite.gateway.protocol.FailJobResponse;
import io.taskite.gateway.protocol.QueryIncidentResponse;
import io.taskite.gateway.protocol.ResolveIncidentResponse;
import io.taskite.gateway.protocol.SetVariablesResponse;
import io.taskite.gateway.protocol.UpdateJobPriorityResponse;
import io.taskite.gateway.protocol.UpdateJobRetriesResponse;
import io.taskite.gateway.protocol.UpdateJobTimeoutResponse;
import io.taskite.placementdriver.protocol.DistributeCommandResponse;
import io.taskite.protocol.impl.record.value.broker.IncidentRecord;
import io.taskite.protocol.impl.record.value.broker.JobBatchRecord;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.impl.record.value.broker.VariableDocumentRecord;

public final class GatewayResponseMapper {

  private GatewayResponseMapper() {}

  public static CreateTimerSubscriptionResponse toCreateTimerSubscriptionResponse(
      final io.taskite.placementdriver.protocol.CreateTimerSubscriptionResponse response) {
    return CreateTimerSubscriptionResponse.newBuilder()
        .setKey(response.getTimerSubscriptionKey())
        .setDueDate(response.getDueDate())
        .build();
  }

  public static DeleteTimerSubscriptionResponse toDeleteTimerSubscriptionResponse(
      final io.taskite.placementdriver.protocol.DeleteTimerSubscriptionResponse response) {
    return DeleteTimerSubscriptionResponse.newBuilder()
        .setKey(response.getTimerSubscriptionKey())
        .build();
  }

  public static CancelJobResponse toCancelJobResponse(final BrokerResponse<JobRecord> response) {
    return CancelJobResponse.newBuilder().setJobKey(response.getKey()).build();
  }

  public static CompleteJobResponse toCompleteJobResponse(
      final BrokerResponse<JobRecord> response) {
    final JobRecord record = response.getResponse();
    return CompleteJobResponse.newBuilder()
        .setJobKey(response.getKey())
        .setJobType(nullToEmpty(record == null ? null : record.getType()))
        .setWorker(nullToEmpty(record == null ? null : record.getWorker()))
        .setWorkerId(nullToEmpty(record == null ? null : record.getWorkerId()))
        .build();
  }

  public static FailJobResponse toFailJobResponse(final BrokerResponse<JobRecord> ignored) {
    return FailJobResponse.getDefaultInstance();
  }

  public static UpdateJobTimeoutResponse toUpdateJobTimeoutResponse(
      final BrokerResponse<JobRecord> ignored) {
    return UpdateJobTimeoutResponse.getDefaultInstance();
  }

  public static UpdateJobPriorityResponse toUpdateJobPriorityResponse(
      final BrokerResponse<JobRecord> ignored) {
    return UpdateJobPriorityResponse.getDefaultInstance();
  }

  public static ActivateJobsResponse toActivateJobsResponse(
      final BrokerResponse<JobBatchRecord> response) {
    final var result = ActivateJobsResponse.newBuilder();
    for (final var activated : response.getResponse().getJobs()) {
      result.addJobs(toActivatedJob(activated.key(), activated.record()));
    }
    return result.build();
  }

  public static ActivatedJob toActivatedJob(final BrokerResponse<JobRecord> response) {
    return toActivatedJob(response.getKey(), response.getResponse());
  }

  public static QueryIncidentResponse toQueryIncidentResponse(
      final BrokerResponse<IncidentRecord> response) {
    final IncidentRecord record = response.getResponse();
    return QueryIncidentResponse.newBuilder()
        .setKey(response.getKey())
        .setErrorMessage(nullToEmpty(record.getErrorMessage()))
        .setErrorType(record.getErrorType() == null ? "" : record.getErrorType().name())
        .setJobKey(record.getJobKey())
        .setTenantId(nullToEmpty(record.getTenantId()))
        .build();
  }

  public static ResolveIncidentResponse toResolveIncidentResponse(
      final BrokerResponse<IncidentRecord> ignored) {
    return ResolveIncidentResponse.getDefaultInstance();
  }

  public static SetVariablesResponse toSetVariablesResponse(
      final BrokerResponse<VariableDocumentRecord> response) {
    return SetVariablesResponse.newBuilder().setJobKey(response.getResponse().getJobKey()).build();
  }

  private static ActivatedJob toActivatedJob(final long key, final JobRecord job) {
    return ActivatedJob.newBuilder()
        .setKey(key)
        .setType(nullToEmpty(job.getType()))
        .setSubscriptionKey(job.getSubscriptionKey())
        .setWorker(nullToEmpty(job.getWorker()))
        .setWorkerId(nullToEmpty(job.getWorkerId()))
        .setWorkerLeaseToken(job.getWorkerLeaseToken())
        .setRetries(job.getRetries())
        .setDeadline(job.getDeadline())
        .setPriority(job.getPriority())
        .setVariables(nullToEmpty(job.getVariablesJson()))
        .setTenantId(nullToEmpty(job.getTenantId()))
        .build();
  }

  public static CreateJobResponse toCreateJobResponse(final BrokerResponse<JobRecord> response) {
    final var job = response.getResponse();
    return CreateJobResponse.newBuilder()
        .setJobKey(response.getKey())
        .setJobType(job.getType())
        .setTenantId(job.getTenantId())
        .setSubscriptionKey(job.getSubscriptionKey())
        .build();
  }

  public static CreateJobSubscriptionResponse toCreateJobSubscriptionResponse(
      final DistributeCommandResponse response) {
    return CreateJobSubscriptionResponse.newBuilder().setKey(response.getDistributionKey()).build();
  }

  public static DeleteJobSubscriptionResponse toDeleteJobSubscriptionResponse(final long key) {
    return DeleteJobSubscriptionResponse.newBuilder().setKey(key).build();
  }

  public static UpdateJobRetriesResponse toUpdateJobRetriesResponse(
      final BrokerResponse<JobRecord> ignored) {
    return UpdateJobRetriesResponse.getDefaultInstance();
  }

  private static String nullToEmpty(final String value) {
    return value == null ? "" : value;
  }
}
