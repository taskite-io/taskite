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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.taskite.broker.client.request.BrokerActivateJobsRequest;
import io.taskite.broker.client.request.BrokerCancelJobRequest;
import io.taskite.broker.client.request.BrokerCompleteJobRequest;
import io.taskite.broker.client.request.BrokerCreateJobRequest;
import io.taskite.broker.client.request.BrokerFailJobRequest;
import io.taskite.broker.client.request.BrokerQueryIncidentRequest;
import io.taskite.broker.client.request.BrokerQueryJobRequest;
import io.taskite.broker.client.request.BrokerResolveIncidentRequest;
import io.taskite.broker.client.request.BrokerSetVariablesRequest;
import io.taskite.broker.client.request.BrokerUpdateJobPriorityRequest;
import io.taskite.broker.client.request.BrokerUpdateJobRetriesRequest;
import io.taskite.broker.client.request.BrokerUpdateJobTimeoutRequest;
import io.taskite.gateway.protocol.ActivateJobsRequest;
import io.taskite.gateway.protocol.CancelJobRequest;
import io.taskite.gateway.protocol.CompleteJobRequest;
import io.taskite.gateway.protocol.CreateJobRequest;
import io.taskite.gateway.protocol.CreateJobSubscriptionRequest;
import io.taskite.gateway.protocol.CreateTimerSubscriptionRequest;
import io.taskite.gateway.protocol.DeleteJobSubscriptionRequest;
import io.taskite.gateway.protocol.DeleteTimerSubscriptionRequest;
import io.taskite.gateway.protocol.FailJobRequest;
import io.taskite.gateway.protocol.QueryIncidentRequest;
import io.taskite.gateway.protocol.QueryJobRequest;
import io.taskite.gateway.protocol.ResolveIncidentRequest;
import io.taskite.gateway.protocol.SetVariablesRequest;
import io.taskite.gateway.protocol.UpdateJobPriorityRequest;
import io.taskite.gateway.protocol.UpdateJobRetriesRequest;
import io.taskite.gateway.protocol.UpdateJobTimeoutRequest;
import io.taskite.placementdriver.protocol.DistributeCommandRequest;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class GatewayRequestMapper {
  private static final long DEFAULT_JOB_ACTIVATION_TIMEOUT_MILLIS = 30_000;
  private static final int DEFAULT_MAX_JOBS_TO_ACTIVATE = 100;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ObjectReader VARIABLES_READER =
      OBJECT_MAPPER
          .readerFor(new TypeReference<Map<String, Object>>() {})
          .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

  private GatewayRequestMapper() {}

  public static DistributeCommandRequest toCreateJobSubscriptionRequest(
      final CreateJobSubscriptionRequest request) {
    parseVariables(request.getVariables());
    final var subscription =
        io.taskite.placementdriver.protocol.CreateJobSubscriptionRequest.newBuilder()
            .setName(request.getName())
            .setJobType(request.getJobType())
            .setVariables(request.getVariables())
            .setRetries(request.hasRetries() ? request.getRetries() : 3)
            .setPriority(request.hasPriority() ? request.getPriority() : 50);
    return DistributeCommandRequest.newBuilder().setCreateJobSubscription(subscription).build();
  }

  public static io.taskite.placementdriver.protocol.CreateTimerSubscriptionRequest
      toCreateTimerSubscriptionRequest(final CreateTimerSubscriptionRequest request) {
    parseVariables(request.getVariables());
    return io.taskite.placementdriver.protocol.CreateTimerSubscriptionRequest.newBuilder()
        .setName(request.getName())
        .setType(
            switch (request.getType()) {
              case DURATION -> io.taskite.placementdriver.protocol.TimerSubscriptionType.DURATION;
              case CYCLE -> io.taskite.placementdriver.protocol.TimerSubscriptionType.CYCLE;
              case CRON -> io.taskite.placementdriver.protocol.TimerSubscriptionType.CRON;
              default ->
                  io.taskite.placementdriver.protocol.TimerSubscriptionType.TIMER_TYPE_UNSPECIFIED;
            })
        .setExpression(request.getExpression())
        .addAllJobTypes(request.getJobTypesList())
        .setVariables(request.getVariables())
        .build();
  }

  public static io.taskite.placementdriver.protocol.DeleteTimerSubscriptionRequest
      toDeleteTimerSubscriptionRequest(final DeleteTimerSubscriptionRequest request) {
    if (request.getKey() <= 0) {
      throw new IllegalArgumentException("key must be positive");
    }
    return io.taskite.placementdriver.protocol.DeleteTimerSubscriptionRequest.newBuilder()
        .setTimerSubscriptionKey(request.getKey())
        .build();
  }

  public static DistributeCommandRequest toDeleteJobSubscriptionRequest(
      final DeleteJobSubscriptionRequest request) {
    if (request.getKey() <= 0) {
      throw new IllegalArgumentException("key must be positive");
    }
    return DistributeCommandRequest.newBuilder()
        .setDeleteJobSubscription(
            io.taskite.placementdriver.protocol.DeleteJobSubscriptionRequest.newBuilder()
                .setSubscriptionKey(request.getKey()))
        .build();
  }

  public static BrokerCreateJobRequest toCreateJobRequest(final CreateJobRequest request) {
    if (request.getJobType().isBlank()) {
      throw new IllegalArgumentException("Job type is required");
    }
    return new BrokerCreateJobRequest(
        "",
        0,
        new JobRecord()
            .setJobType(request.getJobType())
            .setVariablesMap(parseVariables(request.getVariables()))
            .setRetries(request.hasRetries() ? request.getRetries() : 3)
            .setPriority(request.hasPriority() ? request.getPriority() : 50));
  }

  public static BrokerUpdateJobRetriesRequest toUpdateJobRetriesRequest(
      final UpdateJobRetriesRequest request) {
    return new BrokerUpdateJobRetriesRequest(request.getJobKey(), request.getRetries());
  }

  private static Map<String, Object> parseVariables(final String variables) {
    if (variables == null || variables.isBlank()) {
      return Map.of();
    }
    try {
      final Map<String, Object> parsed = VARIABLES_READER.readValue(variables);
      if (parsed == null) {
        throw new IllegalArgumentException("variables must be a JSON object");
      }
      return parsed;
    } catch (final JsonProcessingException error) {
      throw new IllegalArgumentException("variables must be a valid JSON object", error);
    }
  }

  public static BrokerActivateJobsRequest toActivateJobsRequest(final ActivateJobsRequest request) {
    final long timeout =
        request.getTimeout() == 0 ? DEFAULT_JOB_ACTIVATION_TIMEOUT_MILLIS : request.getTimeout();
    final int maxJobs =
        request.getMaxJobsToActivate() == 0
            ? DEFAULT_MAX_JOBS_TO_ACTIVATE
            : request.getMaxJobsToActivate();
    if (request.getType().isBlank() || timeout <= 0 || maxJobs <= 0) {
      throw new IllegalArgumentException(
          "type is required; timeout and maxJobsToActivate must not be negative");
    }
    if (request.getTenantIdsList().stream().anyMatch(String::isBlank)) {
      throw new IllegalArgumentException("tenantIds must not contain blank IDs");
    }
    return new BrokerActivateJobsRequest(request.getType())
        .setWorker(request.getWorker())
        .setWorkerId(request.getWorkerId())
        .setWorkerLeaseToken(request.getWorkerLeaseToken())
        .setTimeout(timeout)
        .setMaxJobsToActivate(maxJobs)
        .setVariables(request.getFetchVariableList());
  }

  public static BrokerCompleteJobRequest toCompleteJobRequest(
      final CompleteJobRequest grpcRequest) {
    return new BrokerCompleteJobRequest(
            grpcRequest.getJobKey(), ensureJsonBuffer(grpcRequest.getVariables()))
        .setWorkerSession(grpcRequest.getWorkerId(), grpcRequest.getWorkerLeaseToken());
  }

  public static BrokerFailJobRequest toFailJobRequest(final FailJobRequest grpcRequest) {
    return new BrokerFailJobRequest(
            grpcRequest.getJobKey(),
            grpcRequest.getRetries(),
            grpcRequest.getErrorMessage(),
            grpcRequest.getRetryBackOff(),
            parseVariables(grpcRequest.getVariables()))
        .setWorkerSession(grpcRequest.getWorkerId(), grpcRequest.getWorkerLeaseToken());
  }

  public static BrokerUpdateJobTimeoutRequest toUpdateJobTimeoutRequest(
      final UpdateJobTimeoutRequest grpcRequest) {
    return new BrokerUpdateJobTimeoutRequest(grpcRequest.getJobKey(), grpcRequest.getTimeout())
        .setWorkerSession(grpcRequest.getWorkerId(), grpcRequest.getWorkerLeaseToken());
  }

  public static BrokerUpdateJobPriorityRequest toUpdateJobPriorityRequest(
      final UpdateJobPriorityRequest grpcRequest) {
    return new BrokerUpdateJobPriorityRequest(grpcRequest.getJobKey(), grpcRequest.getPriority());
  }

  public static BrokerCancelJobRequest toCancelJobRequest(final CancelJobRequest grpcRequest) {
    return new BrokerCancelJobRequest(grpcRequest.getJobKey());
  }

  public static BrokerQueryJobRequest toQueryJobRequest(final QueryJobRequest grpcRequest) {
    return new BrokerQueryJobRequest(grpcRequest.getJobKey());
  }

  public static BrokerQueryIncidentRequest toQueryIncidentRequest(
      final QueryIncidentRequest grpcRequest) {
    return new BrokerQueryIncidentRequest(grpcRequest.getJobKey());
  }

  public static BrokerResolveIncidentRequest toResolveIncidentRequest(
      final ResolveIncidentRequest grpcRequest) {
    return new BrokerResolveIncidentRequest(grpcRequest.getIncidentKey());
  }

  public static BrokerSetVariablesRequest toSetVariablesRequest(
      final SetVariablesRequest grpcRequest) {
    return new BrokerSetVariablesRequest(
        grpcRequest.getJobKey(), parseVariables(grpcRequest.getVariables()));
  }

  public static DirectBuffer ensureJsonBuffer(final String value) {
    final String normalized = value == null || value.isBlank() ? "{}" : value;
    parseVariables(normalized);
    return new UnsafeBuffer(normalized.getBytes(StandardCharsets.UTF_8));
  }
}
