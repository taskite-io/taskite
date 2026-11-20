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
package io.taskite.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.gateway.api.GatewayResponseMapper;
import io.taskite.gateway.protocol.ActivatedJob;
import io.taskite.gateway.protocol.GatewayOuterClass;
import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.record.value.broker.IncidentRecord;
import io.taskite.protocol.impl.record.value.broker.JobBatchRecord;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.impl.record.value.broker.VariableDocumentRecord;
import io.taskite.protocol.record.value.broker.ErrorType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class GatewayResponseMapperTest {
  @Test
  void protocolDescriptorsExposeOnlySupportedFields() {
    final var gateway = io.taskite.gateway.protocol.GatewayOuterClass.getDescriptor();
    for (final var message : gateway.getMessageTypes()) {
      if (message.getName().endsWith("Response")) {
        assertNull(message.findFieldByName("accepted"), message.getName());
      }
    }
    for (final var message :
        io.taskite.placementdriver.protocol.DistributeCommandResponse.getDescriptor()
            .getFile()
            .getMessageTypes()) {
      if (message.getName().endsWith("Response")) {
        assertNull(message.findFieldByName("accepted"), message.getName());
      }
    }
    assertNull(
        io.taskite.gateway.protocol.CreateJobSubscriptionRequest.getDescriptor()
            .findFieldByName("customHeaders"));
    assertNull(
        io.taskite.placementdriver.protocol.CreateTimerSubscriptionRequest.getDescriptor()
            .findFieldByName("timeZone"));
    assertEquals(41, GatewayResponseMapper.toDeleteJobSubscriptionResponse(41).getKey());
    assertNull(
        io.taskite.gateway.protocol.CreateJobRequest.getDescriptor()
            .findFieldByName("partitionId"));
    assertNull(
        io.taskite.gateway.protocol.CreateJobRequest.getDescriptor()
            .findFieldByName("subscriptionKey"));
    assertNull(
        io.taskite.gateway.protocol.CreateJobRequest.getDescriptor()
            .findFieldByName("customHeaders"));
    assertNull(
        io.taskite.gateway.protocol.ActivatedJob.getDescriptor().findFieldByName("customHeaders"));
    assertNull(
        io.taskite.gateway.protocol.CreateTimerSubscriptionRequest.getDescriptor()
            .findFieldByName("timeZone"));
    assertNull(
        io.taskite.gateway.protocol.DeleteJobSubscriptionResponse.getDescriptor()
            .findFieldByName("distributionKey"));
  }

  @Test
  void mapsTimerCreationAndDeletionWithoutLosingLargeIntegers() {
    final long key = 9007199254740993L;
    final var response =
        GatewayResponseMapper.toCreateTimerSubscriptionResponse(
            io.taskite.placementdriver.protocol.CreateTimerSubscriptionResponse.newBuilder()
                .setTimerSubscriptionKey(key)
                .setDueDate(key + 1000)
                .build());
    assertEquals(key, response.getKey());
    assertEquals(key + 1000, response.getDueDate());
    assertEquals(
        key,
        GatewayResponseMapper.toDeleteTimerSubscriptionResponse(
                io.taskite.placementdriver.protocol.DeleteTimerSubscriptionResponse.newBuilder()
                    .setTimerSubscriptionKey(key)
                    .build())
            .getKey());
  }

  @Test
  void mapsCreatedJobWithSubscription() {
    final long key = Protocol.encodePartitionId(3, 123);
    final var record =
        new JobRecord().setSubscriptionKey(41).setJobType("charge").setTenantId("tenant-a");
    final var response =
        GatewayResponseMapper.toCreateJobResponse(new BrokerResponse<>(record, 3, key));

    assertEquals(key, response.getJobKey());
    assertEquals(41, response.getSubscriptionKey());
    assertEquals("charge", response.getJobType());
    assertEquals("tenant-a", response.getTenantId());
  }

  @Test
  void mapsCanceledJobKey() {
    final var response =
        GatewayResponseMapper.toCancelJobResponse(new BrokerResponse<>(new JobRecord(), 3, 123));

    assertEquals(123, response.getJobKey());
  }

  @Test
  void queryJobRpcReturnsActivatedJobDirectly() {
    final var method =
        GatewayOuterClass.getDescriptor().findServiceByName("Gateway").findMethodByName("QueryJob");
    assertEquals("QueryJobRequest", method.getInputType().getName());
    assertEquals("ActivatedJob", method.getOutputType().getName());
    assertTrue(!method.isClientStreaming() && !method.isServerStreaming());
  }

  @Test
  void resolveIncidentRpcUsesIncidentKeyAndReturnsAnEmptyResponse() {
    final var method =
        GatewayOuterClass.getDescriptor()
            .findServiceByName("Gateway")
            .findMethodByName("ResolveIncident");
    assertEquals("ResolveIncidentRequest", method.getInputType().getName());
    assertEquals("ResolveIncidentResponse", method.getOutputType().getName());
    assertEquals(
        0,
        GatewayResponseMapper.toResolveIncidentResponse(
                new BrokerResponse<>(new IncidentRecord(), 3, 123))
            .getSerializedSize());
  }

  @Test
  void failJobRpcReturnsAnEmptyResponse() {
    final var method =
        GatewayOuterClass.getDescriptor().findServiceByName("Gateway").findMethodByName("FailJob");
    assertEquals("FailJobRequest", method.getInputType().getName());
    assertEquals("FailJobResponse", method.getOutputType().getName());
    assertEquals(
        0,
        GatewayResponseMapper.toFailJobResponse(new BrokerResponse<>(new JobRecord(), 3, 123))
            .getSerializedSize());
  }

  @Test
  void setVariablesRpcReturnsTheJobKey() {
    final var method =
        GatewayOuterClass.getDescriptor()
            .findServiceByName("Gateway")
            .findMethodByName("SetVariables");
    assertEquals("SetVariablesRequest", method.getInputType().getName());
    assertEquals("SetVariablesResponse", method.getOutputType().getName());
    assertEquals(
        123,
        GatewayResponseMapper.toSetVariablesResponse(
                new BrokerResponse<>(new VariableDocumentRecord().setJobKey(123), 3, 456))
            .getJobKey());
  }

  @Test
  void mapsQueriedIncidentFields() {
    final var record =
        new IncidentRecord()
            .setErrorMessage("retries exhausted")
            .setErrorType(ErrorType.JOB_NO_RETRIES)
            .setJobKey(201)
            .setTenantId("tenant-a");

    final var response =
        GatewayResponseMapper.toQueryIncidentResponse(new BrokerResponse<>(record, 3, 301));

    assertEquals(301, response.getKey());
    assertEquals("retries exhausted", response.getErrorMessage());
    assertEquals("JOB_NO_RETRIES", response.getErrorType());
    assertEquals(201, response.getJobKey());
    assertEquals("tenant-a", response.getTenantId());
  }

  @ParameterizedTest
  @ValueSource(ints = {Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE})
  void queryAndBatchActivationUseTheSameCompleteActivatedJobMapping(final int priority) {
    final long key = 9_007_199_254_741_111L;
    final var record =
        new JobRecord()
            .setJobType("audit")
            .setWorker("worker-a")
            .setRetries(5)
            .setPriority(priority)
            .setDeadline(45_000)
            .setTimeout(30_000)
            .setVariablesJson("{\"order\":42,\"nested\":{\"ready\":true}}")
            .setSubscriptionKey(41)
            .setWorkerId("worker-session")
            .setWorkerLeaseToken(9_007_199_254_740_993L)
            .setTenantId("tenant-a");
    final ActivatedJob queried =
        GatewayResponseMapper.toActivatedJob(new BrokerResponse<>(record, 3, key));
    final var batch = new JobBatchRecord();
    batch.getJobs().add(new JobBatchRecord.ActivatedJob(key, record));
    final ActivatedJob activated =
        GatewayResponseMapper.toActivateJobsResponse(new BrokerResponse<>(batch, 3, 1)).getJobs(0);

    assertEquals(activated, queried);
    assertEquals(key, queried.getKey());
    assertEquals("audit", queried.getType());
    assertEquals("worker-a", queried.getWorker());
    assertEquals(5, queried.getRetries());
    assertEquals(priority, queried.getPriority());
    assertEquals(45_000, queried.getDeadline());
    assertEquals("{\"order\":42,\"nested\":{\"ready\":true}}", queried.getVariables());
    assertEquals(41, queried.getSubscriptionKey());
    assertEquals("worker-session", queried.getWorkerId());
    assertEquals(9_007_199_254_740_993L, queried.getWorkerLeaseToken());
    assertEquals("tenant-a", queried.getTenantId());
  }

  @Test
  void mapsOptionalJobFieldsToStableDefaults() {
    final var result =
        GatewayResponseMapper.toActivatedJob(
            new BrokerResponse<>(new JobRecord().setVariablesJson("{}"), 1, 42));
    assertEquals(42, result.getKey());
    assertEquals(50, result.getPriority());
    assertEquals("", result.getType());
    assertEquals("", result.getWorker());
    assertEquals("<default>", result.getTenantId());
    assertEquals("{}", result.getVariables());
  }
}
