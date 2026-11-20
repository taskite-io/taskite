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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Descriptors;
import io.grpc.MethodDescriptor;
import io.taskite.gateway.api.GatewayRequestMapper;
import io.taskite.gateway.protocol.ActivateJobsRequest;
import io.taskite.gateway.protocol.CancelJobRequest;
import io.taskite.gateway.protocol.CompleteJobRequest;
import io.taskite.gateway.protocol.CreateJobRequest;
import io.taskite.gateway.protocol.CreateJobSubscriptionRequest;
import io.taskite.gateway.protocol.DeleteJobSubscriptionRequest;
import io.taskite.gateway.protocol.FailJobRequest;
import io.taskite.gateway.protocol.GatewayGrpc;
import io.taskite.gateway.protocol.GatewayOuterClass;
import io.taskite.gateway.protocol.QueryIncidentRequest;
import io.taskite.gateway.protocol.QueryJobRequest;
import io.taskite.gateway.protocol.ResolveIncidentRequest;
import io.taskite.gateway.protocol.SetVariablesRequest;
import io.taskite.gateway.protocol.UpdateJobPriorityRequest;
import io.taskite.gateway.protocol.UpdateJobTimeoutRequest;
import io.taskite.protocol.Protocol;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.IncidentIntent;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.protocol.record.intent.broker.VariableDocumentIntent;
import io.taskite.protocol.record.value.broker.TenantOwned;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class GatewayRequestMapperTest {
  @Test
  void publicRequestsDoNotExposeClusterOrTenantFields() {
    assertNoRoutingFields(CreateJobSubscriptionRequest.getDescriptor());
    assertNoRoutingFields(DeleteJobSubscriptionRequest.getDescriptor());
    assertNoRoutingFields(CreateJobRequest.getDescriptor());
    assertNoRoutingFields(ActivateJobsRequest.getDescriptor());
    assertNoRoutingFields(CompleteJobRequest.getDescriptor());
    assertNoRoutingFields(FailJobRequest.getDescriptor());
    assertNoRoutingFields(QueryJobRequest.getDescriptor());
    assertNoRoutingFields(CancelJobRequest.getDescriptor());
    assertNoRoutingFields(QueryIncidentRequest.getDescriptor());
    assertNoRoutingFields(ResolveIncidentRequest.getDescriptor());
    assertNoRoutingFields(SetVariablesRequest.getDescriptor());
    assertNoRoutingFields(UpdateJobTimeoutRequest.getDescriptor());
    assertNoRoutingFields(UpdateJobPriorityRequest.getDescriptor());
  }

  @Test
  void allActiveProtocolFieldsUseLowerCamelCase() {
    for (final var message : GatewayOuterClass.getDescriptor().getMessageTypes()) {
      for (final var field : message.getFields()) {
        assertFalse(
            field.getName().contains("_"),
            () -> message.getName() + "." + field.getName() + " must use lowerCamelCase");
        assertEquals(field.getName(), field.getJsonName());
        assertTrue(Character.isLowerCase(field.getName().charAt(0)));
      }
    }
  }

  @Test
  void mapsBusinessFieldsWithoutTransportContext() {
    final var subscriptionRequest =
        GatewayRequestMapper.toCreateJobSubscriptionRequest(
            CreateJobSubscriptionRequest.newBuilder()
                .setName("payment")
                .setJobType("charge")
                .setVariables("{\"amount\":42}")
                .build());
    assertEquals("", subscriptionRequest.getClusterId());
    assertEquals("", subscriptionRequest.getTenantId());
    assertEquals("payment", subscriptionRequest.getCreateJobSubscription().getName());
    assertEquals("charge", subscriptionRequest.getCreateJobSubscription().getJobType());
    assertEquals(3, subscriptionRequest.getCreateJobSubscription().getRetries());
    assertEquals(50, subscriptionRequest.getCreateJobSubscription().getPriority());
    assertNull(
        subscriptionRequest
            .getCreateJobSubscription()
            .getDescriptorForType()
            .findFieldByName("dueDate"));

    final var deletionRequest =
        GatewayRequestMapper.toDeleteJobSubscriptionRequest(
            DeleteJobSubscriptionRequest.newBuilder().setKey(41).build());
    assertEquals("", deletionRequest.getClusterId());
    assertEquals("", deletionRequest.getTenantId());
    assertEquals(41, deletionRequest.getDeleteJobSubscription().getSubscriptionKey());

    assertEquals(
        TenantOwned.DEFAULT_TENANT_IDENTIFIER,
        GatewayRequestMapper.toCompleteJobRequest(CompleteJobRequest.getDefaultInstance())
            .getRecordValue()
            .getTenantId());
    assertEquals(
        TenantOwned.DEFAULT_TENANT_IDENTIFIER,
        GatewayRequestMapper.toQueryJobRequest(QueryJobRequest.newBuilder().setJobKey(1).build())
            .getRecordValue()
            .getTenantId());
  }

  @Test
  void mapsSubscriptionWithOnlyJobTypeAndDefaultOptions() {
    final var request =
        GatewayRequestMapper.toCreateJobSubscriptionRequest(
            CreateJobSubscriptionRequest.newBuilder().setJobType("charge").build());
    final var subscription = request.getCreateJobSubscription();
    assertEquals("", subscription.getName());
    assertEquals("", subscription.getVariables());
    assertEquals(3, subscription.getRetries());
    assertEquals(50, subscription.getPriority());
    final var explicit =
        GatewayRequestMapper.toCreateJobSubscriptionRequest(
                CreateJobSubscriptionRequest.newBuilder()
                    .setJobType("charge")
                    .setRetries(0)
                    .setPriority(0)
                    .build())
            .getCreateJobSubscription();
    assertTrue(explicit.hasRetries());
    assertTrue(explicit.hasPriority());
    assertEquals(0, explicit.getRetries());
    assertEquals(0, explicit.getPriority());
  }

  @Test
  void mapsCancellationToKeyedJobCommand() {
    final long jobKey = Protocol.encodePartitionId(7, 42);

    final var request =
        GatewayRequestMapper.toCancelJobRequest(
            CancelJobRequest.newBuilder().setJobKey(jobKey).build());

    assertEquals(jobKey, request.getKey());
    assertEquals(7, request.getPartitionId());
    assertEquals(ValueType.JOB, request.getValueType());
    assertEquals(JobIntent.CANCEL, request.getIntent());
    assertTrue(request.addressesSpecificPartition());
  }

  @Test
  void mapsJobFailureToKeyedCommand() {
    final long jobKey = Protocol.encodePartitionId(7, 41);
    final var request =
        GatewayRequestMapper.toFailJobRequest(
            FailJobRequest.newBuilder()
                .setJobKey(jobKey)
                .setWorkerId("worker-instance")
                .setWorkerLeaseToken(9_007_199_254_740_993L)
                .setRetries(2)
                .setErrorMessage("temporary")
                .setRetryBackOff(500)
                .setVariables("{\"attempt\":2}")
                .build());

    assertEquals(jobKey, request.getKey());
    assertEquals(7, request.getPartitionId());
    assertEquals(ValueType.JOB, request.getValueType());
    assertEquals(JobIntent.FAIL, request.getIntent());
    assertEquals("worker-instance", request.getRecordValue().getWorkerId());
    assertEquals(9_007_199_254_740_993L, request.getRecordValue().getWorkerLeaseToken());
    assertEquals(2, request.getRecordValue().getRetries());
    assertEquals("temporary", request.getRecordValue().getErrorMessage());
    assertEquals(500, request.getRecordValue().getRetryBackoff());
    assertEquals(Map.of("attempt", 2), request.getRecordValue().getVariables());
  }

  @Test
  void shouldMapTimeoutUpdateToKeyedJobCommandWithWorkerSession() {
    // given
    final long jobKey = Protocol.encodePartitionId(7, 41);

    // when
    final var request =
        GatewayRequestMapper.toUpdateJobTimeoutRequest(
            UpdateJobTimeoutRequest.newBuilder()
                .setJobKey(jobKey)
                .setTimeout(60_000)
                .setWorkerId("owner")
                .setWorkerLeaseToken(9_007_199_254_740_993L)
                .build());

    // then
    assertEquals(jobKey, request.getKey());
    assertEquals(7, request.getPartitionId());
    assertEquals(ValueType.JOB, request.getValueType());
    assertEquals(JobIntent.UPDATE_TIMEOUT, request.getIntent());
    assertEquals(60_000, request.getRecordValue().getTimeout());
    assertEquals("owner", request.getRecordValue().getWorkerId());
    assertEquals(9_007_199_254_740_993L, request.getRecordValue().getWorkerLeaseToken());
    assertTrue(request.addressesSpecificPartition());
    assertEquals(TenantOwned.DEFAULT_TENANT_IDENTIFIER, request.getRecordValue().getTenantId());
  }

  @Test
  void rejectsInvalidJobFailureFields() {
    assertThrows(
        IllegalArgumentException.class,
        () -> GatewayRequestMapper.toFailJobRequest(FailJobRequest.getDefaultInstance()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GatewayRequestMapper.toFailJobRequest(
                FailJobRequest.newBuilder().setJobKey(1).setRetries(-1).build()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GatewayRequestMapper.toFailJobRequest(
                FailJobRequest.newBuilder().setJobKey(1).setRetryBackOff(-1).build()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GatewayRequestMapper.toFailJobRequest(
                FailJobRequest.newBuilder().setJobKey(1).setVariables("[]").build()));
  }

  @ParameterizedTest
  @ValueSource(ints = {Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE})
  void shouldMapSignedPriorityToKeyedJobCommand(final int priority) {
    // given
    final long jobKey = Protocol.encodePartitionId(7, 41);

    // when
    final var request =
        GatewayRequestMapper.toUpdateJobPriorityRequest(
            UpdateJobPriorityRequest.newBuilder().setJobKey(jobKey).setPriority(priority).build());

    // then
    assertEquals(jobKey, request.getKey());
    assertEquals(7, request.getPartitionId());
    assertEquals(ValueType.JOB, request.getValueType());
    assertEquals(JobIntent.UPDATE_PRIORITY, request.getIntent());
    assertEquals(priority, request.getRecordValue().getPriority());
    assertTrue(request.addressesSpecificPartition());
    assertEquals(TenantOwned.DEFAULT_TENANT_IDENTIFIER, request.getRecordValue().getTenantId());
    assertEquals("", request.getRecordValue().getWorkerId());
    assertEquals(0, request.getRecordValue().getWorkerLeaseToken());
  }

  @ParameterizedTest
  @ValueSource(longs = {0, -1, Long.MIN_VALUE})
  void shouldRejectNonPositivePriorityUpdateJobKey(final long key) {
    // when / then
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GatewayRequestMapper.toUpdateJobPriorityRequest(
                UpdateJobPriorityRequest.newBuilder().setJobKey(key).setPriority(10).build()));
  }

  @Test
  void mapsIncidentQueryToAnOwnerKeyedCommand() {
    final long key = Protocol.encodePartitionId(7, 44);
    final var request =
        GatewayRequestMapper.toQueryIncidentRequest(
            QueryIncidentRequest.newBuilder().setJobKey(key).build());

    assertEquals(key, request.getKey());
    assertEquals(7, request.getPartitionId());
    assertEquals(ValueType.INCIDENT, request.getValueType());
    assertEquals(IncidentIntent.QUERY, request.getIntent());
    assertTrue(request.addressesSpecificPartition());
  }

  @Test
  void rejectsMissingAndNonPositiveIncidentOwnerKeys() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GatewayRequestMapper.toQueryIncidentRequest(QueryIncidentRequest.getDefaultInstance()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GatewayRequestMapper.toQueryIncidentRequest(
                QueryIncidentRequest.newBuilder().setJobKey(0).build()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GatewayRequestMapper.toQueryIncidentRequest(
                QueryIncidentRequest.newBuilder().setJobKey(-1).build()));
  }

  @Test
  void mapsIncidentResolutionToAnIncidentKeyedCommand() {
    final long incidentKey = Protocol.encodePartitionId(7, 45);
    final var request =
        GatewayRequestMapper.toResolveIncidentRequest(
            ResolveIncidentRequest.newBuilder().setIncidentKey(incidentKey).build());

    assertEquals(incidentKey, request.getKey());
    assertEquals(7, request.getPartitionId());
    assertEquals(ValueType.INCIDENT, request.getValueType());
    assertEquals(IncidentIntent.RESOLVE, request.getIntent());
    assertTrue(request.addressesSpecificPartition());
  }

  @Test
  void rejectsMissingAndNonPositiveIncidentKeysForResolution() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GatewayRequestMapper.toResolveIncidentRequest(
                ResolveIncidentRequest.getDefaultInstance()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GatewayRequestMapper.toResolveIncidentRequest(
                ResolveIncidentRequest.newBuilder().setIncidentKey(0).build()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GatewayRequestMapper.toResolveIncidentRequest(
                ResolveIncidentRequest.newBuilder().setIncidentKey(-1).build()));
  }

  @Test
  void mapsSetVariablesToVariableDocumentCommand() {
    final long scopeKey = Protocol.encodePartitionId(7, 46);
    final var request =
        GatewayRequestMapper.toSetVariablesRequest(
            SetVariablesRequest.newBuilder()
                .setJobKey(scopeKey)
                .setVariables("{\"amount\":42}")
                .build());

    assertEquals(0, request.getKey());
    assertEquals(7, request.getPartitionId());
    assertEquals(ValueType.VARIABLE_DOCUMENT, request.getValueType());
    assertEquals(VariableDocumentIntent.UPDATE, request.getIntent());
    assertEquals(scopeKey, request.getRecordValue().getJobKey());
    assertEquals(Map.of("amount", 42), request.getRecordValue().getVariables());
    assertTrue(request.addressesSpecificPartition());
  }

  @Test
  void mapsEmptyJobVariablesToEmptyObject() {
    final var request =
        GatewayRequestMapper.toSetVariablesRequest(
            SetVariablesRequest.newBuilder().setJobKey(1).build());

    assertEquals(1, request.getRecordValue().getJobKey());
    assertEquals(Map.of(), request.getRecordValue().getVariables());
  }

  @Test
  void rejectsInvalidSetVariablesFields() {
    assertThrows(
        IllegalArgumentException.class,
        () -> GatewayRequestMapper.toSetVariablesRequest(SetVariablesRequest.getDefaultInstance()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GatewayRequestMapper.toSetVariablesRequest(
                SetVariablesRequest.newBuilder().setJobKey(-1).setVariables("{}").build()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GatewayRequestMapper.toSetVariablesRequest(
                SetVariablesRequest.newBuilder().setJobKey(1).setVariables("not-json").build()));
  }

  @Test
  void typeOnlyBatchActivationUsesDefaultsWithoutChoosingPartitionZero() {
    final var request =
        GatewayRequestMapper.toActivateJobsRequest(
            ActivateJobsRequest.newBuilder().setType("payment").build());
    assertFalse(request.addressesSpecificPartition());
    final var value = request.getRecordValue();
    assertEquals("payment", value.getType());
    assertEquals(30_000, value.getTimeout());
    assertEquals(100, value.getMaxJobsToActivate());
    assertEquals("", value.getWorker());
    assertTrue(value.getFetchVariables().isEmpty());
  }

  @Test
  void batchActivationRetainsExplicitOptions() {
    final var value =
        GatewayRequestMapper.toActivateJobsRequest(
                ActivateJobsRequest.newBuilder()
                    .setType("payment")
                    .setWorker("worker")
                    .setWorkerId("worker-instance")
                    .setWorkerLeaseToken(9_007_199_254_740_993L)
                    .setTimeout(5000)
                    .setMaxJobsToActivate(7)
                    .addFetchVariable("amount")
                    .build())
            .getRecordValue();
    assertEquals(5000, value.getTimeout());
    assertEquals(7, value.getMaxJobsToActivate());
    assertEquals("worker", value.getWorker());
    assertEquals("worker-instance", value.getWorkerId());
    assertEquals(9_007_199_254_740_993L, value.getWorkerLeaseToken());
    assertEquals(List.of("amount"), value.getFetchVariables());
  }

  @Test
  void activationProtocolPreservesOriginalFieldsAndAddsWorkerSessionWithoutKeyedRpc() {
    final var fields = ActivateJobsRequest.getDescriptor().getFields();
    assertEquals(
        List.of(
            "type",
            "worker",
            "timeout",
            "maxJobsToActivate",
            "fetchVariable",
            "requestTimeout",
            "tenantIds",
            "workerId",
            "workerLeaseToken"),
        fields.stream().map(Descriptors.FieldDescriptor::getName).toList());
    assertEquals(
        List.of(1, 2, 3, 4, 5, 6, 7, 8, 9),
        fields.stream().map(Descriptors.FieldDescriptor::getNumber).toList());
    assertTrue(
        GatewayGrpc.getServiceDescriptor().getMethods().stream()
            .noneMatch(method -> "ActivateJob".equals(method.getBareMethodName())));
    assertEquals(
        MethodDescriptor.MethodType.SERVER_STREAMING,
        GatewayGrpc.getActivateJobsMethod().getType());
  }

  @ParameterizedTest
  @ValueSource(strings = {"type", "timeout", "limit", "tenant"})
  void rejectsInvalidBatchFields(String field) {
    final var request = ActivateJobsRequest.newBuilder().setType("payment");
    switch (field) {
      case "type" -> request.setType(" ");
      case "timeout" -> request.setTimeout(-1);
      case "limit" -> request.setMaxJobsToActivate(-1);
      default -> request.addTenantIds(" ");
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> GatewayRequestMapper.toActivateJobsRequest(request.build()));
  }

  @Test
  void mapsJobTypeAndNestedVariablesWithoutChoosingAPartition() {
    final var request =
        GatewayRequestMapper.toCreateJobRequest(
            CreateJobRequest.newBuilder()
                .setJobType("charge")
                .setVariables("{\"order\":{\"id\":42},\"tags\":[\"a\",2],\"optional\":null}")
                .build());
    final var record = request.getRecordValue();
    assertEquals("charge", record.getType());
    assertEquals(3, record.getRetries());
    assertEquals(50, record.getPriority());
    assertEquals(Map.of("id", 42), record.getVariables().get("order"));
    assertEquals(List.of("a", 2), record.getVariables().get("tags"));
    assertTrue(record.getVariables().containsKey("optional"));
    assertNull(record.getVariables().get("optional"));
    assertEquals(TenantOwned.DEFAULT_TENANT_IDENTIFIER, record.getTenantId());
    assertFalse(request.addressesSpecificPartition());
    assertTrue(request.requestDispatchStrategy().isEmpty());
  }

  @Test
  void mapsJobCreationForAutomaticRouting() {
    final var request =
        GatewayRequestMapper.toCreateJobRequest(
            CreateJobRequest.newBuilder()
                .setJobType("charge")
                .setRetries(0)
                .setPriority(0)
                .setVariables("{\"orderId\":42}")
                .build());
    final var record = request.getRecordValue();

    assertEquals(0, record.getSubscriptionKey());
    assertEquals("charge", record.getType());
    assertEquals(0, record.getRetries());
    assertEquals(0, record.getPriority());
    assertEquals(Map.of("orderId", 42), record.getVariables());
    assertFalse(request.addressesSpecificPartition());
    assertFalse(request.allowsTransportRetry());
  }

  @Test
  void mapsIndependentTimerWithoutModelParsing() {
    final var command =
        GatewayRequestMapper.toCreateTimerSubscriptionRequest(
            io.taskite.gateway.protocol.CreateTimerSubscriptionRequest.newBuilder()
                .setName("daily")
                .addJobTypes("charge")
                .addJobTypes("notify")
                .setType(io.taskite.gateway.protocol.TimerType.CRON)
                .setExpression("0 * * * * *")
                .setVariables("{\"amount\":42}")
                .build());
    assertEquals(io.taskite.placementdriver.protocol.TimerSubscriptionType.CRON, command.getType());
    assertEquals("0 * * * * *", command.getExpression());
    assertEquals(java.util.List.of("charge", "notify"), command.getJobTypesList());
    assertEquals("{\"amount\":42}", command.getVariables());
  }

  @Test
  void usesAutomaticRoutingWithoutClientSelectedPartition() {
    final var request =
        GatewayRequestMapper.toCreateJobRequest(
            CreateJobRequest.newBuilder().setJobType("charge").build());
    assertFalse(request.addressesSpecificPartition());
  }

  @Test
  void mapsAllTimerTypesAndValidatesDeletionKeyAndVariables() {
    for (final var type :
        java.util.List.of(
            io.taskite.gateway.protocol.TimerType.DURATION,
            io.taskite.gateway.protocol.TimerType.CYCLE,
            io.taskite.gateway.protocol.TimerType.CRON,
            io.taskite.gateway.protocol.TimerType.TIMER_TYPE_UNSPECIFIED)) {
      final var request =
          GatewayRequestMapper.toCreateTimerSubscriptionRequest(
              io.taskite.gateway.protocol.CreateTimerSubscriptionRequest.newBuilder()
                  .setType(type)
                  .setExpression("PT1S")
                  .addJobTypes("charge")
                  .build());
      assertEquals(type.name(), request.getType().name());
    }
    final long key = 9007199254740993L;
    assertEquals(
        key,
        GatewayRequestMapper.toDeleteTimerSubscriptionRequest(
                io.taskite.gateway.protocol.DeleteTimerSubscriptionRequest.newBuilder()
                    .setKey(key)
                    .build())
            .getTimerSubscriptionKey());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GatewayRequestMapper.toDeleteTimerSubscriptionRequest(
                io.taskite.gateway.protocol.DeleteTimerSubscriptionRequest.getDefaultInstance()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GatewayRequestMapper.toCreateTimerSubscriptionRequest(
                io.taskite.gateway.protocol.CreateTimerSubscriptionRequest.newBuilder()
                    .setVariables("[]")
                    .build()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " \n\t", "{}"})
  void emptyJobVariablesMeanEmptyObject(final String variables) {
    final var request =
        GatewayRequestMapper.toCreateJobRequest(
            CreateJobRequest.newBuilder().setJobType("charge").setVariables(variables).build());
    assertEquals(Map.of(), request.getRecordValue().getVariables());
  }

  @ParameterizedTest
  @ValueSource(strings = {"[]", "[{}]", "null", "1", "true", "\"text\"", "{", "{} {}", "{} null"})
  void rejectsNonObjectMalformedAndTrailingVariables(final String variables) {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GatewayRequestMapper.toCreateJobRequest(
                CreateJobRequest.newBuilder()
                    .setJobType("charge")
                    .setVariables(variables)
                    .build()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "\n"})
  void rejectsMissingJobType(final String type) {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GatewayRequestMapper.toCreateJobRequest(
                CreateJobRequest.newBuilder().setJobType(type).build()));
  }

  private static void assertNoRoutingFields(final Descriptors.Descriptor descriptor) {
    assertNull(descriptor.findFieldByName("clusterId"));
    assertNull(descriptor.findFieldByName("tenantId"));
  }
}
