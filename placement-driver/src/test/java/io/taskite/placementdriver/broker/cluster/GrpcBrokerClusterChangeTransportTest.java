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
package io.taskite.placementdriver.broker.cluster;

import static org.junit.jupiter.api.Assertions.*;

import io.taskite.broker.client.BrokerClient;
import io.taskite.broker.client.request.BrokerAdminRequest;
import io.taskite.broker.client.request.BrokerRequest;
import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.placementdriver.broker.request.*;
import io.taskite.protocol.impl.encoding.*;
import io.taskite.protocol.impl.record.value.broker.*;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.TopologyOperationType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.broker.*;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.RaftConfigurationReference;
import io.taskite.topology.RecoveryFence;
import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class GrpcBrokerClusterChangeTransportTest {
  @Test
  void sendsAllAdministrationViaTheCommonClientAndDecodesConcreteResponses() throws Exception {
    final var endpoint = new BrokerEndpoint("broker-1", "localhost", 26501, 26502);
    final var configuration =
        new BrokerPartitionConfigurationRecord(
            "demo",
            7,
            "broker-1",
            2,
            9,
            "digest",
            List.of(new BrokerPartitionMember("broker-1", true)),
            3,
            4,
            "owner",
            11);
    final var types = new ArrayList<Intent>();
    final BrokerClient client =
        new BrokerClient() {
          @Override
          public <T> ActorFuture<BrokerResponse<T>> sendRequest(
              final BrokerEndpoint actualEndpoint, final BrokerRequest<T> request) {
            assertEquals(endpoint, actualEndpoint);
            assertInstanceOf(BrokerAdminRequest.class, request);
            assertFalse(request.allowsTransportRetry());
            final var bytes = request.serializeRequest();
            final var frame = new ExecuteAdminRequest();
            frame.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
            types.add(frame.getIntent());
            final UnifiedRecordValue value;
            if (frame.getValueType() == ValueType.BROKER_PARTITION_CONFIGURATION) {
              final var decoded = new BrokerPartitionConfigurationRecord();
              decoded.wrap(frame.getValue());
              assertEquals("demo", decoded.getClusterId());
              assertEquals(7, decoded.getPartitionId());
              if (frame.getIntent() == BrokerPartitionConfigurationIntent.INSTALL_AUTHORITY) {
                assertEquals(5, decoded.getGeneration());
              }
              value = configuration;
            } else {
              final var decoded = new BrokerPartitionOperationRecord();
              decoded.wrap(frame.getValue());
              assertEquals("operation-1", decoded.getOpId());
              value = decoded.setApplied(true);
            }
            final var payload = new UnsafeBuffer(new byte[value.getLength()]);
            value.write(payload, 0);
            final var response =
                new ExecuteAdminResponse()
                    .setClusterId(frame.getClusterId())
                    .setPartitionId(frame.getPartitionId())
                    .setValueType(frame.getValueType())
                    .setIntent(
                        Intent.fromProtocolValue(
                            frame.getValueType(), (short) (frame.getIntent().value() + 1)))
                    .setRecordType(RecordType.EVENT)
                    .setValue(payload, 0, payload.capacity());
            final var reply = new UnsafeBuffer(new byte[response.getLength()]);
            response.write(reply, 0);
            return CompletableActorFuture.completed(request.getResponse(reply));
          }
        };
    final var transport = new GrpcBrokerClusterChangeTransport(client);
    assertEquals(
        configuration.toJson(),
        transport
            .readConfiguration(endpoint, new BrokerReadPartitionConfigurationRequest("demo", 7))
            .get()
            .toJson());
    assertEquals(
        configuration.toJson(),
        transport
            .installAuthority(
                endpoint, new BrokerInstallPartitionAuthorityRequest("demo", 7, 4, 5, "owner", 3))
            .get()
            .toJson());
    final var operation =
        new BrokerApplyTopologyOperationRequest(
            new BrokerApplyTopologyOperationRequest.Partition("demo", 7),
            new BrokerApplyTopologyOperationRequest.Operation(
                "operation-1", TopologyOperationType.JOIN, "broker-1", 0),
            new BrokerApplyTopologyOperationRequest.Configuration(
                configuration.members(),
                new RaftConfigurationReference(9, "digest"),
                new RecoveryFence(3, 3, "")),
            new BrokerApplyTopologyOperationRequest.Authority(4, "owner"));
    final var result = transport.dispatchBrokerClusterChange(endpoint, operation).get();
    assertEquals("operation-1", result.operationId());
    assertTrue(result.applied());
    assertEquals(
        List.of(
            BrokerPartitionConfigurationIntent.QUERY,
            BrokerPartitionConfigurationIntent.INSTALL_AUTHORITY,
            BrokerPartitionOperationIntent.JOIN),
        types);
  }
}
