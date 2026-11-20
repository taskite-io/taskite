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

import io.grpc.Status;
import io.taskite.broker.client.response.BrokerRejection;
import io.taskite.broker.client.response.BrokerRejectionException;
import io.taskite.broker.client.response.BrokerRequestRejectedBeforeAppendException;
import io.taskite.gateway.api.GatewayErrorMapper;
import io.taskite.gateway.common.ClusterTopologyUnavailableException;
import io.taskite.gateway.common.UnknownClusterException;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import java.util.Set;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;

final class GatewayErrorMapperTest {
  @Test
  void mapsJobSubscriptionNotFoundRejectionToGrpcWithReason() {
    final String reason = "Job subscription not found: missing";
    final var rejection = new BrokerRejection(JobIntent.CREATE, 0, RejectionType.NOT_FOUND, reason);
    final var status = GatewayErrorMapper.toStatus(new BrokerRejectionException(rejection));

    assertEquals(Status.Code.NOT_FOUND, status.getCode());
    assertTrue(status.getDescription().contains(reason));
  }

  @Test
  void mapsExhaustedPreAppendRoutingWaitToUnavailable() {
    final var status =
        GatewayErrorMapper.toStatusException(new BrokerRequestRejectedBeforeAppendException());
    assertEquals(Status.Code.UNAVAILABLE, status.getStatus().getCode());
  }

  @Test
  void mapsUnknownClusterWithSortedKnownClustersInDescription() {
    final var status =
        GatewayErrorMapper.toStatusException(
            new UnknownClusterException(
                "demo3", Set.of("demo2", "demo"), new RuntimeException("missing")));

    assertEquals(Status.Code.NOT_FOUND, status.getStatus().getCode());
    assertEquals(
        "UNKNOWN_CLUSTER: requested cluster \"demo3\" is not known by this gateway, "
            + "know clusters: [\"demo\", \"demo2\"]",
        status.getStatus().getDescription());
    assertNull(status.getTrailers());
  }

  @Test
  void mapsTopologyFailuresToUnavailable() {
    final var status =
        GatewayErrorMapper.toStatusException(
            new ClusterTopologyUnavailableException(
                "cluster-a", Status.DEADLINE_EXCEEDED.asRuntimeException()));

    assertEquals(Status.Code.UNAVAILABLE, status.getStatus().getCode());
  }

  @Test
  void mapsClientCancellationWithoutRetryingAsCancelled() {
    final var status =
        GatewayErrorMapper.toStatusException(new CancellationException("client cancelled"));

    assertEquals(Status.Code.CANCELLED, status.getStatus().getCode());
  }
}
