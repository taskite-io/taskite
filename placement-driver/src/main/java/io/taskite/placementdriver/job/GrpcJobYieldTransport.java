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
package io.taskite.placementdriver.job;

import io.taskite.broker.client.BrokerClient;
import io.taskite.broker.client.impl.BrokerClientImpl;
import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.broker.client.transport.GrpcBrokerClientTransport;
import io.taskite.placementdriver.broker.request.BrokerSyncWorkerSessionsRequest;
import io.taskite.placementdriver.broker.request.BrokerYieldJobsRequest;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.topology.BrokerEndpoint;
import java.time.Duration;

/** Worker control commands use the same request service and transport as other PD commands. */
public final class GrpcJobYieldTransport implements JobYieldTransport {
  private final BrokerClient client;

  public GrpcJobYieldTransport(final Duration timeout) {
    client = new BrokerClientImpl(new GrpcBrokerClientTransport(), timeout, 0);
  }

  @Override
  public ActorFuture<JobYieldRecord> yieldJobs(
      final BrokerEndpoint endpoint, final JobYieldRecord request) {
    return client
        .sendRequest(endpoint, new BrokerYieldJobsRequest(request))
        .thenApply(BrokerResponse::getResponse);
  }

  @Override
  public ActorFuture<JobYieldRecord> syncWorkerSessions(
      final BrokerEndpoint endpoint, final JobYieldRecord request) {
    return client
        .sendRequest(endpoint, new BrokerSyncWorkerSessionsRequest(request))
        .thenApply(BrokerResponse::getResponse);
  }

  @Override
  public void close() throws Exception {
    client.close();
  }
}
