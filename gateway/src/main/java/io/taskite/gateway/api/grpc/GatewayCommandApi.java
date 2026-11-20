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
package io.taskite.gateway.api.grpc;

import io.grpc.stub.StreamObserver;
import io.taskite.gateway.api.GatewayEndpointManager;
import io.taskite.gateway.protocol.ActivateJobsRequest;
import io.taskite.gateway.protocol.ActivateJobsResponse;
import io.taskite.gateway.protocol.ActivatedJob;
import io.taskite.gateway.protocol.CancelJobRequest;
import io.taskite.gateway.protocol.CancelJobResponse;
import io.taskite.gateway.protocol.CompleteJobRequest;
import io.taskite.gateway.protocol.CompleteJobResponse;
import io.taskite.gateway.protocol.CreateJobRequest;
import io.taskite.gateway.protocol.CreateJobResponse;
import io.taskite.gateway.protocol.CreateJobSubscriptionRequest;
import io.taskite.gateway.protocol.CreateJobSubscriptionResponse;
import io.taskite.gateway.protocol.CreateTimerSubscriptionRequest;
import io.taskite.gateway.protocol.CreateTimerSubscriptionResponse;
import io.taskite.gateway.protocol.DeleteJobSubscriptionRequest;
import io.taskite.gateway.protocol.DeleteJobSubscriptionResponse;
import io.taskite.gateway.protocol.DeleteTimerSubscriptionRequest;
import io.taskite.gateway.protocol.DeleteTimerSubscriptionResponse;
import io.taskite.gateway.protocol.FailJobRequest;
import io.taskite.gateway.protocol.FailJobResponse;
import io.taskite.gateway.protocol.GatewayGrpc;
import io.taskite.gateway.protocol.QueryIncidentRequest;
import io.taskite.gateway.protocol.QueryIncidentResponse;
import io.taskite.gateway.protocol.QueryJobRequest;
import io.taskite.gateway.protocol.ResolveIncidentRequest;
import io.taskite.gateway.protocol.ResolveIncidentResponse;
import io.taskite.gateway.protocol.SetVariablesRequest;
import io.taskite.gateway.protocol.SetVariablesResponse;
import io.taskite.gateway.protocol.UpdateJobPriorityRequest;
import io.taskite.gateway.protocol.UpdateJobPriorityResponse;
import io.taskite.gateway.protocol.UpdateJobRetriesRequest;
import io.taskite.gateway.protocol.UpdateJobRetriesResponse;
import io.taskite.gateway.protocol.UpdateJobTimeoutRequest;
import io.taskite.gateway.protocol.UpdateJobTimeoutResponse;
import java.util.Objects;

/** Thin gRPC adapter delegating every command to the shared Endpoint Manager. */
public final class GatewayCommandApi extends GatewayGrpc.GatewayImplBase {
  private final GatewayEndpointManager endpointManager;

  public GatewayCommandApi(final GatewayEndpointManager endpointManager) {
    this.endpointManager = Objects.requireNonNull(endpointManager, "endpointManager");
  }

  @Override
  public void createJobSubscription(
      final CreateJobSubscriptionRequest request,
      final StreamObserver<CreateJobSubscriptionResponse> responseObserver) {
    endpointManager.createJobSubscription(request, responseObserver);
  }

  @Override
  public void deleteJobSubscription(
      final DeleteJobSubscriptionRequest request,
      final StreamObserver<DeleteJobSubscriptionResponse> responseObserver) {
    endpointManager.deleteJobSubscription(request, responseObserver);
  }

  @Override
  public void createTimerSubscription(
      final CreateTimerSubscriptionRequest request,
      final StreamObserver<CreateTimerSubscriptionResponse> observer) {
    endpointManager.createTimerSubscription(request, observer);
  }

  @Override
  public void deleteTimerSubscription(
      final DeleteTimerSubscriptionRequest request,
      final StreamObserver<DeleteTimerSubscriptionResponse> observer) {
    endpointManager.deleteTimerSubscription(request, observer);
  }

  @Override
  public void createJob(
      final CreateJobRequest request, final StreamObserver<CreateJobResponse> responseObserver) {
    endpointManager.createJob(request, responseObserver);
  }

  @Override
  public void activateJobs(
      final ActivateJobsRequest request,
      final StreamObserver<ActivateJobsResponse> responseObserver) {
    endpointManager.activateJobs(request, responseObserver);
  }

  @Override
  public void completeJob(
      final CompleteJobRequest request,
      final StreamObserver<CompleteJobResponse> responseObserver) {
    endpointManager.completeJob(request, responseObserver);
  }

  @Override
  public void failJob(
      final FailJobRequest request, final StreamObserver<FailJobResponse> responseObserver) {
    endpointManager.failJob(request, responseObserver);
  }

  @Override
  public void updateJobRetries(
      final UpdateJobRetriesRequest request,
      final StreamObserver<UpdateJobRetriesResponse> observer) {
    endpointManager.updateJobRetries(request, observer);
  }

  @Override
  public void updateJobTimeout(
      final UpdateJobTimeoutRequest request,
      final StreamObserver<UpdateJobTimeoutResponse> responseObserver) {
    endpointManager.updateJobTimeout(request, responseObserver);
  }

  @Override
  public void updateJobPriority(
      final UpdateJobPriorityRequest request,
      final StreamObserver<UpdateJobPriorityResponse> responseObserver) {
    endpointManager.updateJobPriority(request, responseObserver);
  }

  @Override
  public void queryJob(
      final QueryJobRequest request, final StreamObserver<ActivatedJob> responseObserver) {
    endpointManager.queryJob(request, responseObserver);
  }

  @Override
  public void cancelJob(
      final CancelJobRequest request, final StreamObserver<CancelJobResponse> responseObserver) {
    endpointManager.cancelJob(request, responseObserver);
  }

  @Override
  public void queryIncident(
      final QueryIncidentRequest request,
      final StreamObserver<QueryIncidentResponse> responseObserver) {
    endpointManager.queryIncident(request, responseObserver);
  }

  @Override
  public void resolveIncident(
      final ResolveIncidentRequest request,
      final StreamObserver<ResolveIncidentResponse> responseObserver) {
    endpointManager.resolveIncident(request, responseObserver);
  }

  @Override
  public void setVariables(
      final SetVariablesRequest request,
      final StreamObserver<SetVariablesResponse> responseObserver) {
    endpointManager.setVariables(request, responseObserver);
  }
}
