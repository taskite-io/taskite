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

import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import io.taskite.broker.client.request.BrokerRequest;
import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.gateway.api.grpc.GatewayInterceptorUtil;
import io.taskite.gateway.broker.GatewayBrokerCommandDispatcher;
import io.taskite.gateway.common.GatewayClusterResolver;
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
import io.taskite.placementdriver.client.Client;
import io.taskite.placementdriver.protocol.DistributeCommandRequest;
import io.taskite.placementdriver.protocol.DistributeCommandResponse;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.scheduler.ActorSchedulingService;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.future.ActorFuture;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/** Application entry point shared by all Gateway transport adapters. */
public final class GatewayEndpointManager implements AutoCloseable {
  private final GatewayBrokerCommandDispatcher brokerDispatcher;
  private final Client placementDriver;
  private final GatewayClusterResolver clusterResolver;
  private final ActorSchedulingService requestScheduler;
  private final JobActivationHandler activation;

  public GatewayEndpointManager(
      final GatewayBrokerCommandDispatcher brokerDispatcher,
      final Client placementDriver,
      final GatewayClusterResolver clusterResolver,
      final ActorSchedulingService requestScheduler) {
    this.brokerDispatcher = Objects.requireNonNull(brokerDispatcher, "brokerDispatcher");
    this.placementDriver = Objects.requireNonNull(placementDriver, "placementDriver");
    this.clusterResolver = Objects.requireNonNull(clusterResolver, "clusterResolver");
    this.requestScheduler = Objects.requireNonNull(requestScheduler, "requestScheduler");
    activation = new JobActivationHandler(brokerDispatcher, clusterResolver);
  }

  public void createJobSubscription(
      final CreateJobSubscriptionRequest request,
      final StreamObserver<CreateJobSubscriptionResponse> responseObserver) {
    executeRequest(
        responseObserver,
        (control, observer) ->
            sendPlacementDriverRequest(
                control,
                request,
                GatewayRequestMapper::toCreateJobSubscriptionRequest,
                GatewayResponseMapper::toCreateJobSubscriptionResponse,
                observer));
  }

  public void deleteJobSubscription(
      final DeleteJobSubscriptionRequest request,
      final StreamObserver<DeleteJobSubscriptionResponse> responseObserver) {
    executeRequest(
        responseObserver,
        (control, observer) ->
            sendPlacementDriverRequest(
                control,
                request,
                GatewayRequestMapper::toDeleteJobSubscriptionRequest,
                ignored -> GatewayResponseMapper.toDeleteJobSubscriptionResponse(request.getKey()),
                observer));
  }

  public void createTimerSubscription(
      final CreateTimerSubscriptionRequest request,
      final StreamObserver<CreateTimerSubscriptionResponse> observer) {
    executeRequest(
        observer,
        (control, response) ->
            sendTimerRequest(
                control,
                (cluster, tenant) ->
                    placementDriver.createTimerSubscription(
                        GatewayRequestMapper.toCreateTimerSubscriptionRequest(request).toBuilder()
                            .setClusterId(cluster)
                            .setTenantId(tenant)
                            .build()),
                GatewayResponseMapper::toCreateTimerSubscriptionResponse,
                response));
  }

  public void deleteTimerSubscription(
      final DeleteTimerSubscriptionRequest request,
      final StreamObserver<DeleteTimerSubscriptionResponse> observer) {
    executeRequest(
        observer,
        (control, response) ->
            sendTimerRequest(
                control,
                (cluster, tenant) ->
                    placementDriver.deleteTimerSubscription(
                        GatewayRequestMapper.toDeleteTimerSubscriptionRequest(request).toBuilder()
                            .setClusterId(cluster)
                            .setTenantId(tenant)
                            .build()),
                GatewayResponseMapper::toDeleteTimerSubscriptionResponse,
                response));
  }

  public void createJob(
      final CreateJobRequest request, final StreamObserver<CreateJobResponse> responseObserver) {
    executeRequest(
        responseObserver,
        (control, observer) ->
            sendBrokerRequest(
                control,
                request,
                GatewayRequestMapper::toCreateJobRequest,
                GatewayResponseMapper::toCreateJobResponse,
                observer));
  }

  public void activateJobs(
      final ActivateJobsRequest request,
      final StreamObserver<ActivateJobsResponse> responseObserver) {
    executeRequest(
        responseObserver, (control, observer) -> activation.activate(request, observer, control));
  }

  @Override
  public void close() {
    activation.close();
  }

  public void completeJob(
      final CompleteJobRequest request,
      final StreamObserver<CompleteJobResponse> responseObserver) {
    executeRequest(
        responseObserver,
        (control, observer) ->
            sendBrokerRequest(
                control,
                request,
                GatewayRequestMapper::toCompleteJobRequest,
                GatewayResponseMapper::toCompleteJobResponse,
                observer));
  }

  public void failJob(
      final FailJobRequest request, final StreamObserver<FailJobResponse> responseObserver) {
    executeRequest(
        responseObserver,
        (control, observer) ->
            sendBrokerRequest(
                control,
                request,
                GatewayRequestMapper::toFailJobRequest,
                GatewayResponseMapper::toFailJobResponse,
                observer));
  }

  public void updateJobRetries(
      final UpdateJobRetriesRequest request,
      final StreamObserver<UpdateJobRetriesResponse> observer) {
    executeRequest(
        observer,
        (control, response) ->
            sendBrokerRequest(
                control,
                request,
                GatewayRequestMapper::toUpdateJobRetriesRequest,
                GatewayResponseMapper::toUpdateJobRetriesResponse,
                response));
  }

  public void updateJobTimeout(
      final UpdateJobTimeoutRequest request,
      final StreamObserver<UpdateJobTimeoutResponse> responseObserver) {
    executeRequest(
        responseObserver,
        (control, observer) ->
            sendBrokerRequest(
                control,
                request,
                GatewayRequestMapper::toUpdateJobTimeoutRequest,
                GatewayResponseMapper::toUpdateJobTimeoutResponse,
                observer));
  }

  public void updateJobPriority(
      final UpdateJobPriorityRequest request,
      final StreamObserver<UpdateJobPriorityResponse> responseObserver) {
    executeRequest(
        responseObserver,
        (control, observer) ->
            sendBrokerRequest(
                control,
                request,
                GatewayRequestMapper::toUpdateJobPriorityRequest,
                GatewayResponseMapper::toUpdateJobPriorityResponse,
                observer));
  }

  public void cancelJob(
      final CancelJobRequest request, final StreamObserver<CancelJobResponse> responseObserver) {
    executeRequest(
        responseObserver,
        (control, observer) ->
            sendBrokerRequest(
                control,
                request,
                GatewayRequestMapper::toCancelJobRequest,
                GatewayResponseMapper::toCancelJobResponse,
                observer));
  }

  public void queryJob(
      final QueryJobRequest request, final StreamObserver<ActivatedJob> responseObserver) {
    executeRequest(
        responseObserver,
        (control, observer) ->
            sendBrokerRequest(
                control,
                request,
                GatewayRequestMapper::toQueryJobRequest,
                GatewayResponseMapper::toActivatedJob,
                observer));
  }

  public void queryIncident(
      final QueryIncidentRequest request,
      final StreamObserver<QueryIncidentResponse> responseObserver) {
    executeRequest(
        responseObserver,
        (control, observer) ->
            sendBrokerRequest(
                control,
                request,
                GatewayRequestMapper::toQueryIncidentRequest,
                GatewayResponseMapper::toQueryIncidentResponse,
                observer));
  }

  public void resolveIncident(
      final ResolveIncidentRequest request,
      final StreamObserver<ResolveIncidentResponse> responseObserver) {
    executeRequest(
        responseObserver,
        (control, observer) ->
            sendBrokerRequest(
                control,
                request,
                GatewayRequestMapper::toResolveIncidentRequest,
                GatewayResponseMapper::toResolveIncidentResponse,
                observer));
  }

  public void setVariables(
      final SetVariablesRequest request,
      final StreamObserver<SetVariablesResponse> responseObserver) {
    executeRequest(
        responseObserver,
        (control, observer) ->
            sendBrokerRequest(
                control,
                request,
                GatewayRequestMapper::toSetVariablesRequest,
                GatewayResponseMapper::toSetVariablesResponse,
                observer));
  }

  private <GrpcRequestT, ResponseT extends UnifiedRecordValue, GrpcResponseT>
      void sendBrokerRequest(
          final ConcurrencyControl control,
          final GrpcRequestT grpcRequest,
          final Function<GrpcRequestT, BrokerRequest<ResponseT>> requestMapper,
          final Function<BrokerResponse<ResponseT>, GrpcResponseT> responseMapper,
          final StreamObserver<GrpcResponseT> responseObserver) {
    try {
      final BrokerRequest<ResponseT> brokerRequest = requestMapper.apply(grpcRequest);

      final Context grpcContext = Context.current();
      final String clusterId =
          requireContextValue(
              GatewayInterceptorUtil.getClusterId(grpcContext),
              "clusterId is missing from the gRPC context");
      final String tenantId =
          requireContextValue(
              GatewayInterceptorUtil.getTenantId(grpcContext),
              "tenantId is missing from the gRPC context");

      brokerRequest.setTenantId(tenantId);
      consumeResponse(
          control,
          // Preflight initializes the topology cache/watch. UNKNOWN_CLUSTER or PD unavailability
          // completes this future exceptionally, so no business command is sent to a Broker.
          clusterResolver
              .resolve(clusterId)
              .thenCompose(
                  ignored ->
                      brokerDispatcher.dispatchWhenReady(
                          clusterId,
                          brokerRequest,
                          grpcContext::isCancelled,
                          control,
                          Duration.ofMillis(
                              grpcContext.getDeadline() == null
                                  ? 10_000
                                  : Math.max(
                                      0,
                                      grpcContext
                                          .getDeadline()
                                          .timeRemaining(TimeUnit.MILLISECONDS))))),
          responseMapper,
          responseObserver);
    } catch (final Exception error) {
      responseObserver.onError(GatewayErrorMapper.toStatusException(error));
    }
  }

  private <GrpcRequestT, GrpcResponseT> void sendPlacementDriverRequest(
      final ConcurrencyControl control,
      final GrpcRequestT grpcRequest,
      final Function<GrpcRequestT, DistributeCommandRequest> requestMapper,
      final Function<DistributeCommandResponse, GrpcResponseT> responseMapper,
      final StreamObserver<GrpcResponseT> responseObserver) {
    try {
      final DistributeCommandRequest request = requestMapper.apply(grpcRequest);

      final Context grpcContext = Context.current();
      final String clusterId =
          requireContextValue(
              GatewayInterceptorUtil.getClusterId(grpcContext),
              "clusterId is missing from the gRPC context");
      final String tenantId =
          requireContextValue(
              GatewayInterceptorUtil.getTenantId(grpcContext),
              "tenantId is missing from the gRPC context");

      final DistributeCommandRequest contextualRequest =
          request.toBuilder().setTenantId(tenantId).setClusterId(clusterId).build();

      consumeResponse(
          control,
          clusterResolver
              .resolve(clusterId)
              .thenCompose(ignored -> placementDriver.distributeCommand(contextualRequest)),
          responseMapper,
          responseObserver);
    } catch (final Exception error) {
      responseObserver.onError(GatewayErrorMapper.toStatusException(error));
    }
  }

  private <ResponseT, GrpcResponseT> void sendTimerRequest(
      final ConcurrencyControl control,
      final BiFunction<String, String, ActorFuture<ResponseT>> request,
      final Function<ResponseT, GrpcResponseT> mapper,
      final StreamObserver<GrpcResponseT> observer) {
    try {
      final Context context = Context.current();
      final String cluster =
          requireContextValue(
              GatewayInterceptorUtil.getClusterId(context),
              "clusterId is missing from the gRPC context");
      final String tenant =
          requireContextValue(
              GatewayInterceptorUtil.getTenantId(context),
              "tenantId is missing from the gRPC context");
      consumeResponse(
          control,
          clusterResolver.resolve(cluster).thenCompose(ignored -> request.apply(cluster, tenant)),
          mapper,
          observer);
    } catch (final Exception error) {
      observer.onError(GatewayErrorMapper.toStatusException(error));
    }
  }

  private static String requireContextValue(final String value, final String missingMessage) {
    if (value == null || value.isBlank()) {
      throw new IllegalStateException(missingMessage);
    }
    return value;
  }

  private static <ResponseT, GrpcResponseT> void consumeResponse(
      final ConcurrencyControl control,
      final ActorFuture<ResponseT> responseFuture,
      final Function<ResponseT, GrpcResponseT> responseMapper,
      final StreamObserver<GrpcResponseT> responseObserver) {
    control.runOnCompletion(
        responseFuture,
        (response, error) -> {
          if (error != null) {
            responseObserver.onError(GatewayErrorMapper.toStatusException(error));
            return;
          }
          try {
            responseObserver.onNext(responseMapper.apply(response));
            responseObserver.onCompleted();
          } catch (final Exception mappingError) {
            responseObserver.onError(GatewayErrorMapper.toStatusException(mappingError));
          }
        });
  }

  private <ResponseT> void executeRequest(
      final StreamObserver<ResponseT> responseObserver,
      final BiConsumer<ConcurrencyControl, StreamObserver<ResponseT>> requestAction) {
    GatewayRequestActor.submit(
        requestScheduler, Context.current(), responseObserver, requestAction);
  }
}
