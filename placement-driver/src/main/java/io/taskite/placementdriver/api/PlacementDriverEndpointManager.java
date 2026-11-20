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
package io.taskite.placementdriver.api;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.ClusterConfigurationPatch;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.CreateClusterRequest;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.CreateJobSubscriptionMirrorRequest;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.PlacementDriverConfigurationPatch;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.RecoverQuorumRequest;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.RedistributeJobSubscriptionRequest;
import io.taskite.placementdriver.common.mapper.PlacementDriverTopologyMapper;
import io.taskite.placementdriver.protocol.BrokerTopology;
import io.taskite.placementdriver.protocol.CreateTimerSubscriptionRequest;
import io.taskite.placementdriver.protocol.CreateTimerSubscriptionResponse;
import io.taskite.placementdriver.protocol.DeleteTimerSubscriptionRequest;
import io.taskite.placementdriver.protocol.DeleteTimerSubscriptionResponse;
import io.taskite.placementdriver.protocol.DeregisterBrokerRequest;
import io.taskite.placementdriver.protocol.DeregisterBrokerResponse;
import io.taskite.placementdriver.protocol.DeregisterWorkerRequest;
import io.taskite.placementdriver.protocol.DeregisterWorkerResponse;
import io.taskite.placementdriver.protocol.DistributeCommandRequest;
import io.taskite.placementdriver.protocol.DistributeCommandResponse;
import io.taskite.placementdriver.protocol.GetBrokerTopologyRequest;
import io.taskite.placementdriver.protocol.GetClusterConfigRequest;
import io.taskite.placementdriver.protocol.GetClusterConfigResponse;
import io.taskite.placementdriver.protocol.PlacementDriverTopology;
import io.taskite.placementdriver.protocol.RegisterBrokerRequest;
import io.taskite.placementdriver.protocol.RegisterBrokerResponse;
import io.taskite.placementdriver.protocol.RegisterWorkerRequest;
import io.taskite.placementdriver.protocol.RegisterWorkerResponse;
import io.taskite.placementdriver.protocol.RenewBrokerRequest;
import io.taskite.placementdriver.protocol.RenewBrokerResponse;
import io.taskite.placementdriver.protocol.RenewWorkerRequest;
import io.taskite.placementdriver.protocol.RenewWorkerResponse;
import io.taskite.placementdriver.protocol.UpdatePlacementDriverTopologyRequest;
import io.taskite.placementdriver.protocol.UpdatePlacementDriverTopologyResponse;
import io.taskite.placementdriver.protocol.WatchPlacementDriverTopologyRequest;
import io.taskite.placementdriver.runtime.PlacementDriverCommandSubmitter;
import io.taskite.placementdriver.runtime.PlacementDriverNodeSnapshot;
import io.taskite.placementdriver.runtime.PlacementDriverRejectionException;
import io.taskite.placementdriver.worker.registry.WorkerRegistration;
import io.taskite.protocol.impl.record.value.placementdriver.job.JobYieldPlan;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.protocol.record.intent.placementdriver.BrokerIntent;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.protocol.record.intent.placementdriver.WorkerIntent;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.QueryType;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.topology.ClusterTopology;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.async.DeferredResult;

@Component
public final class PlacementDriverEndpointManager {
  private static final String RUNTIME_NOT_READY = "placement-driver runtime is not ready";
  private final PlacementDriverResponseMapper responseMapper = new PlacementDriverResponseMapper();
  private volatile Binding binding;

  public void bindRuntime(
      final PlacementDriverCommandSubmitter commandSubmitter,
      final TypedEventRegistry eventRegistry,
      final Supplier<PlacementDriverNodeSnapshot> nodeSnapshotSupplier,
      final BrokerTopologyWatchPort brokerTopologyWatchPort,
      final PlacementDriverClusterTopologyWatchPort clusterTopologyWatchPort,
      final WorkerRegistryQueryPort workerRegistryQueryPort,
      final Function<MemberTopologyRecord, ActorFuture<MemberTopologyRecord>> topologyUpdater) {
    unbindRuntime();
    binding =
        new Binding(
            commandSubmitter,
            new PlacementDriverRequestMapper(eventRegistry),
            nodeSnapshotSupplier,
            brokerTopologyWatchPort,
            clusterTopologyWatchPort,
            workerRegistryQueryPort,
            topologyUpdater);
  }

  public void unbindRuntime() {
    binding = null;
  }

  public DeferredResult<ResponseEntity<Object>> queryHealth() {
    final Binding current = requireBinding();
    return submit(
        current,
        current
            .requestMapper()
            .toPlacementDriverQueryRequest(
                QueryType.PLACEMENT_DRIVER_HEALTH, current.nodeSnapshotSupplier().get()),
        responseMapper::toHealthResult,
        "");
  }

  public DeferredResult<ResponseEntity<Object>> queryPlacementDriverTopology() {
    final Binding current = requireBinding();
    return submit(
        current,
        current
            .requestMapper()
            .toPlacementDriverQueryRequest(
                QueryType.PLACEMENT_DRIVER_TOPOLOGY, current.nodeSnapshotSupplier().get()),
        responseMapper::toPlacementDriverTopologyResult,
        "");
  }

  public DeferredResult<ResponseEntity<Object>> queryPlacementDriverConfiguration() {
    final Binding current = requireBinding();
    return submit(
        current,
        current.requestMapper().toQueryRequest(QueryType.PLACEMENT_DRIVER_CONFIGURATION, "", ""),
        responseMapper::toPlacementDriverConfigurationResult,
        "");
  }

  public DeferredResult<ResponseEntity<Object>> patchPlacementDriverConfiguration(
      final PlacementDriverConfigurationPatch request) {
    final Binding current = requireBinding();
    return submit(
        current,
        current.requestMapper().toPlacementDriverMemberChangeRequest(request),
        responseMapper::toPlacementDriverMemberChangeResult,
        "");
  }

  public DeferredResult<ResponseEntity<Object>> queryPlacementDriverMemberChange(
      final String changeId) {
    final Binding current = requireBinding();
    return submit(
        current,
        current.requestMapper().toQueryRequest(QueryType.PLACEMENT_DRIVER_CHANGE, "", changeId),
        responseMapper::toPlacementDriverMemberChangeStatusResult,
        "");
  }

  public DeferredResult<ResponseEntity<Object>> createBrokerCluster(
      final CreateClusterRequest request) {
    final Binding current = requireBinding();
    return submit(
        current,
        current.requestMapper().toCreateClusterRequest(request),
        responseMapper::toCreateClusterResult,
        request == null ? "" : request.clusterId());
  }

  public DeferredResult<ResponseEntity<Object>> queryBrokerClusters() {
    final Binding current = requireBinding();
    return submit(
        current,
        current.requestMapper().toQueryRequest(QueryType.BROKER_CLUSTERS, "", ""),
        responseMapper::toBrokerClustersResult,
        "");
  }

  public DeferredResult<ResponseEntity<Object>> createJobSubscriptionMirror(
      final CreateJobSubscriptionMirrorRequest request) {
    final Binding current = requireBinding();
    return submit(
        current,
        current.requestMapper().toCreateJobSubscriptionMirrorRequest(request),
        responseMapper::toCreateJobSubscriptionMirrorResult,
        request == null ? "" : request.sourceClusterId());
  }

  public DeferredResult<ResponseEntity<Object>> createJobSubscription(
      final PlacementDriverHttpRequestModel.CreateJobSubscriptionRequest request) {
    final Binding current = requireBinding();
    final String clusterId = request == null ? "" : request.clusterId();
    try {
      return submit(
          current,
          current.requestMapper().toCreateJobSubscriptionRequest(request),
          responseMapper::toCreateJobSubscriptionResult,
          clusterId);
    } catch (final RuntimeException error) {
      return completed(responseMapper.toHttpError(error, clusterId));
    }
  }

  public DeferredResult<ResponseEntity<Object>> createTimerSubscription(
      final PlacementDriverHttpRequestModel.CreateTimerSubscriptionRequest request) {
    final Binding current = requireBinding();
    final String clusterId = request == null ? "" : request.clusterId();
    try {
      return submit(
          current,
          current.requestMapper().toCreateTimerSubscriptionRequest(request),
          responseMapper::toCreateTimerSubscriptionResult,
          clusterId);
    } catch (final RuntimeException error) {
      return completed(responseMapper.toHttpError(error, clusterId));
    }
  }

  public DeferredResult<ResponseEntity<Object>> queryTimerSubscriptions() {
    final Binding current = requireBinding();
    return submit(
        current,
        current.requestMapper().toQueryRequest(QueryType.TIMER_SUBSCRIPTIONS, "", ""),
        responseMapper::toTimerSubscriptionsResult,
        "");
  }

  public DeferredResult<ResponseEntity<Object>> deleteJobSubscription(
      final String subscriptionKey, final String clusterId, final String tenantId) {
    final Binding current = requireBinding();
    try {
      return submit(
          current,
          current
              .requestMapper()
              .toDeleteJobSubscriptionRequest(subscriptionKey, clusterId, tenantId),
          responseMapper::toDeleteJobSubscriptionResult,
          clusterId);
    } catch (final RuntimeException error) {
      return completed(responseMapper.toHttpError(error, clusterId));
    }
  }

  public DeferredResult<ResponseEntity<Object>> deleteTimerSubscription(
      final String timerSubscriptionKey, final String clusterId, final String tenantId) {
    final Binding current = requireBinding();
    try {
      return submit(
          current,
          current
              .requestMapper()
              .toDeleteTimerSubscriptionRequest(timerSubscriptionKey, clusterId, tenantId),
          responseMapper::toDeleteTimerSubscriptionResult,
          clusterId);
    } catch (final RuntimeException error) {
      return completed(responseMapper.toHttpError(error, clusterId));
    }
  }

  public DeferredResult<ResponseEntity<Object>> queryJobSubscriptionMirrors(
      final String sourceClusterId, final String targetClusterId) {
    final Binding current = requireBinding();
    return submit(
        current,
        current
            .requestMapper()
            .toJobSubscriptionMirrorQueryRequest("", sourceClusterId, targetClusterId),
        responseMapper::toJobSubscriptionMirrorQueryResult,
        sourceClusterId == null ? "" : sourceClusterId);
  }

  public DeferredResult<ResponseEntity<Object>> queryJobSubscriptionMirror(final String mirrorKey) {
    final Binding current = requireBinding();
    try {
      return submit(
          current,
          current.requestMapper().toJobSubscriptionMirrorQueryRequest(mirrorKey, "", ""),
          responseMapper::toJobSubscriptionMirrorResult,
          "");
    } catch (final RuntimeException error) {
      return completed(responseMapper.toHttpError(error, ""));
    }
  }

  public DeferredResult<ResponseEntity<Object>> deleteJobSubscriptionMirror(
      final String mirrorKey) {
    final Binding current = requireBinding();
    try {
      return submit(
          current,
          current.requestMapper().toDeleteJobSubscriptionMirrorRequest(mirrorKey),
          responseMapper::toDeleteJobSubscriptionMirrorResult,
          "");
    } catch (final RuntimeException error) {
      return completed(responseMapper.toHttpError(error, ""));
    }
  }

  public DeferredResult<ResponseEntity<Object>> queryJobSubscriptionDistribution(
      final String distributionKey) {
    final Binding current = requireBinding();
    try {
      return submit(
          current,
          current.requestMapper().toJobSubscriptionDistributionQueryRequest(distributionKey),
          responseMapper::toJobSubscriptionDistributionResult,
          "");
    } catch (final RuntimeException error) {
      return completed(responseMapper.toHttpError(error, ""));
    }
  }

  public DeferredResult<ResponseEntity<Object>> queryJobSubscriptionDistributions(
      final String jobType, final String distributionKey, final String clusterId) {
    final Binding current = requireBinding();
    try {
      return submit(
          current,
          current
              .requestMapper()
              .toJobSubscriptionDistributionListRequest(jobType, distributionKey, clusterId),
          responseMapper::toJobSubscriptionDistributionListResult,
          "");
    } catch (final RuntimeException error) {
      return completed(responseMapper.toHttpError(error, ""));
    }
  }

  public DeferredResult<ResponseEntity<Object>> redistributeJobSubscription(
      final String distributionKey, final RedistributeJobSubscriptionRequest request) {
    final Binding current = requireBinding();
    try {
      return submit(
          current,
          current.requestMapper().toRedistributeJobSubscriptionRequest(distributionKey, request),
          responseMapper::toRedistributeJobSubscriptionResult,
          "");
    } catch (final RuntimeException error) {
      return completed(responseMapper.toHttpError(error, ""));
    }
  }

  public DeferredResult<ResponseEntity<Object>> deleteBrokerCluster(final String clusterId) {
    final Binding current = requireBinding();
    return submit(
        current,
        current.requestMapper().toDeleteClusterRequest(clusterId),
        responseMapper::toDeleteClusterResult,
        clusterId);
  }

  public DeferredResult<ResponseEntity<Object>> queryBrokerClusterConfiguration(
      final String clusterId) {
    final Binding current = requireBinding();
    return submit(
        current,
        current
            .requestMapper()
            .toQueryRequest(QueryType.BROKER_CLUSTER_CONFIGURATION, clusterId, ""),
        responseMapper::toClusterConfigurationResult,
        clusterId);
  }

  public DeferredResult<ResponseEntity<Object>> patchBrokerClusterConfiguration(
      final String clusterId, final ClusterConfigurationPatch request) {
    final Binding current = requireBinding();
    if (request != null && request.brokers() != null && request.partitions() == null) {
      return submit(
          current,
          current.requestMapper().toBrokerMemberChangeRequest(clusterId, request),
          responseMapper::toMemberChangeResult,
          clusterId);
    }
    return submit(
        current,
        current.requestMapper().toPatchClusterRequest(clusterId, request),
        responseMapper::toConfigurationChangeResult,
        clusterId);
  }

  public DeferredResult<ResponseEntity<Object>> queryBrokerTopology(final String clusterId) {
    final Binding current = requireBinding();
    return submit(
        current,
        current.requestMapper().toQueryRequest(QueryType.BROKER_TOPOLOGY, clusterId, ""),
        responseMapper::toBrokerTopologyResult,
        clusterId);
  }

  public DeferredResult<ResponseEntity<Object>> recoverBrokerTopologyQuorum(
      final String clusterId, final String partitionId, final RecoverQuorumRequest request) {
    final Binding current = requireBinding();
    try {
      return submit(
          current,
          current.requestMapper().toRecoverQuorumRequest(clusterId, partitionId, request),
          responseMapper::toConfigurationChangeResult,
          clusterId);
    } catch (final RuntimeException error) {
      return completed(responseMapper.toHttpError(error, clusterId));
    }
  }

  public DeferredResult<ResponseEntity<Object>> rebalanceBrokerTopology(final String clusterId) {
    final Binding current = requireBinding();
    return submit(
        current,
        current.requestMapper().toRebalanceClusterRequest(clusterId),
        responseMapper::toConfigurationChangeResult,
        clusterId);
  }

  public DeferredResult<ResponseEntity<Object>> queryBrokerClusterChange(
      final String clusterId, final String changeId) {
    final Binding current = requireBinding();
    return submit(
        current,
        current
            .requestMapper()
            .toQueryRequest(QueryType.BROKER_CLUSTER_CHANGE, clusterId, changeId),
        responseMapper::toClusterChangeResult,
        clusterId);
  }

  public DeferredResult<ResponseEntity<Object>> cancelBrokerClusterChange(
      final String clusterId, final String changeId) {
    final Binding current = requireBinding();
    return submit(
        current,
        current.requestMapper().toCancelClusterChangeRequest(clusterId, changeId),
        responseMapper::toCancelChangeResult,
        clusterId);
  }

  public void submitCommand(final byte[] request, final StreamObserver<byte[]> responseObserver) {
    final Binding current = bindingOrReportUnavailable(responseObserver);
    if (current == null) {
      return;
    }
    try {
      final var command = current.requestMapper().toPlacementDriverCommand(request);
      final var intent = command.metadata().getIntent();
      if (intent instanceof BrokerPartitionChangeIntent
          || intent instanceof JobYieldIntent
          || intent == TopologyIntent.UPDATE
          || (intent instanceof MemberChangeIntent && intent != MemberChangeIntent.REQUEST)
          || intent == BrokerIntent.MARK_OFFLINE
          || intent == BrokerIntent.DRAIN
          || intent == WorkerIntent.MARK_OFFLINE
          || intent == WorkerIntent.DELETE
          || intent == CommandDistributionIntent.ACKNOWLEDGE
          || intent == CommandDistributionIntent.COMPLETE) {
        throw new IllegalArgumentException(
            "Internal command requires the internal submission port");
      }
      current
          .commandSubmitter()
          .submitPlacementDriverCommand(command)
          .onComplete(
              (response, error) -> {
                if (error != null) {
                  responseObserver.onError(responseMapper.toGrpcStatus(error).asRuntimeException());
                } else {
                  responseObserver.onNext(responseMapper.toCommandResponse(response));
                  responseObserver.onCompleted();
                }
              });
    } catch (final Throwable error) {
      responseObserver.onError(responseMapper.toGrpcStatus(error).asRuntimeException());
    }
  }

  public void updatePlacementDriverTopology(
      final UpdatePlacementDriverTopologyRequest request,
      final StreamObserver<UpdatePlacementDriverTopologyResponse> responseObserver) {
    final Binding current = bindingOrReportUnavailable(responseObserver);
    if (current == null) {
      return;
    }
    try {
      if (!request.hasTopology()) {
        throw new IllegalArgumentException("topology is required");
      }
      current
          .topologyUpdater()
          .apply(PlacementDriverTopologyMapper.fromProto(request.getTopology()))
          .onComplete(
              (topology, error) -> {
                if (error != null) {
                  responseObserver.onError(responseMapper.toGrpcStatus(error).asRuntimeException());
                  return;
                }
                try {
                  responseObserver.onNext(
                      UpdatePlacementDriverTopologyResponse.newBuilder()
                          .setTopology(PlacementDriverTopologyMapper.toProto(topology))
                          .build());
                  responseObserver.onCompleted();
                } catch (final RuntimeException mappingError) {
                  responseObserver.onError(
                      responseMapper.toGrpcStatus(mappingError).asRuntimeException());
                }
              });
    } catch (final RuntimeException error) {
      responseObserver.onError(responseMapper.toGrpcStatus(error).asRuntimeException());
    }
  }

  public void getClusterConfig(
      final GetClusterConfigRequest request,
      final StreamObserver<GetClusterConfigResponse> responseObserver) {
    submitGrpcCommand(
        request,
        PlacementDriverRequestMapper::toGetClusterConfigRequest,
        responseMapper::toGetClusterConfigResponse,
        responseObserver);
  }

  public void getBrokerTopology(
      final GetBrokerTopologyRequest request,
      final StreamObserver<BrokerTopology> responseObserver) {
    submitGrpcCommand(
        request,
        PlacementDriverRequestMapper::toGetBrokerTopologyRequest,
        responseMapper::toBrokerTopology,
        responseObserver);
  }

  /**
   * Accepts a typed request that PD must persist and distribute to the relevant Broker partitions.
   */
  public void distributeCommand(
      final DistributeCommandRequest request,
      final StreamObserver<DistributeCommandResponse> responseObserver) {
    submitGrpcCommand(
        request,
        PlacementDriverRequestMapper::toDistributeCommandRequest,
        responseMapper::toDistributeCommandResponse,
        responseObserver);
  }

  public void createTimerSubscription(
      final CreateTimerSubscriptionRequest request,
      final StreamObserver<CreateTimerSubscriptionResponse> observer) {
    submitGrpcCommand(
        request,
        PlacementDriverRequestMapper::toCreateTimerSubscriptionRequest,
        responseMapper::toCreateTimerSubscriptionResponse,
        observer);
  }

  public void deleteTimerSubscription(
      final DeleteTimerSubscriptionRequest request,
      final StreamObserver<DeleteTimerSubscriptionResponse> observer) {
    submitGrpcCommand(
        request,
        PlacementDriverRequestMapper::toDeleteTimerSubscriptionRequest,
        responseMapper::toDeleteTimerSubscriptionResponse,
        observer);
  }

  public void registerBroker(
      final RegisterBrokerRequest request,
      final StreamObserver<RegisterBrokerResponse> responseObserver) {
    submitGrpcCommand(
        request,
        PlacementDriverRequestMapper::toRegisterBrokerRequest,
        responseMapper::toRegisterBrokerResponse,
        responseObserver);
  }

  public void renewBroker(
      final RenewBrokerRequest request,
      final StreamObserver<RenewBrokerResponse> responseObserver) {
    submitGrpcCommand(
        request,
        PlacementDriverRequestMapper::toRenewBrokerRequest,
        responseMapper::toRenewBrokerResponse,
        responseObserver);
  }

  public void deregisterBroker(
      final DeregisterBrokerRequest request,
      final StreamObserver<DeregisterBrokerResponse> responseObserver) {
    submitGrpcCommand(
        request,
        PlacementDriverRequestMapper::toDeregisterBrokerRequest,
        responseMapper::toDeregisterBrokerResponse,
        responseObserver);
  }

  public void registerWorker(
      final RegisterWorkerRequest request,
      final StreamObserver<RegisterWorkerResponse> responseObserver) {
    submitGrpcCommand(
        request,
        PlacementDriverRequestMapper::toRegisterWorkerRequest,
        responseMapper::toRegisterWorkerResponse,
        responseObserver);
  }

  public void renewWorker(
      final RenewWorkerRequest request,
      final StreamObserver<RenewWorkerResponse> responseObserver) {
    submitGrpcCommand(
        request,
        PlacementDriverRequestMapper::toRenewWorkerRequest,
        responseMapper::toRenewWorkerResponse,
        responseObserver);
  }

  public void deregisterWorker(
      final DeregisterWorkerRequest request,
      final StreamObserver<DeregisterWorkerResponse> responseObserver) {
    submitGrpcCommand(
        request,
        PlacementDriverRequestMapper::toDeregisterWorkerRequest,
        responseMapper::toDeregisterWorkerResponse,
        responseObserver);
  }

  public DeferredResult<ResponseEntity<Object>> queryWorkers(
      final String clusterId, final String tenantId, final String name) {
    final Binding current = requireBinding();
    final CompletableActorFuture<PlacementDriverEndpointResult> result =
        new CompletableActorFuture<>();
    try {
      current
          .workerRegistryQueryPort()
          .queryWorkers(clusterId, tenantId, name)
          .onComplete(
              (workers, error) -> {
                if (error != null) {
                  result.complete(responseMapper.toHttpError(error, clusterId));
                } else {
                  result.complete(responseMapper.toWorkerRegistryResult(workers));
                }
              });
    } catch (final Throwable error) {
      result.complete(responseMapper.toHttpError(error, clusterId));
    }
    return responseMapper.toHttpResponse(result);
  }

  public void watchBrokerTopology(
      final GetBrokerTopologyRequest request,
      final StreamObserver<BrokerTopology> responseObserver) {
    final Binding current = bindingOrReportUnavailable(responseObserver);
    if (current == null) {
      return;
    }
    final AtomicReference<AutoCloseable> subscription = new AtomicReference<>();
    subscription.set(
        current
            .brokerTopologyWatchPort()
            .watchBrokerTopology(
                request.getClusterId(),
                topology -> responseObserver.onNext(responseMapper.toBrokerTopology(topology)),
                clusterId ->
                    responseObserver.onError(
                        Status.NOT_FOUND
                            .withDescription("CLUSTER_NOT_REGISTERED: " + clusterId)
                            .asRuntimeException())));
    closeOnCancel(responseObserver, subscription);
  }

  public DeferredResult<ResponseEntity<Object>> queryJobYields(
      String clusterId, String tenantId, String workerId, String workerLeaseToken, String yieldId) {
    final Binding current = requireBinding();
    final CompletableActorFuture<PlacementDriverEndpointResult> result =
        new CompletableActorFuture<>();
    try {
      current
          .workerRegistryQueryPort()
          .queryYields(clusterId, tenantId, workerId, workerLeaseToken, yieldId)
          .onComplete(
              (plans, error) -> {
                if (error != null) {
                  result.complete(responseMapper.toHttpError(error, clusterId));
                } else if (yieldId != null && !yieldId.isBlank() && plans.isEmpty()) {
                  result.complete(
                      new PlacementDriverEndpointResult(404, Map.of("code", "YIELD_NOT_FOUND")));
                } else {
                  result.complete(
                      responseMapper.toJobYieldResult(
                          plans, yieldId != null && !yieldId.isBlank()));
                }
              });
    } catch (Throwable error) {
      result.complete(responseMapper.toHttpError(error, clusterId));
    }
    return responseMapper.toHttpResponse(result);
  }

  public void watchPlacementDriverClusterTopology(
      final WatchPlacementDriverTopologyRequest request,
      final StreamObserver<PlacementDriverTopology> responseObserver) {
    final Binding current = bindingOrReportUnavailable(responseObserver);
    if (current == null) {
      return;
    }
    final AtomicReference<AutoCloseable> subscription = new AtomicReference<>();
    subscription.set(
        current
            .clusterTopologyWatchPort()
            .watchPlacementDriverClusterTopology(
                topology ->
                    responseObserver.onNext(responseMapper.toPlacementDriverTopology(topology))));
    closeOnCancel(responseObserver, subscription);
  }

  private <T extends UnifiedRecordValue> DeferredResult<ResponseEntity<Object>> submit(
      final Binding current,
      final PlacementDriverApiRequest<T> request,
      final Function<PlacementDriverApiResponse<T>, PlacementDriverEndpointResult> mapper,
      final String clusterId) {
    final CompletableActorFuture<PlacementDriverEndpointResult> result =
        new CompletableActorFuture<>();
    final ActorFuture<PlacementDriverApiResponse<T>> response;
    try {
      response = submitLocally(current, request);
    } catch (final Throwable error) {
      result.complete(responseMapper.toHttpError(error, clusterId));
      return responseMapper.toHttpResponse(result);
    }
    response.onComplete(
        (value, error) -> {
          if (error != null) {
            result.complete(responseMapper.toHttpError(error, clusterId));
          } else {
            result.complete(mapper.apply(value));
          }
        });
    return responseMapper.toHttpResponse(result);
  }

  private <GrpcRequestT, T extends UnifiedRecordValue, GrpcResponseT> void submitGrpcCommand(
      final GrpcRequestT grpcRequest,
      final BiFunction<PlacementDriverRequestMapper, GrpcRequestT, PlacementDriverApiRequest<T>>
          requestMapper,
      final Function<PlacementDriverApiResponse<T>, GrpcResponseT> grpcResponseMapper,
      final StreamObserver<GrpcResponseT> responseObserver) {
    final Binding current = bindingOrReportUnavailable(responseObserver);
    if (current == null) {
      return;
    }
    try {
      final PlacementDriverApiRequest<T> request =
          requestMapper.apply(current.requestMapper(), grpcRequest);
      submitLocally(current, request)
          .onComplete(
              (response, error) -> {
                if (error != null) {
                  responseObserver.onError(responseMapper.toGrpcStatus(error).asRuntimeException());
                  return;
                }
                try {
                  responseObserver.onNext(grpcResponseMapper.apply(response));
                  responseObserver.onCompleted();
                } catch (final Throwable mappingError) {
                  responseObserver.onError(
                      responseMapper.toGrpcStatus(mappingError).asRuntimeException());
                }
              });
    } catch (final Throwable error) {
      responseObserver.onError(responseMapper.toGrpcStatus(error).asRuntimeException());
    }
  }

  private <T extends UnifiedRecordValue> ActorFuture<PlacementDriverApiResponse<T>> submitLocally(
      final Binding current, final PlacementDriverApiRequest<T> request) {
    final CompletableActorFuture<PlacementDriverApiResponse<T>> result =
        new CompletableActorFuture<>();
    try {
      current
          .commandSubmitter()
          .submitPlacementDriverCommand(current.requestMapper().toPlacementDriverCommand(request))
          .onComplete(
              (record, error) -> {
                if (error != null) {
                  result.completeExceptionally(error);
                  return;
                }
                try {
                  if (record.metadata().getRecordType() == RecordType.COMMAND_REJECTION) {
                    result.completeExceptionally(
                        new PlacementDriverRejectionException(
                            record.metadata().getRejectionType(),
                            record.metadata().getRejectionReason()));
                  } else {
                    result.complete(
                        new PlacementDriverApiResponse<>(
                            request.mapResponse(record), record.key()));
                  }
                } catch (final Throwable mappingError) {
                  result.completeExceptionally(mappingError);
                }
              });
    } catch (final Throwable error) {
      result.completeExceptionally(error);
    }
    return result;
  }

  private static DeferredResult<ResponseEntity<Object>> completed(
      final PlacementDriverEndpointResult result) {
    final DeferredResult<ResponseEntity<Object>> deferred = new DeferredResult<>();
    deferred.setResult(ResponseEntity.status(result.status()).body(result.body()));
    return deferred;
  }

  private Binding requireBinding() {
    final Binding current = binding;
    if (current == null) {
      throw new IllegalStateException("placement-driver endpoint manager is not bound");
    }
    return current;
  }

  private Binding bindingOrReportUnavailable(final StreamObserver<?> responseObserver) {
    final Binding current = binding;
    if (current == null) {
      responseObserver.onError(
          Status.UNAVAILABLE.withDescription(RUNTIME_NOT_READY).asRuntimeException());
    }
    return current;
  }

  private static void closeOnCancel(
      final StreamObserver<?> responseObserver, final AtomicReference<AutoCloseable> subscription) {
    if (responseObserver instanceof ServerCallStreamObserver<?> serverObserver) {
      serverObserver.setOnCancelHandler(() -> closeSubscription(subscription.get()));
    }
  }

  private static void closeSubscription(final AutoCloseable subscription) {
    if (subscription == null) {
      return;
    }
    try {
      subscription.close();
    } catch (final Exception ignored) {
      // Stream cancellation is best effort.
    }
  }

  @FunctionalInterface
  public interface BrokerTopologyWatchPort {
    AutoCloseable watchBrokerTopology(
        String clusterId,
        Consumer<ClusterTopology> topologyListener,
        Consumer<String> unavailableListener);
  }

  @FunctionalInterface
  public interface PlacementDriverClusterTopologyWatchPort {
    AutoCloseable watchPlacementDriverClusterTopology(
        Consumer<MemberTopologyRecord> topologyListener);
  }

  @FunctionalInterface
  public interface WorkerRegistryQueryPort {
    ActorFuture<List<WorkerRegistration>> queryWorkers(
        String clusterId, String tenantId, String name);

    default ActorFuture<List<JobYieldPlan>> queryYields(
        String clusterId,
        String tenantId,
        String workerId,
        String workerLeaseToken,
        String yieldId) {
      return CompletableActorFuture.completed(List.of());
    }
  }

  private record Binding(
      PlacementDriverCommandSubmitter commandSubmitter,
      PlacementDriverRequestMapper requestMapper,
      Supplier<PlacementDriverNodeSnapshot> nodeSnapshotSupplier,
      BrokerTopologyWatchPort brokerTopologyWatchPort,
      PlacementDriverClusterTopologyWatchPort clusterTopologyWatchPort,
      WorkerRegistryQueryPort workerRegistryQueryPort,
      Function<MemberTopologyRecord, ActorFuture<MemberTopologyRecord>> topologyUpdater) {
    private Binding {
      Objects.requireNonNull(commandSubmitter, "commandSubmitter");
      Objects.requireNonNull(requestMapper, "requestMapper");
      Objects.requireNonNull(nodeSnapshotSupplier, "nodeSnapshotSupplier");
      Objects.requireNonNull(brokerTopologyWatchPort, "brokerTopologyWatchPort");
      Objects.requireNonNull(clusterTopologyWatchPort, "clusterTopologyWatchPort");
      Objects.requireNonNull(workerRegistryQueryPort, "workerRegistryQueryPort");
      Objects.requireNonNull(topologyUpdater, "topologyUpdater");
    }
  }
}
