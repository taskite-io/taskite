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
package io.taskite.placementdriver.api.grpc;

import io.grpc.stub.StreamObserver;
import io.taskite.placementdriver.api.PlacementDriverEndpointManager;
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
import io.taskite.placementdriver.protocol.PlacementDriverGrpc;
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
import java.util.Objects;

/** Thin generated gRPC adapter delegating every public RPC to the endpoint manager. */
public final class PlacementDriverGrpcApi extends PlacementDriverGrpc.PlacementDriverImplBase {
  private final PlacementDriverEndpointManager endpointManager;

  public PlacementDriverGrpcApi(final PlacementDriverEndpointManager endpointManager) {
    this.endpointManager = Objects.requireNonNull(endpointManager, "endpointManager");
  }

  @Override
  public void updatePlacementDriverTopology(
      final UpdatePlacementDriverTopologyRequest request,
      final StreamObserver<UpdatePlacementDriverTopologyResponse> responseObserver) {
    endpointManager.updatePlacementDriverTopology(request, responseObserver);
  }

  @Override
  public void getClusterConfig(
      final GetClusterConfigRequest request,
      final StreamObserver<GetClusterConfigResponse> responseObserver) {
    endpointManager.getClusterConfig(request, responseObserver);
  }

  @Override
  public void getBrokerTopology(
      final GetBrokerTopologyRequest request,
      final StreamObserver<BrokerTopology> responseObserver) {
    endpointManager.getBrokerTopology(request, responseObserver);
  }

  @Override
  public void watchBrokerTopology(
      final GetBrokerTopologyRequest request,
      final StreamObserver<BrokerTopology> responseObserver) {
    endpointManager.watchBrokerTopology(request, responseObserver);
  }

  @Override
  public void watchPlacementDriverTopology(
      final WatchPlacementDriverTopologyRequest request,
      final StreamObserver<PlacementDriverTopology> responseObserver) {
    endpointManager.watchPlacementDriverClusterTopology(request, responseObserver);
  }

  /** Handles typed requests that PD must coordinate and distribute to Broker partitions. */
  @Override
  public void distributeCommand(
      final DistributeCommandRequest request,
      final StreamObserver<DistributeCommandResponse> responseObserver) {
    endpointManager.distributeCommand(request, responseObserver);
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
  public void registerBroker(
      final RegisterBrokerRequest request,
      final StreamObserver<RegisterBrokerResponse> responseObserver) {
    endpointManager.registerBroker(request, responseObserver);
  }

  @Override
  public void renewBroker(
      final RenewBrokerRequest request,
      final StreamObserver<RenewBrokerResponse> responseObserver) {
    endpointManager.renewBroker(request, responseObserver);
  }

  @Override
  public void deregisterBroker(
      final DeregisterBrokerRequest request,
      final StreamObserver<DeregisterBrokerResponse> responseObserver) {
    endpointManager.deregisterBroker(request, responseObserver);
  }

  @Override
  public void registerWorker(
      final RegisterWorkerRequest request,
      final StreamObserver<RegisterWorkerResponse> responseObserver) {
    endpointManager.registerWorker(request, responseObserver);
  }

  @Override
  public void renewWorker(
      final RenewWorkerRequest request,
      final StreamObserver<RenewWorkerResponse> responseObserver) {
    endpointManager.renewWorker(request, responseObserver);
  }

  @Override
  public void deregisterWorker(
      final DeregisterWorkerRequest request,
      final StreamObserver<DeregisterWorkerResponse> responseObserver) {
    endpointManager.deregisterWorker(request, responseObserver);
  }
}
