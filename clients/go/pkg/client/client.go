// Copyright (c) 2026 lzgabel and Taskite contributors
//
// Licensed under the Taskite Community License, Version 1.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
//     https://github.com/taskite-io/taskite/blob/main/LICENSE
//
// The Software is source-available for non-commercial use and limited evaluation
// use. Other commercial use requires a separate written license. The Software is
// provided "AS IS", without warranties or conditions of any kind.

package client

import (
	"context"
	"errors"

	"github.com/taskite-io/taskite/clients/go/pkg/commands"
	"github.com/taskite-io/taskite/clients/go/pkg/pb"
	pdpb "github.com/taskite-io/taskite/clients/go/pkg/pb/placementdriver"
	"github.com/taskite-io/taskite/clients/go/pkg/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	ClusterIDHeader = "x-cluster-id"
	TenantIDHeader  = commands.TenantIDHeader
)

var Version = "0.1.0"

type Client interface {
	NewCreateJobCommand() commands.CreateJobCommandStep1
	NewCancelJobCommand() commands.CancelJobCommandStep1
	NewCreateJobSubscriptionCommand() commands.CreateJobSubscriptionCommandStep1
	NewDeleteJobSubscriptionCommand() commands.DeleteJobSubscriptionCommandStep1
	NewCreateTimerSubscriptionCommand() commands.CreateTimerSubscriptionCommandStep1
	NewDeleteTimerSubscriptionCommand() commands.DeleteTimerSubscriptionCommandStep1
	NewUpdateJobRetriesCommand() commands.UpdateJobRetriesCommandStep1
	NewUpdateJobPriorityCommand() commands.UpdateJobPriorityCommandStep1
	NewUpdateJobTimeoutCommand() commands.UpdateJobTimeoutCommandStep1
	NewActivateJobsCommand() commands.ActivateJobsCommandStep1
	NewCompleteJobCommand() commands.CompleteJobCommandStep1
	NewFailJobCommand() commands.FailJobCommandStep1
	NewQueryJobCommand() commands.QueryJobCommandStep1
	NewQueryIncidentCommand() commands.QueryIncidentCommandStep1
	NewResolveIncidentCommand() commands.ResolveIncidentCommandStep1
	NewSetVariablesCommand() commands.SetVariablesCommandStep1
	NewJobWorker() worker.JobWorkerBuilderStep1
	Close() error
}

type ClientImpl struct {
	gateway             pb.GatewayClient
	workerRegistry      worker.RegistryClient
	gatewayConnection   *grpc.ClientConn
	placementConnection *grpc.ClientConn
	registryConfig      worker.RegistryConfig
}

func NewClient(config ClientConfig) (Client, error) {
	resolved, err := resolveConfig(config)
	if err != nil {
		return nil, err
	}
	options := dialOptions(resolved)
	gatewayConnection, err := grpc.NewClient(grpcAddress(resolved.Gateway.Address), options...)
	if err != nil {
		return nil, err
	}
	placementConnection, err := grpc.NewClient(grpcAddress(resolved.PlacementDriver.Address), options...)
	if err != nil {
		_ = gatewayConnection.Close()
		return nil, err
	}
	return &ClientImpl{
		gateway:             pb.NewGatewayClient(gatewayConnection),
		workerRegistry:      newPlacementDriverWorkerRegistry(placementConnection),
		gatewayConnection:   gatewayConnection,
		placementConnection: placementConnection,
		registryConfig: worker.RegistryConfig{
			ClusterID:     resolved.ClusterID,
			TenantID:      resolved.TenantID,
			ClientVersion: Version,
			Registration:  resolved.Worker.Registration,
		},
	}, nil
}

func (client *ClientImpl) NewActivateJobsCommand() commands.ActivateJobsCommandStep1 {
	return commands.NewActivateJobsCommand(client.gateway)
}

func (client *ClientImpl) NewCompleteJobCommand() commands.CompleteJobCommandStep1 {
	return commands.NewCompleteJobCommand(client.gateway)
}

func (client *ClientImpl) NewFailJobCommand() commands.FailJobCommandStep1 {
	return commands.NewFailJobCommand(client.gateway)
}

func (client *ClientImpl) NewQueryJobCommand() commands.QueryJobCommandStep1 {
	return commands.NewQueryJobCommand(client.gateway)
}

func (client *ClientImpl) NewQueryIncidentCommand() commands.QueryIncidentCommandStep1 {
	return commands.NewQueryIncidentCommand(client.gateway)
}

func (client *ClientImpl) NewResolveIncidentCommand() commands.ResolveIncidentCommandStep1 {
	return commands.NewResolveIncidentCommand(client.gateway)
}

func (client *ClientImpl) NewSetVariablesCommand() commands.SetVariablesCommandStep1 {
	return commands.NewSetVariablesCommand(client.gateway)
}

func (client *ClientImpl) NewJobWorker() worker.JobWorkerBuilderStep1 {
	return worker.NewRegisteredJobWorkerBuilder(
		client.gateway,
		client.workerRegistry,
		client,
		client.registryConfig,
	)
}

func (client *ClientImpl) Close() error {
	return errors.Join(
		client.gatewayConnection.Close(),
		client.placementConnection.Close(),
	)
}

func requestUnaryInterceptor(clusterID, tenantID string) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		request any,
		reply any,
		connection *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		options ...grpc.CallOption,
	) error {
		return invoker(
			withRequestMetadata(ctx, clusterID, tenantID),
			method,
			request,
			reply,
			connection,
			options...,
		)
	}
}

func requestStreamInterceptor(clusterID, tenantID string) grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		description *grpc.StreamDesc,
		connection *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		options ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		return streamer(
			withRequestMetadata(ctx, clusterID, tenantID),
			description,
			connection,
			method,
			options...,
		)
	}
}

func withRequestMetadata(ctx context.Context, clusterID, tenantID string) context.Context {
	outgoing, _ := metadata.FromOutgoingContext(ctx)
	outgoing = outgoing.Copy()
	outgoing.Set(ClusterIDHeader, clusterID)
	if len(outgoing.Get(TenantIDHeader)) == 0 {
		outgoing.Set(TenantIDHeader, tenantID)
	}
	return metadata.NewOutgoingContext(ctx, outgoing)
}

type placementDriverWorkerRegistry struct {
	client pdpb.PlacementDriverClient
}

func newPlacementDriverWorkerRegistry(
	connection grpc.ClientConnInterface,
) worker.RegistryClient {
	return &placementDriverWorkerRegistry{
		client: pdpb.NewPlacementDriverClient(connection),
	}
}

func (registry *placementDriverWorkerRegistry) Register(
	ctx context.Context,
	request worker.RegistrationRequest,
) (worker.RegistrationResult, error) {
	response, err := registry.client.RegisterWorker(
		ctx,
		registerWorkerRequest(request),
		grpc.WaitForReady(true),
	)
	if err != nil {
		return worker.RegistrationResult{}, err
	}
	return worker.RegistrationResult{
		WorkerLeaseToken:         response.GetWorkerLeaseToken(),
		LeaseDeadlineEpochMillis: response.GetLeaseDeadlineEpochMillis(),
	}, nil
}

func (registry *placementDriverWorkerRegistry) Renew(
	ctx context.Context,
	request worker.RegistrationRequest,
) (worker.RegistrationResult, error) {
	response, err := registry.client.RenewWorker(
		ctx,
		renewWorkerRequest(request),
		grpc.WaitForReady(true),
	)
	if err != nil {
		return worker.RegistrationResult{}, err
	}
	return worker.RegistrationResult{
		WorkerLeaseToken:         response.GetWorkerLeaseToken(),
		LeaseDeadlineEpochMillis: response.GetLeaseDeadlineEpochMillis(),
	}, nil
}

func (registry *placementDriverWorkerRegistry) Deregister(
	ctx context.Context,
	request worker.DeregisterRequest,
) error {
	_, err := registry.client.DeregisterWorker(
		ctx,
		&pdpb.DeregisterWorkerRequest{
			ClusterId:        request.ClusterID,
			TenantIds:        request.TenantIDs,
			WorkerId:         request.WorkerID,
			Name:             request.Name,
			WorkerLeaseToken: request.WorkerLeaseToken,
		},
		grpc.WaitForReady(true),
	)
	return err
}

func registerWorkerRequest(request worker.RegistrationRequest) *pdpb.RegisterWorkerRequest {
	return &pdpb.RegisterWorkerRequest{
		ClusterId:            request.ClusterID,
		TenantIds:            request.TenantIDs,
		WorkerId:             request.WorkerID,
		Name:                 request.Name,
		JobType:              request.JobType,
		MaxJobsActive:        int32(request.MaxJobsActive),
		ActiveJobs:           int32(request.ActiveJobs),
		Concurrency:          int32(request.Concurrency),
		TimeoutMillis:        request.TimeoutMillis,
		RequestTimeoutMillis: request.RequestTimeoutMillis,
		PollIntervalMillis:   request.PollIntervalMillis,
		FetchVariables:       request.FetchVariables,
		ClientVersion:        request.ClientVersion,
		RenewalTimeoutMillis: request.RenewalTimeoutMillis,
	}
}

func renewWorkerRequest(request worker.RegistrationRequest) *pdpb.RenewWorkerRequest {
	return &pdpb.RenewWorkerRequest{
		WorkerLeaseToken:     request.WorkerLeaseToken,
		ClusterId:            request.ClusterID,
		TenantIds:            request.TenantIDs,
		WorkerId:             request.WorkerID,
		Name:                 request.Name,
		JobType:              request.JobType,
		MaxJobsActive:        int32(request.MaxJobsActive),
		ActiveJobs:           int32(request.ActiveJobs),
		Concurrency:          int32(request.Concurrency),
		TimeoutMillis:        request.TimeoutMillis,
		RequestTimeoutMillis: request.RequestTimeoutMillis,
		PollIntervalMillis:   request.PollIntervalMillis,
		FetchVariables:       request.FetchVariables,
		ClientVersion:        request.ClientVersion,
		RenewalTimeoutMillis: request.RenewalTimeoutMillis,
	}
}

func (client *ClientImpl) NewCreateJobCommand() commands.CreateJobCommandStep1 {
	return commands.NewCreateJobCommand(client.gateway)
}

func (client *ClientImpl) NewCancelJobCommand() commands.CancelJobCommandStep1 {
	return commands.NewCancelJobCommand(client.gateway)
}

func (client *ClientImpl) NewCreateJobSubscriptionCommand() commands.CreateJobSubscriptionCommandStep1 {
	return commands.NewCreateJobSubscriptionCommand(client.gateway)
}

func (client *ClientImpl) NewDeleteJobSubscriptionCommand() commands.DeleteJobSubscriptionCommandStep1 {
	return commands.NewDeleteJobSubscriptionCommand(client.gateway)
}

func (client *ClientImpl) NewCreateTimerSubscriptionCommand() commands.CreateTimerSubscriptionCommandStep1 {
	return commands.NewCreateTimerSubscriptionCommand(client.gateway)
}

func (client *ClientImpl) NewDeleteTimerSubscriptionCommand() commands.DeleteTimerSubscriptionCommandStep1 {
	return commands.NewDeleteTimerSubscriptionCommand(client.gateway)
}

func (client *ClientImpl) NewUpdateJobRetriesCommand() commands.UpdateJobRetriesCommandStep1 {
	return commands.NewUpdateJobRetriesCommand(client.gateway)
}

func (client *ClientImpl) NewUpdateJobPriorityCommand() commands.UpdateJobPriorityCommandStep1 {
	return commands.NewUpdateJobPriorityCommand(client.gateway)
}

func (client *ClientImpl) NewUpdateJobTimeoutCommand() commands.UpdateJobTimeoutCommandStep1 {
	return commands.NewUpdateJobTimeoutCommand(client.gateway)
}
