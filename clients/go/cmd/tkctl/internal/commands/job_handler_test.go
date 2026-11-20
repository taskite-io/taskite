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

package commands

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/cobra"
	tempoclient "github.com/taskite-io/taskite/clients/go/pkg/client"
	clientcommands "github.com/taskite-io/taskite/clients/go/pkg/commands"
	"github.com/taskite-io/taskite/clients/go/pkg/entities"
	"github.com/taskite-io/taskite/clients/go/pkg/pb"
	pdpb "github.com/taskite-io/taskite/clients/go/pkg/pb/placementdriver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestShutdownReturnPreservesRetriesBackoffAndIdentity(t *testing.T) {
	t.Parallel()
	gateway := &handlerGateway{}
	job := entities.Job{ActivatedJob: &pb.ActivatedJob{
		Key: 99, Retries: 3, TenantId: "tenant-a", WorkerId: "worker-a",
		WorkerLeaseToken: 9007199254741099,
	}}
	if err := returnQueuedJob(context.Background(), handlerJobClient{gateway}, job, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	request := gateway.fail
	if request == nil || request.GetRetries() != 3 || request.GetRetryBackOff() != 2000 ||
		request.GetWorkerId() != job.GetWorkerId() || request.GetWorkerLeaseToken() != job.GetWorkerLeaseToken() {
		t.Fatalf("shutdown return changed retry budget or identity: %+v", request)
	}
	if tenants := gateway.failMetadata.Get("taskite-tenant-id"); len(tenants) != 1 || tenants[0] != job.GetTenantId() {
		t.Fatalf("shutdown return lost tenant metadata: %v", gateway.failMetadata)
	}
}

func TestCatHandlerCompletesJobWithVariables(t *testing.T) {
	t.Parallel()

	gateway := &handlerGateway{}
	client := handlerJobClient{gateway: gateway}
	arguments, err := parseHandler("cat")
	if err != nil {
		t.Fatal(err)
	}
	err = handleActivatedJob(
		context.Background(),
		client,
		arguments,
		entities.Job{ActivatedJob: &pb.ActivatedJob{
			Key:       42,
			Retries:   3,
			Variables: `{"orderId":7}`,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if gateway.complete == nil ||
		gateway.complete.GetJobKey() != 42 ||
		gateway.complete.GetVariables() != `{"orderId":7}` {
		t.Fatalf("unexpected complete request: %+v", gateway.complete)
	}
	if gateway.fail != nil {
		t.Fatalf("cat handler unexpectedly failed job: %+v", gateway.fail)
	}
}

func TestInvalidHandlerOutputFailsJob(t *testing.T) {
	t.Parallel()

	gateway := &handlerGateway{}
	client := handlerJobClient{gateway: gateway}
	arguments, err := parseHandler("printf invalid")
	if err != nil {
		t.Fatal(err)
	}
	err = handleActivatedJob(
		context.Background(),
		client,
		arguments,
		entities.Job{ActivatedJob: &pb.ActivatedJob{
			Key:       43,
			Retries:   3,
			Variables: `{}`,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if gateway.fail == nil ||
		gateway.fail.GetJobKey() != 43 ||
		gateway.fail.GetRetries() != 2 ||
		!strings.Contains(gateway.fail.GetErrorMessage(), "invalid variables") {
		t.Fatalf("unexpected fail request: %+v", gateway.fail)
	}
	if gateway.complete != nil {
		t.Fatalf("invalid output unexpectedly completed job: %+v", gateway.complete)
	}
}

func TestHandlerModeConsumesAcrossActivationRequests(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	gateway := &continuousHandlerGateway{
		completed: make(chan int64, 2),
	}
	pb.RegisterGatewayServer(server, gateway)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	placementListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	placementServer := grpc.NewServer()
	registry := &handlerWorkerRegistry{deregistered: make(chan string, 1)}
	pdpb.RegisterPlacementDriverServer(placementServer, registry)
	go func() {
		_ = placementServer.Serve(placementListener)
	}()
	t.Cleanup(func() {
		placementServer.Stop()
		_ = placementListener.Close()
	})
	t.Setenv(
		tempoclient.PlacementDriverAddressEnvironmentVariable,
		placementListener.Addr().String(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	command := &cobra.Command{}
	var output bytes.Buffer
	command.SetContext(ctx)
	command.SetOut(&output)
	command.SetErr(io.Discard)
	app := &application{
		address:        listener.Addr().String(),
		clusterID:      "demo",
		requestTimeout: time.Second,
		output:         "json",
	}

	result := make(chan error, 1)
	go func() {
		result <- app.runJobHandler(
			command,
			"payment",
			"cat",
			"taskite-test",
			time.Minute,
			clientcommands.DisableLongPolling,
			1,
			1,
			time.Millisecond,
			nil,
			nil,
			time.Second,
			time.Second,
		)
	}()

	for expected := int64(1); expected <= 2; expected++ {
		select {
		case key := <-gateway.completed:
			if key != expected {
				t.Fatalf("expected completed job %d, got %d", expected, key)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("job %d was not completed", expected)
		}
	}
	cancel()

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("continuous handler: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("continuous handler did not stop after cancellation")
	}
	if calls := gateway.activations.Load(); calls < 2 {
		t.Fatalf("expected at least two activation requests, got %d", calls)
	}
	select {
	case workerID := <-registry.deregistered:
		if workerID == "" {
			t.Fatal("worker deregistration did not include a worker ID")
		}
	case <-time.After(time.Second):
		t.Fatal("handler shutdown did not deregister the worker")
	}
	for _, expected := range []string{
		`worker started for job type "payment"`,
		"activated job 1",
		`{"activation":true}`,
		"handled job 2",
	} {
		if !strings.Contains(output.String(), expected) {
			t.Fatalf("worker output does not contain %q:\n%s", expected, output.String())
		}
	}
}

type handlerJobClient struct {
	gateway *handlerGateway
}

func (client handlerJobClient) NewCompleteJobCommand() clientcommands.CompleteJobCommandStep1 {
	return clientcommands.NewCompleteJobCommand(client.gateway)
}

func (client handlerJobClient) NewFailJobCommand() clientcommands.FailJobCommandStep1 {
	return clientcommands.NewFailJobCommand(client.gateway)
}

type handlerGateway struct {
	pb.GatewayClient
	complete     *pb.CompleteJobRequest
	fail         *pb.FailJobRequest
	failMetadata metadata.MD
}

func (gateway *handlerGateway) CompleteJob(
	_ context.Context,
	request *pb.CompleteJobRequest,
	_ ...grpc.CallOption,
) (*pb.CompleteJobResponse, error) {
	gateway.complete = request
	return &pb.CompleteJobResponse{JobKey: request.GetJobKey()}, nil
}

func (gateway *handlerGateway) FailJob(
	ctx context.Context,
	request *pb.FailJobRequest,
	_ ...grpc.CallOption,
) (*pb.FailJobResponse, error) {
	gateway.fail = request
	gateway.failMetadata, _ = metadata.FromOutgoingContext(ctx)
	return &pb.FailJobResponse{}, nil
}

type continuousHandlerGateway struct {
	pb.UnimplementedGatewayServer
	activations atomic.Int32
	completed   chan int64
}

func (gateway *continuousHandlerGateway) ActivateJobs(
	_ *pb.ActivateJobsRequest,
	stream grpc.ServerStreamingServer[pb.ActivateJobsResponse],
) error {
	activation := gateway.activations.Add(1)
	if activation > 2 {
		return nil
	}
	return stream.Send(&pb.ActivateJobsResponse{
		Jobs: []*pb.ActivatedJob{{
			Key:       int64(activation),
			Retries:   3,
			Variables: `{"activation":true}`,
		}},
	})
}

func (gateway *continuousHandlerGateway) CompleteJob(
	_ context.Context,
	request *pb.CompleteJobRequest,
) (*pb.CompleteJobResponse, error) {
	gateway.completed <- request.GetJobKey()
	return &pb.CompleteJobResponse{JobKey: request.GetJobKey()}, nil
}

type handlerWorkerRegistry struct {
	pdpb.UnimplementedPlacementDriverServer
	deregistered chan string
}

func (*handlerWorkerRegistry) RegisterWorker(
	_ context.Context,
	request *pdpb.RegisterWorkerRequest,
) (*pdpb.RegisterWorkerResponse, error) {
	return &pdpb.RegisterWorkerResponse{
		WorkerLeaseToken:         1,
		LeaseDeadlineEpochMillis: time.Now().Add(time.Hour).UnixMilli(),
		ClusterId:                request.GetClusterId(),
		TenantIds:                request.GetTenantIds(),
		WorkerId:                 request.GetWorkerId(),
		Name:                     request.GetName(),
	}, nil
}

func (registry *handlerWorkerRegistry) DeregisterWorker(
	_ context.Context,
	request *pdpb.DeregisterWorkerRequest,
) (*pdpb.DeregisterWorkerResponse, error) {
	registry.deregistered <- request.GetWorkerId()
	return &pdpb.DeregisterWorkerResponse{
		ClusterId: request.GetClusterId(),
		TenantIds: request.GetTenantIds(),
		WorkerId:  request.GetWorkerId(),
		Name:      request.GetName(),
	}, nil
}
