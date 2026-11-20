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
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/taskite-io/taskite/clients/go/pkg/commands"
	"github.com/taskite-io/taskite/clients/go/pkg/entities"
	"github.com/taskite-io/taskite/clients/go/pkg/pb"
	pdpb "github.com/taskite-io/taskite/clients/go/pkg/pb/placementdriver"
	"github.com/taskite-io/taskite/clients/go/pkg/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientTenantDefaultsAndOverridesOverGRPC(t *testing.T) {
	for _, test := range []struct {
		name        string
		environment string
		config      string
		request     string
		expected    string
	}{
		{"default", "", "", "", DefaultTenantID},
		{"environment", "tenant-env", "", "", "tenant-env"},
		{"config", "tenant-env", "tenant-config", "", "tenant-config"},
		{"request", "tenant-env", "tenant-config", "tenant-request", "tenant-request"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(TenantEnvironmentVariable, test.environment)
			gateway := &tenantIntegrationGateway{requests: make(chan tenantRequest, 1)}
			config := tenantTransport(t, gateway)
			config.TenantID = test.config
			client, err := NewClient(config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(
				"authorization", "Bearer token", ClusterIDHeader, "stale",
			))
			if test.request != "" {
				ctx = commands.WithTenantID(ctx, test.request)
			}
			original, _ := metadata.FromOutgoingContext(ctx)
			original = original.Copy()
			operations := []struct {
				name string
				send func() error
			}{
				{"subscription", func() error {
					_, err := client.NewCreateJobSubscriptionCommand().JobType("charge").Name("payment").Send(ctx)
					return err
				}},
				{"create", func() error {
					_, err := client.NewCreateJobCommand().JobType("payment").Send(ctx)
					return err
				}},
				{"query", func() error {
					_, err := client.NewQueryJobCommand().JobKey(42).Send(ctx)
					return err
				}},
				{"activate", func() error {
					_, err := client.NewActivateJobsCommand().JobType("payment").Send(ctx)
					return err
				}},
				{"complete", func() error {
					_, err := client.NewCompleteJobCommand().JobKey(42).Send(ctx)
					return err
				}},
				{"fail", func() error {
					_, err := client.NewFailJobCommand().JobKey(42).Send(ctx)
					return err
				}},
			}
			for _, operation := range operations {
				t.Run(operation.name, func(t *testing.T) {
					if err := operation.send(); err != nil {
						t.Fatal(err)
					}
					request := <-gateway.requests
					assertTenantRequest(t, request, test.expected)
					if !reflect.DeepEqual(request.metadata.Get("authorization"), []string{"Bearer token"}) {
						t.Fatalf("authorization metadata lost: %v", request.metadata)
					}
					after, _ := metadata.FromOutgoingContext(ctx)
					if !reflect.DeepEqual(after, original) {
						t.Fatalf("caller metadata changed: before=%v after=%v", original, after)
					}
				})
			}
			for _, operation := range []struct {
				name string
				send func() error
			}{
				{"explicit complete", func() error {
					_, err := client.NewCompleteJobCommand().JobKey(42).TenantID("tenant-job").Send(ctx)
					return err
				}},
				{"job response", func() error {
					_, err := client.NewFailJobCommand().Job(entities.Job{
						ActivatedJob: &pb.ActivatedJob{Key: 42, TenantId: "tenant-job"},
					}).Send(ctx)
					return err
				}},
				{"explicit activation", func() error {
					_, err := client.NewActivateJobsCommand().JobType("payment").
						TenantIDs("tenant-job", "tenant-other").Send(ctx)
					return err
				}},
			} {
				t.Run(operation.name, func(t *testing.T) {
					if err := operation.send(); err != nil {
						t.Fatal(err)
					}
					request := <-gateway.requests
					assertTenantRequest(t, request, "tenant-job")
					if operation.name == "explicit activation" &&
						!reflect.DeepEqual(request.tenants, []string{"tenant-job", "tenant-other"}) {
						t.Fatalf("activation scope lost: %v", request.tenants)
					}
				})
			}
		})
	}
}

func TestWorkerTenantSelectionOverGRPC(t *testing.T) {
	t.Setenv(TenantEnvironmentVariable, "tenant-env")
	for _, tenants := range [][]string{nil, {"tenant-explicit", "tenant-other"}} {
		expected := tenants
		if len(expected) == 0 {
			expected = []string{"tenant-env"}
		}
		t.Run(expected[0], func(t *testing.T) {
			gateway := &tenantIntegrationGateway{requests: make(chan tenantRequest, 1)}
			registry := &ipv6WorkerRegistry{
				registered: make(chan *pdpb.RegisterWorkerRequest, 1),
				removed:    make(chan *pdpb.DeregisterWorkerRequest, 1),
				renewed:    make(chan *pdpb.RenewWorkerRequest, 1),
			}
			config := tenantTransport(t, gateway, registry)
			client, err := NewClient(config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			opened, err := client.NewJobWorker().JobType("payment").
				Handler(func(worker.JobClient, entities.Job) {}).
				TenantIDs(tenants...).Context(ctx).Open()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				shutdown, stop := context.WithTimeout(context.Background(), time.Second)
				defer stop()
				_ = opened.Close(shutdown)
			})
			select {
			case request := <-registry.registered:
				if !reflect.DeepEqual(request.GetTenantIds(), expected) {
					t.Fatalf("registration tenants=%v, want %v", request.GetTenantIds(), expected)
				}
			case <-ctx.Done():
				t.Fatal("worker did not register")
			}
			select {
			case request := <-gateway.requests:
				assertTenantRequest(t, request, expected[0])
				if !reflect.DeepEqual(request.tenants, expected) {
					t.Fatalf("activation tenants=%v, want %v", request.tenants, expected)
				}
			case <-ctx.Done():
				t.Fatal("worker did not activate")
			}
			if err := opened.Close(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case request := <-registry.removed:
				if !reflect.DeepEqual(request.GetTenantIds(), expected) {
					t.Fatalf("deregistration tenants=%v, want %v", request.GetTenantIds(), expected)
				}
			case <-ctx.Done():
				t.Fatal("worker did not deregister")
			}
		})
	}
}

func tenantTransport(t *testing.T, gateway pb.GatewayServer, registries ...pdpb.PlacementDriverServer) ClientConfig {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	pb.RegisterGatewayServer(server, gateway)
	for _, registry := range registries {
		pdpb.RegisterPlacementDriverServer(server, registry)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	return ClientConfig{
		Gateway:         GatewayConfig{Address: "passthrough:///tenant-test"},
		PlacementDriver: PlacementDriverConfig{Address: "passthrough:///tenant-test"},
		ClusterID:       "cluster-a",
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return listener.DialContext(ctx)
			}),
		},
	}
}

type tenantRequest struct {
	metadata metadata.MD
	tenants  []string
}

func assertTenantRequest(t *testing.T, request tenantRequest, expected string) {
	t.Helper()
	if !reflect.DeepEqual(request.metadata.Get(TenantIDHeader), []string{expected}) ||
		!reflect.DeepEqual(request.metadata.Get(ClusterIDHeader), []string{"cluster-a"}) {
		t.Fatalf("unexpected request metadata: %v", request.metadata)
	}
}

type tenantIntegrationGateway struct {
	pb.UnimplementedGatewayServer
	requests chan tenantRequest
}

func (gateway *tenantIntegrationGateway) record(ctx context.Context, tenants []string) {
	incoming, _ := metadata.FromIncomingContext(ctx)
	select {
	case gateway.requests <- tenantRequest{metadata: incoming, tenants: tenants}:
	default:
	}
}

func (gateway *tenantIntegrationGateway) CreateJobSubscription(ctx context.Context, _ *pb.CreateJobSubscriptionRequest) (*pb.CreateJobSubscriptionResponse, error) {
	gateway.record(ctx, nil)
	return &pb.CreateJobSubscriptionResponse{}, nil
}

func (gateway *tenantIntegrationGateway) CreateJob(ctx context.Context, _ *pb.CreateJobRequest) (*pb.CreateJobResponse, error) {
	gateway.record(ctx, nil)
	return &pb.CreateJobResponse{}, nil
}

func (gateway *tenantIntegrationGateway) QueryJob(ctx context.Context, _ *pb.QueryJobRequest) (*pb.ActivatedJob, error) {
	gateway.record(ctx, nil)
	return &pb.ActivatedJob{}, nil
}

func (gateway *tenantIntegrationGateway) CompleteJob(ctx context.Context, _ *pb.CompleteJobRequest) (*pb.CompleteJobResponse, error) {
	gateway.record(ctx, nil)
	return &pb.CompleteJobResponse{}, nil
}

func (gateway *tenantIntegrationGateway) FailJob(ctx context.Context, _ *pb.FailJobRequest) (*pb.FailJobResponse, error) {
	gateway.record(ctx, nil)
	return &pb.FailJobResponse{}, nil
}

func (gateway *tenantIntegrationGateway) ActivateJobs(request *pb.ActivateJobsRequest, stream grpc.ServerStreamingServer[pb.ActivateJobsResponse]) error {
	gateway.record(stream.Context(), request.GetTenantIds())
	return stream.Send(&pb.ActivateJobsResponse{})
}
