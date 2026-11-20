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
	"strconv"
	"testing"
	"time"

	"github.com/taskite-io/taskite/clients/go/pkg/entities"
	"github.com/taskite-io/taskite/clients/go/pkg/pb"
	pdpb "github.com/taskite-io/taskite/clients/go/pkg/pb/placementdriver"
	"github.com/taskite-io/taskite/clients/go/pkg/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientGatewayOverIPv6TCP(t *testing.T) {
	t.Parallel()

	for _, format := range []string{"bracketed", "bare", "dns", "passthrough"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			listener, err := net.Listen("tcp6", "[::1]:0")
			if err != nil {
				t.Fatal(err)
			}
			gateway := &integrationGateway{metadata: make(chan metadata.MD, 2)}
			server := grpc.NewServer()
			pb.RegisterGatewayServer(server, gateway)
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(func() { server.Stop(); _ = listener.Close() })

			address := listener.Addr().String()
			switch format {
			case "bare":
				address = "0:0:0:0:0:0:0:1:" + strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
			case "dns", "passthrough":
				address = format + ":///" + address
			}
			client, err := NewClient(ClientConfig{Gateway: GatewayConfig{Address: address}, ClusterID: "cluster-a"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer token")
			job, err := client.NewQueryJobCommand().JobKey(42).Send(ctx)
			if err != nil || job.GetKey() != 42 {
				t.Fatalf("IPv6 query: job=%v err=%v", job, err)
			}
			assertMetadata(t, <-gateway.metadata)
			jobs, err := client.NewActivateJobsCommand().JobType("payment").MaxJobsToActivate(1).Send(ctx)
			if err != nil || len(jobs) != 1 || jobs[0].GetKey() != 43 {
				t.Fatalf("IPv6 stream: jobs=%v err=%v", jobs, err)
			}
			assertMetadata(t, <-gateway.metadata)
		})
	}
}

func TestScopedIPv6AddressReachesGRPCDialerWithOriginalZone(t *testing.T) {
	t.Parallel()

	for _, address := range []string{"[fe80::1%eth0]:26500", "fe80::1%eth0:26500", "passthrough:///[fe80::1%25eth0]:26500"} {
		t.Run(address, func(t *testing.T) {
			listener := bufconn.Listen(1 << 20)
			server := grpc.NewServer()
			pb.RegisterGatewayServer(server, &integrationGateway{metadata: make(chan metadata.MD, 1)})
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(func() { server.Stop(); _ = listener.Close() })
			targets := make(chan string, 1)
			client, err := NewClient(ClientConfig{
				Gateway:   GatewayConfig{Address: address},
				ClusterID: "demo",
				DialOptions: []grpc.DialOption{
					grpc.WithContextDialer(func(ctx context.Context, target string) (net.Conn, error) {
						select {
						case targets <- target:
						default:
						}
						return listener.DialContext(ctx)
					}),
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if _, err := client.NewQueryJobCommand().JobKey(42).Send(ctx); err != nil {
				t.Fatal(err)
			}
			if target := <-targets; target != "[fe80::1%eth0]:26500" {
				t.Fatalf("IPv6 zone changed at dialer: %q", target)
			}
		})
	}
}

func TestWorkerLifecycleOverIPv6TCP(t *testing.T) {
	t.Parallel()

	gatewayListener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Fatal(err)
	}
	placementListener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		_ = gatewayListener.Close()
		t.Fatal(err)
	}
	gateway := &ipv6WorkerGateway{activated: make(chan struct{}, 1)}
	registry := &ipv6WorkerRegistry{
		registered: make(chan *pdpb.RegisterWorkerRequest, 1),
		renewed:    make(chan *pdpb.RenewWorkerRequest, 1),
		removed:    make(chan *pdpb.DeregisterWorkerRequest, 1),
	}
	gatewayServer, placementServer := grpc.NewServer(), grpc.NewServer()
	pb.RegisterGatewayServer(gatewayServer, gateway)
	pdpb.RegisterPlacementDriverServer(placementServer, registry)
	go func() { _ = gatewayServer.Serve(gatewayListener) }()
	go func() { _ = placementServer.Serve(placementListener) }()
	t.Cleanup(func() {
		gatewayServer.Stop()
		placementServer.Stop()
		_ = gatewayListener.Close()
		_ = placementListener.Close()
	})
	client, err := NewClient(ClientConfig{
		Gateway:         GatewayConfig{Address: gatewayListener.Addr().String()},
		PlacementDriver: PlacementDriverConfig{Address: "0:0:0:0:0:0:0:1:" + strconv.Itoa(placementListener.Addr().(*net.TCPAddr).Port)},
		ClusterID:       "demo",
		TenantID:        "tenant-a",
		Worker:          WorkerConfig{Registration: worker.RegistrationConfig{RenewInterval: 100 * time.Millisecond}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	jobWorker, err := client.NewJobWorker().JobType("payment").
		Handler(func(worker.JobClient, entities.Job) {}).
		Name("ipv6-worker").PollInterval(10 * time.Millisecond).Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		shutdown, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		_ = jobWorker.Close(shutdown)
	})
	var registration *pdpb.RegisterWorkerRequest
	select {
	case registration = <-registry.registered:
		if registration.GetClusterId() != "demo" || registration.GetName() != "ipv6-worker" ||
			registration.GetWorkerId() == "" || len(registration.GetTenantIds()) != 1 ||
			registration.GetTenantIds()[0] != "tenant-a" {
			t.Fatalf("unexpected IPv6 registration: %v", registration)
		}
	case <-ctx.Done():
		t.Fatal("worker did not register over IPv6")
	}
	select {
	case renewal := <-registry.renewed:
		if renewal.GetWorkerId() != registration.GetWorkerId() || renewal.GetJobType() != "payment" {
			t.Fatalf("unexpected IPv6 renewal: %v", renewal)
		}
	case <-ctx.Done():
		t.Fatal("worker did not renew over IPv6")
	}
	select {
	case <-gateway.activated:
	case <-ctx.Done():
		t.Fatal("worker did not activate jobs over IPv6")
	}
	if err := jobWorker.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case removal := <-registry.removed:
		if removal.GetWorkerId() != registration.GetWorkerId() {
			t.Fatalf("unexpected IPv6 deregistration: %v", removal)
		}
	case <-ctx.Done():
		t.Fatal("worker did not deregister over IPv6")
	}
}

type ipv6WorkerGateway struct {
	pb.UnimplementedGatewayServer
	activated chan struct{}
}

func (gateway *ipv6WorkerGateway) ActivateJobs(
	_ *pb.ActivateJobsRequest,
	stream grpc.ServerStreamingServer[pb.ActivateJobsResponse],
) error {
	select {
	case gateway.activated <- struct{}{}:
	default:
	}
	return stream.Send(&pb.ActivateJobsResponse{})
}

type ipv6WorkerRegistry struct {
	pdpb.UnimplementedPlacementDriverServer
	registered chan *pdpb.RegisterWorkerRequest
	renewed    chan *pdpb.RenewWorkerRequest
	removed    chan *pdpb.DeregisterWorkerRequest
}

func (registry *ipv6WorkerRegistry) RegisterWorker(
	_ context.Context, request *pdpb.RegisterWorkerRequest,
) (*pdpb.RegisterWorkerResponse, error) {
	select {
	case registry.registered <- request:
	default:
	}
	return &pdpb.RegisterWorkerResponse{WorkerId: request.GetWorkerId(), WorkerLeaseToken: 1, LeaseDeadlineEpochMillis: time.Now().Add(time.Hour).UnixMilli()}, nil
}

func (registry *ipv6WorkerRegistry) RenewWorker(
	_ context.Context, request *pdpb.RenewWorkerRequest,
) (*pdpb.RenewWorkerResponse, error) {
	select {
	case registry.renewed <- request:
	default:
	}
	return &pdpb.RenewWorkerResponse{WorkerId: request.GetWorkerId(), WorkerLeaseToken: 1, LeaseDeadlineEpochMillis: time.Now().Add(time.Hour).UnixMilli()}, nil
}

func (registry *ipv6WorkerRegistry) DeregisterWorker(
	_ context.Context, request *pdpb.DeregisterWorkerRequest,
) (*pdpb.DeregisterWorkerResponse, error) {
	registry.removed <- request
	return &pdpb.DeregisterWorkerResponse{WorkerId: request.GetWorkerId()}, nil
}
