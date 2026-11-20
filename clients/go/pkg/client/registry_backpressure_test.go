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
	"sync/atomic"
	"testing"
	"time"

	pdpb "github.com/taskite-io/taskite/clients/go/pkg/pb/placementdriver"
	"github.com/taskite-io/taskite/clients/go/pkg/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestRegistryPropagatesBackpressureWithoutInternalRetry(t *testing.T) {
	t.Parallel()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	service := &backpressurePlacementDriver{}
	pdpb.RegisterPlacementDriverServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///backpressure-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	registry := newPlacementDriverWorkerRegistry(conn)
	request := worker.RegistrationRequest{ClusterID: "cluster-a", WorkerID: "worker-1"}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, send := range []func(context.Context, worker.RegistrationRequest) (worker.RegistrationResult, error){registry.Register, registry.Renew} {
		if _, err := send(ctx, request); status.Code(err) != codes.ResourceExhausted ||
			status.Convert(err).Message() != "BACKPRESSURE: queue is full" {
			t.Fatalf("expected original backpressure: %v", err)
		}
	}
	if calls := service.calls.Load(); calls != 2 {
		t.Fatalf("registry retried internally: %d calls", calls)
	}
}

type backpressurePlacementDriver struct {
	pdpb.UnimplementedPlacementDriverServer
	calls atomic.Int32
}

func (service *backpressurePlacementDriver) RegisterWorker(context.Context, *pdpb.RegisterWorkerRequest) (*pdpb.RegisterWorkerResponse, error) {
	service.calls.Add(1)
	return nil, status.Error(codes.ResourceExhausted, "BACKPRESSURE: queue is full")
}

func (service *backpressurePlacementDriver) RenewWorker(context.Context, *pdpb.RenewWorkerRequest) (*pdpb.RenewWorkerResponse, error) {
	service.calls.Add(1)
	return nil, status.Error(codes.ResourceExhausted, "BACKPRESSURE: queue is full")
}
