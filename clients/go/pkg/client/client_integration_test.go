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

	"github.com/taskite-io/taskite/clients/go/pkg/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientUsesGeneratedGatewayOverGRPC(t *testing.T) {
	t.Parallel()

	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	gateway := &integrationGateway{
		metadata: make(chan metadata.MD, 2),
	}
	pb.RegisterGatewayServer(server, gateway)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	client, err := NewClient(ClientConfig{
		Gateway:   GatewayConfig{Address: "passthrough:///taskite-test"},
		ClusterID: "cluster-a",
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return listener.DialContext(ctx)
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer token")

	job, err := client.NewQueryJobCommand().JobKey(42).Send(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if job.GetKey() != 42 {
		t.Fatalf("unexpected queried job: %+v", job)
	}
	assertMetadata(t, <-gateway.metadata)

	jobs, err := client.
		NewActivateJobsCommand().
		JobType("payment").
		MaxJobsToActivate(1).
		Send(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].GetKey() != 43 {
		t.Fatalf("unexpected activated jobs: %+v", jobs)
	}
	assertMetadata(t, <-gateway.metadata)
}

func assertMetadata(t *testing.T, incoming metadata.MD) {
	t.Helper()
	if values := incoming.Get(ClusterIDHeader); !reflect.DeepEqual(values, []string{"cluster-a"}) {
		t.Fatalf("unexpected cluster metadata: %v", values)
	}
	if values := incoming.Get("authorization"); !reflect.DeepEqual(
		values,
		[]string{"Bearer token"},
	) {
		t.Fatalf("unrelated metadata was not preserved: %v", values)
	}
}

type integrationGateway struct {
	pb.UnimplementedGatewayServer
	metadata chan metadata.MD
}

func (gateway *integrationGateway) QueryJob(
	ctx context.Context,
	request *pb.QueryJobRequest,
) (*pb.ActivatedJob, error) {
	incoming, _ := metadata.FromIncomingContext(ctx)
	gateway.metadata <- incoming
	return &pb.ActivatedJob{Key: request.GetJobKey()}, nil
}

func (gateway *integrationGateway) ActivateJobs(
	_ *pb.ActivateJobsRequest,
	stream grpc.ServerStreamingServer[pb.ActivateJobsResponse],
) error {
	incoming, _ := metadata.FromIncomingContext(stream.Context())
	gateway.metadata <- incoming
	return stream.Send(&pb.ActivateJobsResponse{
		Jobs: []*pb.ActivatedJob{{Key: 43}},
	})
}
