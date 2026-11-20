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
	"context"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	taskite "github.com/taskite-io/taskite/clients/go/pkg/client"
	"github.com/taskite-io/taskite/clients/go/pkg/pb"
	pdpb "github.com/taskite-io/taskite/clients/go/pkg/pb/placementdriver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestTaskiteQueriesIPv6GatewayFromFlagAndEnvironment(t *testing.T) {
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Fatal(err)
	}
	gateway := &ipv6QueryGateway{queries: make(chan ipv6Query, 1)}
	server := grpc.NewServer()
	pb.RegisterGatewayServer(server, gateway)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })

	for _, scenario := range []struct {
		name string
		flag bool
		bare bool
	}{
		{"bracketed flag", true, false},
		{"bare flag", true, true},
		{"bracketed environment", false, false},
		{"bare environment", false, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			address := listener.Addr().String()
			if scenario.bare {
				address = "0:0:0:0:0:0:0:1:" + strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
			}
			args := []string{"--cluster", "ipv6-demo", "--request-timeout", "3s", "query", "job", "42"}
			if scenario.flag {
				t.Setenv(taskite.AddressEnvironmentVariable, "invalid-environment:port")
				args = append(args, "--address", address)
			} else {
				t.Setenv(taskite.AddressEnvironmentVariable, address)
			}
			root := NewRootCommand()
			root.SetArgs(args)
			root.SetOut(io.Discard)
			root.SetErr(io.Discard)
			if err := root.Execute(); err != nil {
				t.Fatal(err)
			}
			select {
			case query := <-gateway.queries:
				if query.key != 42 || len(query.clusters) != 1 || query.clusters[0] != "ipv6-demo" {
					t.Fatalf("unexpected CLI request over IPv6: %+v", query)
				}
			default:
				t.Fatal("CLI did not query the IPv6 Gateway")
			}
		})
	}
}

func TestTaskiteHandlerUsesIPv6GatewayAndPlacementDriver(t *testing.T) {
	gatewayListener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Fatal(err)
	}
	placementListener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		_ = gatewayListener.Close()
		t.Fatal(err)
	}
	gateway := &continuousHandlerGateway{completed: make(chan int64, 2)}
	registry := &handlerWorkerRegistry{deregistered: make(chan string, 1)}
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
	t.Setenv(taskite.PlacementDriverAddressEnvironmentVariable,
		"0:0:0:0:0:0:0:1:"+strconv.Itoa(placementListener.Addr().(*net.TCPAddr).Port))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := NewRootCommand()
	root.SetContext(ctx)
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	root.SetArgs([]string{
		"--address", gatewayListener.Addr().String(), "--cluster", "demo", "--request-timeout", "3s",
		"activate", "jobs", "payment", "--handler", "cat", "--max-jobs", "1",
		"--concurrency", "1", "--poll-interval", "1ms", "--long-poll-timeout", "-1ms",
	})
	result := make(chan error, 1)
	go func() { result <- root.Execute() }()
	for expected := int64(1); expected <= 2; expected++ {
		select {
		case key := <-gateway.completed:
			if key != expected {
				t.Fatalf("unexpected completed job over IPv6: %d", key)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("IPv6 handler did not complete job %d", expected)
		}
	}
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("IPv6 handler did not stop")
	}
	select {
	case id := <-registry.deregistered:
		if id == "" {
			t.Fatal("IPv6 handler deregistered without a worker ID")
		}
	default:
		t.Fatal("IPv6 handler did not deregister with PD")
	}
}

type ipv6Query struct {
	key      int64
	clusters []string
}

type ipv6QueryGateway struct {
	pb.UnimplementedGatewayServer
	queries chan ipv6Query
}

func (gateway *ipv6QueryGateway) QueryJob(
	ctx context.Context, request *pb.QueryJobRequest,
) (*pb.ActivatedJob, error) {
	incoming, _ := metadata.FromIncomingContext(ctx)
	gateway.queries <- ipv6Query{key: request.GetJobKey(), clusters: incoming.Get(taskite.ClusterIDHeader)}
	return &pb.ActivatedJob{Key: request.GetJobKey()}, nil
}
