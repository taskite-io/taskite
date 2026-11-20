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
	"reflect"
	"testing"

	taskite "github.com/taskite-io/taskite/clients/go/pkg/client"
	"github.com/taskite-io/taskite/clients/go/pkg/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestTaskiteTenantEnvironmentAndExplicitActivationOverGRPC(t *testing.T) {
	t.Setenv(taskite.TenantEnvironmentVariable, "tenant-env")
	observed := make(chan metadata.MD, 1)
	deny := func(ctx context.Context) error {
		incoming, _ := metadata.FromIncomingContext(ctx)
		observed <- incoming
		return status.Error(codes.PermissionDenied, "fixture tenant denial")
	}
	server := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, _ any, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
			return nil, deny(ctx)
		}),
		grpc.StreamInterceptor(func(_ any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, _ grpc.StreamHandler) error {
			return deny(stream.Context())
		}),
	)
	pb.RegisterGatewayServer(server, &pb.UnimplementedGatewayServer{})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	for _, test := range []struct {
		args     []string
		expected string
	}{
		{[]string{"create", "job", "payment"}, "tenant-env"},
		{[]string{"create", "subscription", "payment"}, "tenant-env"},
		{[]string{"create", "timer", "payment", "audit", "--name", "daily", "--type", "cycle", "--expression", "R3/PT1M"}, "tenant-env"},
		{[]string{"delete", "timer", "9007199254740993"}, "tenant-env"},
		{[]string{"query", "job", "42"}, "tenant-env"},
		{[]string{"delete", "subscription", "42"}, "tenant-env"},
		{[]string{"cancel", "job", "42"}, "tenant-env"},
		{[]string{"resolve", "incident", "42"}, "tenant-env"},
		{[]string{"set", "variables", "42"}, "tenant-env"},
		{[]string{"activate", "jobs", "payment", "--long-poll-timeout=-1ms"}, "tenant-env"},
		{[]string{"complete", "job", "42"}, "tenant-env"},
		{[]string{"fail", "job", "42"}, "tenant-env"},
		{[]string{"update", "retries", "42", "3"}, "tenant-env"},
		{[]string{"update", "priority", "42", "50"}, "tenant-env"},
		{[]string{"update", "timeout", "42", "1m"}, "tenant-env"},
		{[]string{"activate", "jobs", "payment", "--long-poll-timeout=-1ms", "--tenant", "tenant-explicit,tenant-other"}, "tenant-explicit"},
		{[]string{"complete", "job", "42", "--tenant", "tenant-explicit"}, "tenant-explicit"},
		{[]string{"fail", "job", "42", "--tenant", "tenant-explicit"}, "tenant-explicit"},
	} {
		t.Run(test.args[0]+" "+test.args[1]+" "+test.expected, func(t *testing.T) {
			root := NewRootCommand()
			root.SetOut(io.Discard)
			root.SetErr(io.Discard)
			root.SetArgs(append([]string{
				"--address", listener.Addr().String(), "--cluster", "demo", "--request-timeout", "3s",
			}, test.args...))
			if err := root.Execute(); status.Code(err) != codes.PermissionDenied {
				t.Fatalf("Gateway rejection was not propagated: %v", err)
			}
			select {
			case incoming := <-observed:
				if !reflect.DeepEqual(incoming.Get(taskite.TenantIDHeader), []string{test.expected}) {
					t.Fatalf("tenant metadata=%v, want %s", incoming, test.expected)
				}
			default:
				t.Fatal("CLI did not reach the Gateway")
			}
		})
	}
}
