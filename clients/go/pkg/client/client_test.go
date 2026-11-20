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
	"math"
	"strings"
	"testing"
	"time"

	pdpb "github.com/taskite-io/taskite/clients/go/pkg/pb/placementdriver"
	"github.com/taskite-io/taskite/clients/go/pkg/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestResolveConfigUsesExplicitValuesBeforeEnvironment(t *testing.T) {
	t.Setenv(AddressEnvironmentVariable, "environment:26500")
	t.Setenv(PlacementDriverAddressEnvironmentVariable, "environment:29001")
	t.Setenv(ClusterEnvironmentVariable, "environment")
	t.Setenv(TenantEnvironmentVariable, "tenant-env")
	t.Setenv(KeepAliveEnvironmentVariable, "2m")
	t.Setenv(WorkerRegistrationRenewIntervalEnvironmentVariable, "30s")
	t.Setenv(WorkerRegistrationRenewalTimeoutEnvironmentVariable, "60s")
	t.Setenv(WorkerRegistrationBackpressureMinDelayEnvironmentVariable, "100ms")
	t.Setenv(WorkerRegistrationBackpressureMaxDelayEnvironmentVariable, "2s")
	t.Setenv(WorkerRegistrationBackpressureBackoffFactorEnvironmentVariable, "2")
	t.Setenv(WorkerRegistrationBackpressureJitterFactorEnvironmentVariable, "0.5")

	zeroJitter := 0.0
	resolved, err := resolveConfig(ClientConfig{
		Gateway:         GatewayConfig{Address: "explicit:26500"},
		PlacementDriver: PlacementDriverConfig{Address: "explicit:29001"},
		ClusterID:       "explicit",
		TenantID:        "tenant-explicit",
		Transport:       TransportConfig{KeepAlive: time.Minute},
		Worker: WorkerConfig{Registration: worker.RegistrationConfig{
			RenewInterval: 15 * time.Second, RenewalTimeout: 45 * time.Second,
			Backpressure: worker.BackpressureConfig{
				MinDelay: 25 * time.Millisecond, MaxDelay: 500 * time.Millisecond,
				BackoffFactor: 1.2, JitterFactor: &zeroJitter,
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Gateway.Address != "explicit:26500" ||
		resolved.PlacementDriver.Address != "explicit:29001" ||
		resolved.ClusterID != "explicit" ||
		resolved.TenantID != "tenant-explicit" ||
		resolved.Transport.KeepAlive != time.Minute ||
		resolved.Worker.Registration.RenewInterval != 15*time.Second ||
		resolved.Worker.Registration.RenewalTimeout != 45*time.Second ||
		resolved.Worker.Registration.Backpressure.MinDelay != 25*time.Millisecond ||
		resolved.Worker.Registration.Backpressure.MaxDelay != 500*time.Millisecond ||
		resolved.Worker.Registration.Backpressure.BackoffFactor != 1.2 ||
		*resolved.Worker.Registration.Backpressure.JitterFactor != 0 {
		t.Fatalf("environment replaced explicit values: %+v", resolved)
	}
}

func TestResolveConfigUsesEnvironmentForZeroValues(t *testing.T) {
	t.Setenv(AddressEnvironmentVariable, "gateway.internal:26500")
	t.Setenv(PlacementDriverAddressEnvironmentVariable, "pd.internal:29001")
	t.Setenv(ClusterEnvironmentVariable, "cluster-a")
	t.Setenv(TenantEnvironmentVariable, "tenant-a")
	t.Setenv(KeepAliveEnvironmentVariable, "20s")
	t.Setenv(WorkerRegistrationRenewIntervalEnvironmentVariable, "7s")
	t.Setenv(WorkerRegistrationRenewalTimeoutEnvironmentVariable, "40s")
	t.Setenv(WorkerRegistrationBackpressureMinDelayEnvironmentVariable, "30ms")
	t.Setenv(WorkerRegistrationBackpressureMaxDelayEnvironmentVariable, "700ms")
	t.Setenv(WorkerRegistrationBackpressureBackoffFactorEnvironmentVariable, "2.5")
	t.Setenv(WorkerRegistrationBackpressureJitterFactorEnvironmentVariable, "0")

	resolved, err := resolveConfig(ClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Gateway.Address != "gateway.internal:26500" ||
		resolved.PlacementDriver.Address != "pd.internal:29001" ||
		resolved.ClusterID != "cluster-a" ||
		resolved.TenantID != "tenant-a" ||
		resolved.Transport.KeepAlive != 20*time.Second ||
		resolved.Worker.Registration.RenewInterval != 7*time.Second ||
		resolved.Worker.Registration.RenewalTimeout != 40*time.Second ||
		resolved.Worker.Registration.Backpressure.MinDelay != 30*time.Millisecond ||
		resolved.Worker.Registration.Backpressure.MaxDelay != 700*time.Millisecond ||
		resolved.Worker.Registration.Backpressure.BackoffFactor != 2.5 ||
		*resolved.Worker.Registration.Backpressure.JitterFactor != 0 {
		t.Fatalf("unexpected resolved config: %+v", resolved)
	}
}

func TestResolveConfigBackpressureDefaultsAndInvalidParameters(t *testing.T) {
	for _, environment := range []string{
		WorkerRegistrationRenewIntervalEnvironmentVariable,
		WorkerRegistrationRenewalTimeoutEnvironmentVariable,
		WorkerRegistrationBackpressureMinDelayEnvironmentVariable,
		WorkerRegistrationBackpressureMaxDelayEnvironmentVariable,
		WorkerRegistrationBackpressureBackoffFactorEnvironmentVariable,
		WorkerRegistrationBackpressureJitterFactorEnvironmentVariable,
	} {
		t.Setenv(environment, "")
	}
	resolved, err := resolveConfig(ClientConfig{ClusterID: "cluster-a"})
	if err != nil {
		t.Fatal(err)
	}
	parameters := resolved.Worker.Registration.Backpressure
	if resolved.Worker.Registration.RenewInterval != 5*time.Second ||
		resolved.Worker.Registration.RenewalTimeout != 30*time.Second {
		t.Fatalf("unexpected registration defaults: %+v", resolved.Worker.Registration)
	}
	if parameters.MinDelay != 50*time.Millisecond || parameters.MaxDelay != 5*time.Second ||
		parameters.BackoffFactor != 1.6 || *parameters.JitterFactor != 0.1 {
		t.Fatalf("unexpected backpressure defaults: %+v", resolved)
	}
	for _, backpressure := range []worker.BackpressureConfig{
		{MinDelay: -time.Second},
		{MinDelay: time.Microsecond},
		{MinDelay: time.Second, MaxDelay: time.Millisecond},
		{MaxDelay: -time.Second},
		{BackoffFactor: 0.9},
		{BackoffFactor: math.NaN()},
		{BackoffFactor: math.Inf(1)},
	} {
		config := ClientConfig{ClusterID: "cluster-a", Worker: WorkerConfig{
			Registration: worker.RegistrationConfig{Backpressure: backpressure},
		}}
		if _, err := resolveConfig(config); err == nil {
			t.Fatalf("accepted invalid retry configuration: %+v", config)
		}
	}
	for _, jitter := range []float64{-0.1, 1.1, math.NaN(), math.Inf(1)} {
		if _, err := resolveConfig(ClientConfig{ClusterID: "cluster-a", Worker: WorkerConfig{
			Registration: worker.RegistrationConfig{Backpressure: worker.BackpressureConfig{JitterFactor: &jitter}},
		}}); err == nil {
			t.Fatalf("accepted invalid jitter: %v", jitter)
		}
	}
	for _, test := range []struct {
		environment string
		value       string
	}{
		{WorkerRegistrationRenewIntervalEnvironmentVariable, "invalid"},
		{WorkerRegistrationRenewIntervalEnvironmentVariable, "0s"},
		{WorkerRegistrationRenewIntervalEnvironmentVariable, "30s"},
		{WorkerRegistrationRenewalTimeoutEnvironmentVariable, "invalid"},
		{WorkerRegistrationRenewalTimeoutEnvironmentVariable, "0s"},
		{WorkerRegistrationRenewalTimeoutEnvironmentVariable, "1s"},
		{WorkerRegistrationBackpressureMinDelayEnvironmentVariable, "invalid"},
		{WorkerRegistrationBackpressureMinDelayEnvironmentVariable, "0s"},
		{WorkerRegistrationBackpressureMaxDelayEnvironmentVariable, "invalid"},
		{WorkerRegistrationBackpressureMaxDelayEnvironmentVariable, "0s"},
		{WorkerRegistrationBackpressureBackoffFactorEnvironmentVariable, "invalid"},
		{WorkerRegistrationBackpressureBackoffFactorEnvironmentVariable, "0"},
		{WorkerRegistrationBackpressureBackoffFactorEnvironmentVariable, "NaN"},
		{WorkerRegistrationBackpressureJitterFactorEnvironmentVariable, "invalid"},
		{WorkerRegistrationBackpressureJitterFactorEnvironmentVariable, "+Inf"},
	} {
		t.Run(test.environment+"/"+test.value, func(t *testing.T) {
			t.Setenv(test.environment, test.value)
			if _, err := resolveConfig(ClientConfig{ClusterID: "cluster-a"}); err == nil {
				t.Fatalf("accepted invalid environment %s=%s", test.environment, test.value)
			}
		})
	}
}

func TestResolveConfigPartialBackpressureAndCopiesOptionalValue(t *testing.T) {
	for _, environment := range []string{
		WorkerRegistrationBackpressureMinDelayEnvironmentVariable,
		WorkerRegistrationBackpressureMaxDelayEnvironmentVariable,
		WorkerRegistrationBackpressureBackoffFactorEnvironmentVariable,
		WorkerRegistrationBackpressureJitterFactorEnvironmentVariable,
	} {
		t.Setenv(environment, "")
	}
	jitter := 0.0
	resolved, err := resolveConfig(ClientConfig{ClusterID: "cluster-a", Worker: WorkerConfig{
		Registration: worker.RegistrationConfig{Backpressure: worker.BackpressureConfig{
			MaxDelay: time.Second, JitterFactor: &jitter,
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	jitter = 0.7
	parameters := resolved.Worker.Registration.Backpressure
	if parameters.MinDelay != 50*time.Millisecond || parameters.MaxDelay != time.Second ||
		parameters.BackoffFactor != 1.6 || *parameters.JitterFactor != 0 {
		t.Fatalf("partial config lost defaults or kept mutable pointer: %+v", parameters)
	}
}

func TestNewClientPassesNestedBackpressureToWorkerRegistry(t *testing.T) {
	t.Parallel()
	jitter := 0.0
	opened, err := NewClient(ClientConfig{
		ClusterID: "cluster-a", Gateway: GatewayConfig{Address: "localhost:26500"},
		PlacementDriver: PlacementDriverConfig{Address: "localhost:26501"}, TenantID: "<default>",
		Transport: TransportConfig{KeepAlive: time.Minute},
		Worker: WorkerConfig{Registration: worker.RegistrationConfig{
			RenewInterval: time.Second, RenewalTimeout: 40 * time.Second,
			Backpressure: worker.BackpressureConfig{
				MinDelay: 10 * time.Millisecond, MaxDelay: 100 * time.Millisecond,
				BackoffFactor: 2, JitterFactor: &jitter,
				Random: func() float64 { return 0.5 },
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	jitter = 0.8
	registration := opened.(*ClientImpl).registryConfig.Registration
	parameters := registration.Backpressure
	if parameters.MinDelay != 10*time.Millisecond || parameters.MaxDelay != 100*time.Millisecond ||
		parameters.BackoffFactor != 2 || *parameters.JitterFactor != 0 ||
		parameters.Random() != 0.5 || registration.RenewInterval != time.Second ||
		registration.RenewalTimeout != 40*time.Second {
		t.Fatalf("nested parameters were not passed to registry: %+v", parameters)
	}
}

func TestResolveConfigRejectsInvalidClusterID(t *testing.T) {
	t.Setenv(ClusterEnvironmentVariable, "")

	_, err := resolveConfig(ClientConfig{
		ClusterID: "cluster a",
	})
	if err == nil || !strings.Contains(err.Error(), "[A-Za-z0-9._-]+") {
		t.Fatalf("expected cluster validation error, got %v", err)
	}
}

func TestClusterUnaryInterceptorReplacesExistingHeader(t *testing.T) {
	t.Parallel()

	ctx := metadata.NewOutgoingContext(
		context.Background(),
		metadata.Pairs(
			ClusterIDHeader,
			"stale",
			ClusterIDHeader,
			"duplicate",
			"authorization",
			"Bearer token",
		),
	)
	err := requestUnaryInterceptor("cluster-a", DefaultTenantID)(
		ctx,
		"/gateway_protocol.Gateway/QueryJob",
		nil,
		nil,
		nil,
		func(
			ctx context.Context,
			_ string,
			_ any,
			_ any,
			_ *grpc.ClientConn,
			_ ...grpc.CallOption,
		) error {
			outgoing, _ := metadata.FromOutgoingContext(ctx)
			if values := outgoing.Get(ClusterIDHeader); len(values) != 1 ||
				values[0] != "cluster-a" {
				t.Fatalf("unexpected cluster metadata: %v", values)
			}
			if values := outgoing.Get("authorization"); len(values) != 1 {
				t.Fatalf("unrelated metadata was lost: %v", values)
			}
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterStreamInterceptorAddsHeader(t *testing.T) {
	t.Parallel()

	_, err := requestStreamInterceptor("cluster-b", DefaultTenantID)(
		context.Background(),
		&grpc.StreamDesc{},
		nil,
		"/gateway_protocol.Gateway/ActivateJobs",
		func(
			ctx context.Context,
			_ *grpc.StreamDesc,
			_ *grpc.ClientConn,
			_ string,
			_ ...grpc.CallOption,
		) (grpc.ClientStream, error) {
			outgoing, _ := metadata.FromOutgoingContext(ctx)
			if values := outgoing.Get(ClusterIDHeader); len(values) != 1 ||
				values[0] != "cluster-b" {
				t.Fatalf("unexpected cluster metadata: %v", values)
			}
			return nil, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPlacementDriverWorkerRegistryUsesGeneratedRegisterStub(t *testing.T) {
	t.Parallel()

	connection := &recordingPlacementDriverConnection{}
	registry := newPlacementDriverWorkerRegistry(connection)
	result, err := registry.Register(context.Background(), worker.RegistrationRequest{
		ClusterID:            "demo",
		TenantIDs:            []string{"tenant-a"},
		WorkerID:             "instance-1",
		Name:                 "payments",
		JobType:              "payment",
		MaxJobsActive:        32,
		ActiveJobs:           3,
		Concurrency:          4,
		TimeoutMillis:        30_000,
		RequestTimeoutMillis: 10_000,
		PollIntervalMillis:   100,
		FetchVariables:       []string{"orderId"},
		ClientVersion:        "0.1.0",
		RenewalTimeoutMillis: 30_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.WorkerLeaseToken != 42 || result.LeaseDeadlineEpochMillis != 123456 {
		t.Fatalf("registration result was lost: %+v", result)
	}
	if connection.method != pdpb.PlacementDriver_RegisterWorker_FullMethodName {
		t.Fatalf("unexpected method: %s", connection.method)
	}

	request, ok := connection.request.(*pdpb.RegisterWorkerRequest)
	if !ok {
		t.Fatalf("unexpected request type: %T", connection.request)
	}
	if request.GetClusterId() != "demo" ||
		request.GetTenantIds()[0] != "tenant-a" ||
		request.GetWorkerId() != "instance-1" ||
		request.GetJobType() != "payment" ||
		request.GetMaxJobsActive() != 32 ||
		request.GetActiveJobs() != 3 || request.GetRenewalTimeoutMillis() != 30_000 {
		t.Fatalf("unexpected worker registration: %+v", request)
	}
}

func TestPlacementDriverWorkerRegistryMapsGrpcRejection(t *testing.T) {
	t.Parallel()

	connection := &recordingPlacementDriverConnection{
		err: status.Error(codes.NotFound, "CLUSTER_NOT_REGISTERED: demo"),
	}
	registry := newPlacementDriverWorkerRegistry(connection)
	err := registry.Deregister(context.Background(), worker.DeregisterRequest{
		ClusterID: "demo",
		TenantIDs: []string{"tenant-a"},
		WorkerID:  "instance-1",
		Name:      "payments",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected not found rejection, got %v", err)
	}
}

func TestPlacementDriverWorkerRegistryUsesGeneratedRenewStub(t *testing.T) {
	t.Parallel()

	connection := &recordingPlacementDriverConnection{}
	registry := newPlacementDriverWorkerRegistry(connection)
	result, err := registry.Renew(context.Background(), worker.RegistrationRequest{
		WorkerLeaseToken: 42,
		ClusterID:        "demo",
		TenantIDs:        []string{"tenant-a"},
		WorkerID:         "instance-1",
		Name:             "payments",
		JobType:          "payment",
		MaxJobsActive:    32,
		ActiveJobs:       2,
		Concurrency:      4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.WorkerLeaseToken != 42 {
		t.Fatalf("renewal token was lost: %+v", result)
	}
	if connection.method != pdpb.PlacementDriver_RenewWorker_FullMethodName {
		t.Fatalf("unexpected method: %s", connection.method)
	}
	request, ok := connection.request.(*pdpb.RenewWorkerRequest)
	if !ok {
		t.Fatalf("unexpected request type: %T", connection.request)
	}
	if request.GetWorkerId() != "instance-1" || request.GetActiveJobs() != 2 || request.GetWorkerLeaseToken() != 42 {
		t.Fatalf("unexpected worker renewal: %+v", request)
	}
}

type recordingPlacementDriverConnection struct {
	method  string
	request any
	err     error
}

func (connection *recordingPlacementDriverConnection) Invoke(
	_ context.Context,
	method string,
	args any,
	reply any,
	_ ...grpc.CallOption,
) error {
	connection.method = method
	connection.request = args
	switch response := reply.(type) {
	case *pdpb.RegisterWorkerResponse:
		response.WorkerLeaseToken = 42
		response.LeaseDeadlineEpochMillis = 123456
	case *pdpb.RenewWorkerResponse:
		response.WorkerLeaseToken = 42
		response.LeaseDeadlineEpochMillis = 123456
	}
	return connection.err
}

func (*recordingPlacementDriverConnection) NewStream(
	context.Context,
	*grpc.StreamDesc,
	string,
	...grpc.CallOption,
) (grpc.ClientStream, error) {
	return nil, status.Error(codes.Unimplemented, "not used")
}
