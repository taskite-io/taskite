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

package worker

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/taskite-io/taskite/clients/go/pkg/entities"
	"github.com/taskite-io/taskite/clients/go/pkg/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRegistrationBackpressureKeepsRegisterUntilSuccessAndResetsOnSuccess(t *testing.T) {
	t.Parallel()
	bp := status.Error(codes.ResourceExhausted, "BACKPRESSURE: queue is full")
	registry := &backpressuredRegistry{
		calls:  make(chan registrationAttempt, 8),
		errors: []error{bp, bp, nil, bp, nil},
	}
	worker := heartbeatTestWorker(registry)
	defer worker.heartbeatStop()
	if err := worker.register(); err != nil {
		t.Fatal(err)
	}
	go worker.heartbeatLoop()
	var last registrationAttempt
	for i := 0; i < 5; i++ {
		select {
		case attempt := <-registry.calls:
			if attempt.register != (i < 3) {
				t.Fatalf("attempt %d: register=%v; rejected REGISTER must not become RENEW", i, attempt.register)
			}
			if i > 0 {
				elapsed := attempt.started.Sub(last.started)
				if i == 3 && elapsed < worker.registryConfig.Registration.RenewInterval {
					t.Fatalf("success did not restore the configured heartbeat: %s", elapsed)
				}
				if i == 4 && elapsed > 60*time.Millisecond {
					t.Fatalf("success did not reset backoff to the 10ms initial interval: %s", elapsed)
				}
			}
			last = attempt
		case <-time.After(time.Second):
			t.Fatal("worker did not retry registration and transition to renew")
		}
	}
	worker.heartbeatStop()
	select {
	case <-worker.heartbeatDone:
	case <-time.After(time.Second):
		t.Fatal("worker heartbeat did not stop")
	}
}

func TestRegistrationBackoffCapsConfiguredMaximumAndHeartbeat(t *testing.T) {
	t.Parallel()
	for _, intervals := range [][3]time.Duration{
		{10 * time.Millisecond, 30 * time.Millisecond, 100 * time.Millisecond},
		{10 * time.Millisecond, 200 * time.Millisecond, 100 * time.Millisecond},
		{50 * time.Millisecond, 200 * time.Millisecond, time.Millisecond},
	} {
		config := RegistryConfig{
			Registration: RegistrationConfig{
				RenewInterval: intervals[2],
				Backpressure: BackpressureConfig{
					MinDelay: intervals[0], MaxDelay: intervals[1],
				}.WithDefaults(),
			},
		}
		for _, random := range []float64{0, 0.5, math.Nextafter(1, 0)} {
			backoff := registrationBackoff(config)
			backoff.random = func() float64 { return random }
			var previous time.Duration
			for i := 0; i < 100; i++ {
				delay := backoff.Next(previous)
				if delay < time.Millisecond || delay > config.Registration.Backpressure.MaxDelay ||
					delay > config.Registration.RenewInterval {
					t.Fatalf("unbounded retry delay %s (config %+v)", delay, config)
				}
				if random == 0.5 && i == 0 && delay != min(intervals[0], intervals[2]) {
					t.Fatalf("initial delay ignored: %s", delay)
				}
				if random >= 0.5 && i > 20 && delay != min(intervals[1], intervals[2]) {
					t.Fatalf("backoff did not reach configured ceiling: %s", delay)
				}
				previous = delay
			}
		}
	}
}

func TestRegistrationBackoffBuilderDefaultsGrowthJitterAndReset(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		random float64
		delays []time.Duration
	}{
		{0, []time.Duration{45, 65, 94}},
		{0.5, []time.Duration{50, 80, 128}},
		{math.Nextafter(1, 0), []time.Duration{55, 97, 171}},
	} {
		builder := NewExponentialBackoffBuilder().Random(func() float64 { return test.random })
		backoff, err := builder.Build()
		if err != nil {
			t.Fatal(err)
		}
		if backoff.MaxDelay != 5*time.Second || backoff.Factor != 1.6 || backoff.JitterFactor != 0.1 {
			t.Fatalf("unexpected defaults: %+v", backoff)
		}
		builder.MinDelay(time.Second).MaxDelay(time.Second)
		var previous time.Duration
		for _, millis := range test.delays {
			delay := backoff.Next(previous)
			if delay != millis*time.Millisecond {
				t.Fatalf("random=%v previous=%s: got %s, want %s", test.random, previous, delay, millis*time.Millisecond)
			}
			previous = delay
		}
		if reset := backoff.Next(0); reset != test.delays[0]*time.Millisecond {
			t.Fatalf("reset delay: %s", reset)
		}
	}
	backoff, err := NewExponentialBackoffBuilder().
		MinDelay(6 * time.Millisecond).MaxDelay(20 * time.Millisecond).
		BackoffFactor(1.6).JitterFactor(0).Build()
	if err != nil {
		t.Fatal(err)
	}
	if delay := backoff.Next(6 * time.Millisecond); delay != 10*time.Millisecond {
		t.Fatalf("registration delay must round like Broker: %s", delay)
	}
	backoff, err = NewExponentialBackoffBuilder().
		MinDelay(time.Millisecond).MaxDelay(time.Second).
		BackoffFactor(math.MaxFloat64).JitterFactor(1).
		Random(func() float64 { return 0 }).Build()
	if err != nil {
		t.Fatal(err)
	}
	if delay := backoff.Next(time.Duration(1<<63 - 1)); delay != time.Millisecond {
		t.Fatalf("large factor or negative jitter overflowed: %s", delay)
	}
}

func TestRegistrationBackoffBuilderRejectsInvalidParameters(t *testing.T) {
	t.Parallel()
	for _, builder := range []*ExponentialBackoffBuilder{
		NewExponentialBackoffBuilder().MinDelay(0),
		NewExponentialBackoffBuilder().MinDelay(time.Microsecond),
		NewExponentialBackoffBuilder().MaxDelay(time.Millisecond),
		NewExponentialBackoffBuilder().BackoffFactor(0),
		NewExponentialBackoffBuilder().BackoffFactor(0.9),
		NewExponentialBackoffBuilder().BackoffFactor(math.NaN()),
		NewExponentialBackoffBuilder().BackoffFactor(math.Inf(1)),
		NewExponentialBackoffBuilder().JitterFactor(-0.1),
		NewExponentialBackoffBuilder().JitterFactor(1.1),
		NewExponentialBackoffBuilder().JitterFactor(math.NaN()),
		NewExponentialBackoffBuilder().JitterFactor(math.Inf(1)),
		NewExponentialBackoffBuilder().Random(nil),
	} {
		if _, err := builder.Build(); err == nil {
			t.Fatalf("accepted invalid backoff: %+v", builder)
		}
	}
	if _, err := NewExponentialBackoffBuilder().BackoffFactor(1).JitterFactor(0).Build(); err != nil {
		t.Fatalf("valid boundary rejected: %v", err)
	}
}

func TestRegisteredWorkerBuilderResolvesAndValidatesBackpressure(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		config BackpressureConfig
		valid  bool
	}{
		{BackpressureConfig{}, true},
		{BackpressureConfig{MaxDelay: time.Second}, true},
		{BackpressureConfig{MinDelay: time.Microsecond}, false},
		{BackpressureConfig{MaxDelay: time.Millisecond}, false},
		{BackpressureConfig{BackoffFactor: math.NaN()}, false},
	} {
		builder := NewRegisteredJobWorkerBuilder(&workerGateway{}, &backpressuredRegistry{},
			fakeJobClient{}, RegistryConfig{
				ClusterID: "cluster-a", TenantID: "<default>",
				Registration: RegistrationConfig{RenewInterval: time.Second, Backpressure: test.config},
			}).(*JobWorkerBuilder)
		builder.JobType("payment")
		builder.Handler(func(JobClient, entities.Job) {})
		if err := builder.validate(); (err == nil) != test.valid {
			t.Fatalf("unexpected validation result for %+v: %v", test.config, err)
		}
		if test.valid && builder.registryConfig.Registration.Backpressure.JitterFactor == nil {
			t.Fatal("registry builder did not supply the default jitter")
		}
	}
}

func TestTechnicalRenewalFailureRetriesWithoutRegistering(t *testing.T) {
	t.Parallel()
	registry := &backpressuredRegistry{
		calls:  make(chan registrationAttempt, 2),
		errors: []error{status.Error(codes.ResourceExhausted, "unrelated quota")},
	}
	worker := heartbeatTestWorker(registry)
	defer worker.heartbeatStop()
	worker.wait.Add(1)
	go worker.heartbeatLoop()
	first := <-registry.calls
	select {
	case next := <-registry.calls:
		if first.register || next.register {
			t.Fatal("renewal failure attempted to create another registration")
		}
		if next.started.Sub(first.started) < worker.registryConfig.Registration.Backpressure.MinDelay {
			t.Fatal("technical failure retried without backoff")
		}
	case <-time.After(time.Second):
		t.Fatal("normal registration retry did not run")
	}
}

func TestHeartbeatCancellationInterruptsBackpressureWait(t *testing.T) {
	t.Parallel()
	registry := &backpressuredRegistry{
		calls:  make(chan registrationAttempt, 2),
		errors: []error{status.Error(codes.ResourceExhausted, "BACKPRESSURE: queue is full")},
	}
	worker := heartbeatTestWorker(registry)
	worker.registryConfig.Registration.RenewInterval = time.Hour
	worker.registryConfig.Registration.Backpressure.MinDelay = time.Second
	worker.registryConfig.Registration.Backpressure.MaxDelay = time.Second
	result := make(chan error, 1)
	go func() { result <- worker.register() }()
	select {
	case <-registry.calls:
	case <-time.After(time.Second):
		t.Fatal("registration did not start")
	}
	worker.cancel()
	select {
	case <-result:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("cancellation did not interrupt the retry wait")
	}
}

func heartbeatTestWorker(registry RegistryClient) *jobWorker {
	ctx, cancel := context.WithCancel(context.Background())
	worker := &jobWorker{
		registry: registry,
		registryConfig: RegistryConfig{
			ClusterID: "cluster-a", TenantID: "<default>",
			Registration: RegistrationConfig{
				RenewInterval: 100 * time.Millisecond,
				Backpressure: BackpressureConfig{
					MinDelay: 10 * time.Millisecond, MaxDelay: 80 * time.Millisecond,
					Random: func() float64 { return 0.5 },
				}.WithDefaults(),
			}.WithDefaults(),
		},
		context: ctx, cancel: cancel,
		request:  &pb.ActivateJobsRequest{Type: "payment"},
		workerID: "worker-1", heartbeatCtx: ctx, heartbeatStop: cancel,
		heartbeatDone: make(chan struct{}),
	}
	worker.token.Store(1)
	worker.deadline.Store(time.Now().Add(time.Hour).UnixMilli())
	return worker
}

type registrationAttempt struct {
	register bool
	started  time.Time
}

type backpressuredRegistry struct {
	calls  chan registrationAttempt
	errors []error // owned by the heartbeat goroutine
}

func (registry *backpressuredRegistry) attempt(register bool) error {
	registry.calls <- registrationAttempt{register, time.Now()}
	if len(registry.errors) == 0 {
		return nil
	}
	err := registry.errors[0]
	registry.errors = registry.errors[1:]
	return err
}

func (registry *backpressuredRegistry) Register(context.Context, RegistrationRequest) (RegistrationResult, error) {
	return testRegistrationResult(), registry.attempt(true)
}

func (registry *backpressuredRegistry) Renew(context.Context, RegistrationRequest) (RegistrationResult, error) {
	return testRegistrationResult(), registry.attempt(false)
}

func (*backpressuredRegistry) Deregister(context.Context, DeregisterRequest) error { return nil }
