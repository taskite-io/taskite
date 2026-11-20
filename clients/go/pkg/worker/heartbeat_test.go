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
	"errors"
	"testing"
	"time"
)

func TestHeartbeatRPCsUseConfiguredTimeoutAndReportIt(t *testing.T) {
	t.Parallel()
	for _, method := range []string{"register", "renew", "deregister"} {
		t.Run(method, func(t *testing.T) {
			const timeout = 50 * time.Millisecond
			var deadline time.Time
			registry := &deadlineRegistry{call: func(ctx context.Context) error {
				deadline, _ = ctx.Deadline()
				<-ctx.Done()
				return ctx.Err()
			}}
			worker := heartbeatTestWorker(registry)
			defer worker.heartbeatStop()
			worker.registryConfig.Registration.RenewInterval = 5 * time.Millisecond
			worker.registryConfig.Registration.RenewalTimeout = timeout
			start := time.Now()
			if method == "deregister" {
				worker.deregister(context.Background())
				if !errors.Is(worker.LastError(), context.DeadlineExceeded) {
					t.Fatalf("deregister did not apply the configured timeout: %v", worker.LastError())
				}
			} else {
				err := worker.sendRegistration(context.Background(), method == "register")
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("RPC did not expire: %v", err)
				}
				if registry.reportedTimeout != timeout.Milliseconds() {
					t.Fatalf("timeout was not reported: %d", registry.reportedTimeout)
				}
			}
			if budget := deadline.Sub(start); budget < timeout || budget > timeout+time.Second {
				t.Fatalf("RPC timeout was capped by the 5ms renewal interval: %s", budget)
			}
		})
	}
}

func TestHeartbeatRPCsRespectEarlierParentDeadline(t *testing.T) {
	t.Parallel()
	for _, method := range []string{"register", "renew", "deregister"} {
		t.Run(method, func(t *testing.T) {
			parent, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			expected, _ := parent.Deadline()
			registry := &deadlineRegistry{call: func(ctx context.Context) error {
				actual, _ := ctx.Deadline()
				if actual != expected {
					t.Fatalf("parent deadline changed: got %s, want %s", actual, expected)
				}
				<-ctx.Done()
				return ctx.Err()
			}}
			worker := heartbeatTestWorker(registry)
			defer worker.heartbeatStop()
			worker.registryConfig.Registration.RenewalTimeout = time.Second
			if method == "deregister" {
				worker.deregister(parent)
			} else if err := worker.sendRegistration(parent, method == "register"); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("parent deadline did not interrupt RPC: %v", err)
			}
		})
	}
}

func TestHeartbeatTimeoutKeepsRequestsSerialAndCancellationStopsRPC(t *testing.T) {
	t.Parallel()
	const timeout = 50 * time.Millisecond
	attempts := make(chan time.Time, 4)
	registry := &deadlineRegistry{call: func(ctx context.Context) error {
		attempts <- time.Now()
		<-ctx.Done()
		return ctx.Err()
	}}
	worker := heartbeatTestWorker(registry)
	defer worker.heartbeatStop()
	worker.registryConfig.Registration.RenewInterval = 5 * time.Millisecond
	worker.registryConfig.Registration.RenewalTimeout = timeout
	worker.wait.Add(1)
	go worker.heartbeatLoop()
	var previous time.Time
	for i := 0; i < 3; i++ {
		select {
		case started := <-attempts:
			if i > 0 && started.Sub(previous) < timeout {
				t.Fatal("renewal interval started another request before the in-flight RPC finished")
			}
			previous = started
		case <-time.After(time.Second):
			t.Fatal("worker did not retry after RPC timeout")
		}
	}
	worker.heartbeatStop()
	select {
	case <-worker.heartbeatDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("cancellation did not interrupt the in-flight RPC")
	}
}

type deadlineRegistry struct {
	call            func(context.Context) error
	reportedTimeout int64
}

func (registry *deadlineRegistry) Register(ctx context.Context, request RegistrationRequest) (RegistrationResult, error) {
	registry.reportedTimeout = request.RenewalTimeoutMillis
	return RegistrationResult{}, registry.call(ctx)
}

func (registry *deadlineRegistry) Renew(ctx context.Context, request RegistrationRequest) (RegistrationResult, error) {
	registry.reportedTimeout = request.RenewalTimeoutMillis
	return RegistrationResult{}, registry.call(ctx)
}

func (registry *deadlineRegistry) Deregister(ctx context.Context, _ DeregisterRequest) error {
	return registry.call(ctx)
}
