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

//go:build unix

package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	tempoclient "github.com/taskite-io/taskite/clients/go/pkg/client"
	"github.com/taskite-io/taskite/clients/go/pkg/pb"
	pdpb "github.com/taskite-io/taskite/clients/go/pkg/pb/placementdriver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestTaskiteSignalHelper(t *testing.T) {
	raw := os.Getenv("TASKITE_TEST_SIGNAL_ARGS")
	if raw == "" {
		t.Skip("subprocess entry")
	}
	var args []string
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		t.Fatal(err)
	}
	command := NewRootCommand()
	command.SetArgs(args)
	err := command.Execute()
	wantFailure := os.Getenv("TASKITE_TEST_PD_UNAVAILABLE") == "true"
	if wantFailure {
		if err == nil || !strings.Contains(err.Error(), "PD unavailable") {
			t.Fatalf("deregistration failure hidden: %v", err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
}

func TestTaskiteSignalReturnsQueueAndEndsSession(t *testing.T) {
	for _, mode := range []string{"graceful", "timeout", "PD-unavailable", "queue-failure", "SIGTERM"} {
		t.Run(mode, func(t *testing.T) {
			failures := make(chan *pb.FailJobRequest, 2)
			completed := make(chan *pb.CompleteJobRequest, 1)
			deregistered := make(chan *pdpb.DeregisterWorkerRequest, 1)
			gateway := &shutdownGateway{failures: failures, completed: completed, failReturn: mode == "queue-failure"}
			registry := &shutdownRegistry{deregistered: deregistered, unavailable: mode == "PD-unavailable"}
			gatewayAddress := serveShutdownGateway(t, gateway)
			pdAddress := serveShutdownRegistry(t, registry)

			directory := t.TempDir()
			startFile, releaseFile := filepath.Join(directory, "started"), filepath.Join(directory, "release")
			handler := filepath.Join(directory, "handler.sh")
			script := "#!/bin/sh\necho $$ > \"$TASKITE_HANDLER_STARTED\"\n" +
				"while [ ! -f \"$TASKITE_HANDLER_RELEASE\" ]; do sleep 0.02; done\nprintf '{}'\n"
			if err := os.WriteFile(handler, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			shutdownTimeout := "2s"
			if mode != "graceful" && mode != "SIGTERM" {
				shutdownTimeout = "150ms"
			}
			args, _ := json.Marshal([]string{
				"--address", gatewayAddress, "--cluster", "demo", "--request-timeout", "10s",
				"activate", "jobs", "work", "--handler", "/bin/sh " + handler,
				"--max-jobs", "3", "--concurrency", "1", "--tenant", "tenant-a",
				"--long-poll-timeout", "-1ms", "--shutdown-timeout", shutdownTimeout, "--retry-backoff", "2s",
			})
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			child := exec.Command(executable, "-test.run=^TestTaskiteSignalHelper$", "-test.timeout=10s")
			child.Env = append(os.Environ(),
				"TASKITE_TEST_SIGNAL_ARGS="+string(args),
				tempoclient.PlacementDriverAddressEnvironmentVariable+"="+pdAddress,
				tempoclient.WorkerRegistrationRenewIntervalEnvironmentVariable+"=50ms",
				tempoclient.WorkerRegistrationRenewalTimeoutEnvironmentVariable+"=500ms",
				"TASKITE_HANDLER_STARTED="+startFile, "TASKITE_HANDLER_RELEASE="+releaseFile,
				"TASKITE_TEST_PD_UNAVAILABLE="+strconv.FormatBool(mode == "PD-unavailable"),
			)
			var output bytes.Buffer
			child.Stdout, child.Stderr = &output, &output
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			exited := make(chan error, 1)
			go func() { exited <- child.Wait() }()
			t.Cleanup(func() { _ = child.Process.Kill() })
			pid := awaitHandlerPID(t, startFile)
			t.Cleanup(func() { _ = syscall.Kill(-pid, syscall.SIGKILL) })
			signal := os.Interrupt
			if mode == "SIGTERM" {
				signal = syscall.SIGTERM
			}
			if err := child.Process.Signal(signal); err != nil {
				t.Fatal(err)
			}
			for expected := int64(2); expected <= 3; expected++ {
				select {
				case request := <-failures:
					if request.GetJobKey() != expected || request.GetRetries() != 3 ||
						request.GetRetryBackOff() != 2000 || request.GetWorkerLeaseToken() != 9007199254741099 ||
						request.GetWorkerId() == "" {
						t.Fatalf("invalid queued return: %+v", request)
					}
				case <-time.After(time.Second):
					t.Fatal("SIGINT did not return queued jobs while handler was running")
				}
			}
			if mode == "graceful" || mode == "SIGTERM" {
				select {
				case <-deregistered:
					t.Fatal("session ended before running handler could finish")
				default:
				}
				if err := os.WriteFile(releaseFile, nil, 0600); err != nil {
					t.Fatal(err)
				}
				select {
				case request := <-completed:
					if request.GetJobKey() != 1 || request.GetWorkerLeaseToken() != 9007199254741099 {
						t.Fatalf("running job completion lost identity: %+v", request)
					}
				case <-time.After(time.Second):
					t.Fatal("running handler did not finish in grace period")
				}
			}
			select {
			case request := <-deregistered:
				if request.GetWorkerId() == "" || request.GetWorkerLeaseToken() != 9007199254741099 ||
					request.GetClusterId() != "demo" || len(request.GetTenantIds()) != 1 || request.GetTenantIds()[0] != "tenant-a" {
					t.Fatalf("PD deregistration lost identity: %+v", request)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("unfinished jobs were not handed back to PD")
			}
			select {
			case err := <-exited:
				if err != nil {
					t.Fatalf("SIGINT subprocess: %v\n%s", err, output.String())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("taskite did not exit within shutdown bound")
			}
			if mode != "graceful" && mode != "SIGTERM" {
				if len(completed) != 0 || !strings.Contains(output.String(), "grace period expired") {
					t.Fatalf("unfinished handler completed or timeout was hidden:\n%s", output.String())
				}
			}
			if err := syscall.Kill(-pid, 0); err != syscall.ESRCH {
				t.Fatalf("handler process group survived taskite exit: %v", err)
			}
			if gateway.activations.Load() != 1 {
				t.Fatalf("worker kept polling with full queue: %d", gateway.activations.Load())
			}
			if gateway.invalidTenant.Load() {
				t.Fatal("queued FailJob lost tenant metadata")
			}
		})
	}
}

func awaitHandlerPID(t *testing.T, path string) int {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()
	for {
		if raw, err := os.ReadFile(path); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil && pid > 0 {
				return pid
			}
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			t.Fatal("handler subprocess did not start")
			return 0
		}
	}
}

func serveShutdownGateway(t *testing.T, gateway pb.GatewayServer) string {
	return serveShutdownRPC(t, func(server *grpc.Server) { pb.RegisterGatewayServer(server, gateway) })
}

func serveShutdownRegistry(t *testing.T, registry pdpb.PlacementDriverServer) string {
	return serveShutdownRPC(t, func(server *grpc.Server) { pdpb.RegisterPlacementDriverServer(server, registry) })
}

func serveShutdownRPC(t *testing.T, register func(*grpc.Server)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	return listener.Addr().String()
}

type shutdownGateway struct {
	pb.UnimplementedGatewayServer
	activations   atomic.Int32
	invalidTenant atomic.Bool
	failures      chan *pb.FailJobRequest
	completed     chan *pb.CompleteJobRequest
	failReturn    bool
}

func (gateway *shutdownGateway) ActivateJobs(request *pb.ActivateJobsRequest, stream grpc.ServerStreamingServer[pb.ActivateJobsResponse]) error {
	if gateway.activations.Add(1) > 1 {
		return nil
	}
	jobs := make([]*pb.ActivatedJob, 3)
	for i := range jobs {
		jobs[i] = &pb.ActivatedJob{Key: int64(i + 1), Retries: 3, Variables: "{}",
			WorkerId: request.GetWorkerId(), WorkerLeaseToken: request.GetWorkerLeaseToken(), TenantId: "tenant-a"}
	}
	return stream.Send(&pb.ActivateJobsResponse{Jobs: jobs})
}

func (gateway *shutdownGateway) FailJob(ctx context.Context, request *pb.FailJobRequest) (*pb.FailJobResponse, error) {
	gateway.failures <- request
	incoming, _ := metadata.FromIncomingContext(ctx)
	if tenants := incoming.Get("taskite-tenant-id"); len(tenants) != 1 || tenants[0] != "tenant-a" {
		gateway.invalidTenant.Store(true)
		return nil, status.Error(codes.InvalidArgument, "shutdown return lost tenant")
	}
	if gateway.failReturn {
		return nil, status.Error(codes.Unavailable, "return failed")
	}
	return &pb.FailJobResponse{}, nil
}

func (gateway *shutdownGateway) CompleteJob(_ context.Context, request *pb.CompleteJobRequest) (*pb.CompleteJobResponse, error) {
	gateway.completed <- request
	return &pb.CompleteJobResponse{}, nil
}

type shutdownRegistry struct {
	pdpb.UnimplementedPlacementDriverServer
	deregistered chan *pdpb.DeregisterWorkerRequest
	unavailable  bool
}

func (*shutdownRegistry) RegisterWorker(_ context.Context, _ *pdpb.RegisterWorkerRequest) (*pdpb.RegisterWorkerResponse, error) {
	return &pdpb.RegisterWorkerResponse{WorkerLeaseToken: 9007199254741099,
		LeaseDeadlineEpochMillis: time.Now().Add(time.Minute).UnixMilli()}, nil
}

func (*shutdownRegistry) RenewWorker(_ context.Context, request *pdpb.RenewWorkerRequest) (*pdpb.RenewWorkerResponse, error) {
	return &pdpb.RenewWorkerResponse{WorkerLeaseToken: request.GetWorkerLeaseToken(),
		LeaseDeadlineEpochMillis: time.Now().Add(time.Minute).UnixMilli()}, nil
}

func (registry *shutdownRegistry) DeregisterWorker(_ context.Context, request *pdpb.DeregisterWorkerRequest) (*pdpb.DeregisterWorkerResponse, error) {
	registry.deregistered <- request
	if registry.unavailable {
		return nil, status.Error(codes.Unavailable, "PD unavailable")
	}
	return &pdpb.DeregisterWorkerResponse{}, nil
}
