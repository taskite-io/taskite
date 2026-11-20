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
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/taskite-io/taskite/clients/go/pkg/commands"
	"github.com/taskite-io/taskite/clients/go/pkg/entities"
	"github.com/taskite-io/taskite/clients/go/pkg/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWorkerAppliesBackpressureAndDrainsJobsOnClose(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	requests := make(chan *pb.ActivateJobsRequest, 1)
	gateway := &workerGateway{
		activate: func(
			ctx context.Context,
			request *pb.ActivateJobsRequest,
		) (grpc.ServerStreamingClient[pb.ActivateJobsResponse], error) {
			calls.Add(1)
			requests <- request
			return &workerStream{
				context: ctx,
				responses: []*pb.ActivateJobsResponse{
					{Jobs: []*pb.ActivatedJob{{Key: 1}, {Key: 2}}},
				},
			}, nil
		},
	}

	started := make(chan int64, 2)
	release := make(chan struct{}, 2)
	openedWorker, err := NewJobWorkerBuilder(gateway, fakeJobClient{}).
		JobType("payment").
		Handler(func(_ JobClient, job entities.Job) {
			started <- job.GetKey()
			<-release
		}).
		MaxJobsActive(2).
		Concurrency(1).
		RequestTimeout(commands.DisableLongPolling).
		Open()
	if err != nil {
		t.Fatal(err)
	}

	select {
	case request := <-requests:
		if request.GetMaxJobsToActivate() != 2 {
			t.Fatalf("expected activation limit 2, got %d", request.GetMaxJobsToActivate())
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not activate jobs")
	}
	select {
	case key := <-started:
		if key != 1 {
			t.Fatalf("expected first job 1, got %d", key)
		}
	case <-time.After(time.Second):
		t.Fatal("first job was not dispatched")
	}

	time.Sleep(30 * time.Millisecond)
	if count := calls.Load(); count != 1 {
		t.Fatalf("worker polled while max jobs were active: %d calls", count)
	}

	closeResult := make(chan error, 1)
	closeCtx, cancelClose := context.WithTimeout(context.Background(), time.Second)
	defer cancelClose()
	go func() {
		closeResult <- openedWorker.Close(closeCtx)
	}()
	select {
	case <-openedWorker.(*jobWorker).context.Done():
	case <-time.After(time.Second):
		t.Fatal("worker close did not cancel polling")
	}

	release <- struct{}{}
	select {
	case key := <-started:
		if key != 2 {
			t.Fatalf("expected second job 2, got %d", key)
		}
	case <-time.After(time.Second):
		t.Fatal("queued job was not drained during close")
	}
	release <- struct{}{}

	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatalf("close worker: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not close")
	}
	if openedWorker.ActiveJobs() != 0 {
		t.Fatalf("expected no active jobs, got %d", openedWorker.ActiveJobs())
	}
}

func TestWorkerReportsHeartbeatsAndDeregistersOnClose(t *testing.T) {
	t.Parallel()

	registry := &workerRegistry{
		registrations: make(chan RegistrationRequest, 1),
		renewals:      make(chan RegistrationRequest, 4),
		deregisters:   make(chan DeregisterRequest, 1),
	}
	gateway := &workerGateway{
		activate: func(
			ctx context.Context,
			_ *pb.ActivateJobsRequest,
		) (grpc.ServerStreamingClient[pb.ActivateJobsResponse], error) {
			return &blockingWorkerStream{context: ctx}, nil
		},
	}

	openedWorker, err := NewRegisteredJobWorkerBuilder(
		gateway,
		registry,
		fakeJobClient{},
		RegistryConfig{
			ClusterID:     "cluster-a",
			TenantID:      "<default>",
			ClientVersion: "0.1.0",
			Registration: RegistrationConfig{
				RenewInterval: 20 * time.Millisecond, RenewalTimeout: 2 * time.Second,
			},
		},
	).
		JobType("payment").
		Handler(func(JobClient, entities.Job) {}).
		Name("payment-worker").
		MaxJobsActive(12).
		Concurrency(3).
		TenantIDs("tenant-a", "tenant-b").
		FetchVariables("orderId").
		Open()
	if err != nil {
		t.Fatal(err)
	}

	var heartbeat RegistrationRequest
	select {
	case heartbeat = <-registry.registrations:
	case <-time.After(time.Second):
		t.Fatal("worker did not report its initial heartbeat")
	}
	if heartbeat.ClusterID != "cluster-a" ||
		heartbeat.Name != "payment-worker" ||
		heartbeat.JobType != "payment" ||
		heartbeat.MaxJobsActive != 12 ||
		heartbeat.Concurrency != 3 ||
		heartbeat.ClientVersion != "0.1.0" ||
		len(heartbeat.TenantIDs) != 2 ||
		heartbeat.RenewalTimeoutMillis != 2_000 {
		t.Fatalf("unexpected worker heartbeat: %+v", heartbeat)
	}
	if heartbeat.WorkerID == "" {
		t.Fatal("worker heartbeat is missing the instance ID")
	}
	if openedWorker.WorkerLeaseToken() != 1 || openedWorker.State() != RUNNING {
		t.Fatal("registered worker did not enter its stable session")
	}

	select {
	case renewed := <-registry.renewals:
		if renewed.WorkerID != heartbeat.WorkerID {
			t.Fatalf("worker ID changed across heartbeats: %q != %q",
				renewed.WorkerID, heartbeat.WorkerID)
		}
		if renewed.WorkerLeaseToken != 1 {
			t.Fatal("renewal lost its original token")
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not renew its heartbeat")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := openedWorker.Close(closeCtx); err != nil {
		t.Fatalf("close worker: %v", err)
	}
	select {
	case deregister := <-registry.deregisters:
		if deregister.WorkerID != heartbeat.WorkerID ||
			deregister.Name != "payment-worker" ||
			len(deregister.TenantIDs) != 2 || deregister.WorkerLeaseToken != 1 {
			t.Fatalf("unexpected deregistration: %+v", deregister)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not deregister")
	}
}

func TestWorkerSessionLifecycle(t *testing.T) {
	for _, mode := range []string{"drain", "fenced", "close-timeout"} {
		t.Run(mode, func(t *testing.T) {
			started, release := make(chan struct{}, 2), make(chan struct{})
			renewed := make(chan struct{}, 8)
			deregistered := make(chan DeregisterRequest, 1)
			var handled atomic.Int32
			var calls atomic.Int32
			registry := &sessionRegistry{
				renew: func(_ context.Context, request RegistrationRequest) (RegistrationResult, error) {
					select {
					case renewed <- struct{}{}:
					default:
					}
					if mode == "fenced" {
						return RegistrationResult{}, status.Error(codes.FailedPrecondition, "STALE_WORKER_SESSION")
					}
					return testRegistrationResult(), nil
				},
				deregister: func(_ context.Context, request DeregisterRequest) error {
					deregistered <- request
					return nil
				},
			}
			gateway := &workerGateway{activate: func(ctx context.Context, request *pb.ActivateJobsRequest) (grpc.ServerStreamingClient[pb.ActivateJobsResponse], error) {
				if request.GetWorkerId() == "" || request.GetWorkerLeaseToken() != 1 ||
					len(request.GetTenantIds()) != 1 || request.GetTenantIds()[0] != "tenant-a" {
					return nil, status.Error(codes.InvalidArgument, "activation lost registered session")
				}
				if calls.Add(1) == 1 {
					return &workerStream{context: ctx, responses: []*pb.ActivateJobsResponse{{Jobs: []*pb.ActivatedJob{{Key: 1}, {Key: 2}}}}}, nil
				}
				return &blockingWorkerStream{context: ctx}, nil
			}}
			opened, err := NewRegisteredJobWorkerBuilder(gateway, registry, fakeJobClient{}, RegistryConfig{
				ClusterID: "demo", TenantID: "tenant-a",
				Registration: RegistrationConfig{RenewInterval: 20 * time.Millisecond},
			}).JobType("payment").Handler(func(JobClient, entities.Job) {
				handled.Add(1)
				started <- struct{}{}
				<-release
			}).MaxJobsActive(2).Concurrency(1).Open()
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("handler did not start")
			}
			if mode == "drain" {
				result := make(chan error, 1)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				go func() { result <- opened.Close(ctx) }()
				select {
				case <-opened.(*jobWorker).context.Done():
				case <-time.After(time.Second):
					t.Fatal("polling did not stop")
				}
				select {
				case <-renewed:
				case <-time.After(time.Second):
					t.Fatal("draining worker stopped renewing")
				}
				select {
				case <-deregistered:
					t.Fatal("worker deregistered before handlers drained")
				default:
				}
				close(release)
				select {
				case err := <-result:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("drain did not finish")
				}
				if handled.Load() != 2 || opened.State() != CLOSED {
					t.Fatal("queued tasks did not drain")
				}
			} else {
				if mode == "fenced" {
					select {
					case <-opened.(*jobWorker).context.Done():
					case <-time.After(time.Second):
						t.Fatal("stale session continued polling")
					}
				} else {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
					defer cancel()
					if err := opened.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("close timeout hidden: %v", err)
					}
				}
				if opened.State() != FENCED {
					t.Fatal("worker did not enter FENCED")
				}
				if mode == "close-timeout" {
					select {
					case request := <-deregistered:
						if request.WorkerLeaseToken != 1 {
							t.Fatal("timeout deregistration lost token")
						}
					case <-time.After(time.Second):
						t.Fatal("timed-out close did not end session")
					}
				}
				close(release)
				select {
				case <-opened.Done():
				case <-time.After(time.Second):
					t.Fatal("fenced worker did not finish")
				}
				if handled.Load() != 1 || opened.ActiveJobs() != 0 {
					t.Fatal("fenced worker dispatched its queued job")
				}
			}
		})
	}
}

func TestWorkerReturnsQueueWhileRunningHandlerDrains(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, release := make(chan int64, 17), make(chan struct{})
	returned := make(chan int64, 17)
	deregistered := make(chan DeregisterRequest, 1)
	jobs := make([]*pb.ActivatedJob, 17)
	for i := range jobs {
		jobs[i] = &pb.ActivatedJob{Key: int64(i + 1)}
	}
	gateway := &workerGateway{activate: func(ctx context.Context, _ *pb.ActivateJobsRequest) (grpc.ServerStreamingClient[pb.ActivateJobsResponse], error) {
		return &workerStream{context: ctx, responses: []*pb.ActivateJobsResponse{{Jobs: jobs}}}, nil
	}}
	registry := &sessionRegistry{deregister: func(_ context.Context, request DeregisterRequest) error {
		deregistered <- request
		return nil
	}}
	opened, err := NewRegisteredJobWorkerBuilder(gateway, registry, fakeJobClient{}, RegistryConfig{
		ClusterID: "demo", TenantID: "tenant-a",
	}).JobType("work").Handler(func(_ JobClient, job entities.Job) { started <- job.GetKey(); <-release }).
		Context(ctx).MaxJobsActive(17).Concurrency(1).
		ShutdownHandler(func(_ JobClient, job entities.Job) { returned <- job.GetKey() }).Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		close(release)
		<-opened.Done()
		if opened.ActiveJobs() != 0 {
			t.Errorf("drained worker retained %d jobs", opened.ActiveJobs())
		}
	})
	select {
	case key := <-started:
		if key != 1 {
			t.Fatalf("unexpected first job: %d", key)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	cancel()
	seen := map[int64]bool{1: true}
	for range 16 {
		select {
		case key := <-returned:
			if seen[key] {
				t.Fatalf("duplicate job returned: %d", key)
			}
			seen[key] = true
		case <-time.After(time.Second):
			t.Fatal("queued jobs waited for running handler")
		}
	}
	select {
	case <-deregistered:
		t.Fatal("deregistered before running handler finished")
	default:
	}
	if len(started) != 0 {
		t.Fatalf("queue was executed: started=%d", len(started))
	}
}

func TestWorkerReturnsEveryJobFromCanceledActivationResponse(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	returned := make(chan int64, 3)
	gateway := &workerGateway{activate: func(ctx context.Context, _ *pb.ActivateJobsRequest) (grpc.ServerStreamingClient[pb.ActivateJobsResponse], error) {
		cancel()
		return &workerStream{context: ctx, responses: []*pb.ActivateJobsResponse{{Jobs: []*pb.ActivatedJob{
			{Key: 1}, {Key: 2}, {Key: 3},
		}}}}, nil
	}}
	opened, err := NewJobWorkerBuilder(gateway, fakeJobClient{}).JobType("work").
		Handler(func(_ JobClient, job entities.Job) { t.Errorf("executed canceled job %d", job.GetKey()) }).
		Context(ctx).MaxJobsActive(3).Concurrency(1).
		ShutdownHandler(func(_ JobClient, job entities.Job) { returned <- job.GetKey() }).Open()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-opened.Done():
	case <-time.After(time.Second):
		t.Fatal("canceled activation did not finish")
	}
	if len(returned) != 3 || opened.ActiveJobs() != 0 {
		t.Fatalf("response jobs lost on cancellation: returned=%d active=%d", len(returned), opened.ActiveJobs())
	}
}

func TestWorkerCloseWaitsForDeregistrationAfterTimeout(t *testing.T) {
	for _, mode := range []string{"ack", "unavailable", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			deregisterStarted, acknowledge := make(chan DeregisterRequest, 1), make(chan struct{})
			var deregisterCalls atomic.Int32
			unavailable := status.Error(codes.Unavailable, "PD unavailable")
			registry := &sessionRegistry{deregister: func(ctx context.Context, request DeregisterRequest) error {
				deregisterCalls.Add(1)
				deregisterStarted <- request
				select {
				case <-acknowledge:
					if mode == "unavailable" {
						return unavailable
					}
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}}
			gateway := &workerGateway{activate: func(ctx context.Context, _ *pb.ActivateJobsRequest) (grpc.ServerStreamingClient[pb.ActivateJobsResponse], error) {
				return &workerStream{context: ctx, responses: []*pb.ActivateJobsResponse{{Jobs: []*pb.ActivatedJob{{Key: 1}}}}}, nil
			}}
			opened, err := NewRegisteredJobWorkerBuilder(gateway, registry, fakeJobClient{}, RegistryConfig{
				ClusterID: "demo", TenantID: "tenant-a",
				Registration: RegistrationConfig{RenewInterval: 10 * time.Millisecond, RenewalTimeout: 200 * time.Millisecond},
			}).JobType("work").Handler(func(JobClient, entities.Job) { close(started); <-release }).
				MaxJobsActive(1).Concurrency(1).Open()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { close(release); <-opened.Done() })
			<-started
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- opened.Close(ctx) }()
			select {
			case request := <-deregisterStarted:
				if request.WorkerID != opened.WorkerID() || request.WorkerLeaseToken != opened.WorkerLeaseToken() {
					t.Fatal("timeout deregistration lost session identity")
				}
			case <-time.After(time.Second):
				t.Fatal("deregistration did not start")
			}
			select {
			case err := <-result:
				t.Fatalf("Close returned before PD attempt completed: %v", err)
			default:
			}
			if mode != "deadline" {
				close(acknowledge)
			}
			select {
			case err := <-result:
				if !errors.Is(err, context.DeadlineExceeded) || (mode == "unavailable" && !errors.Is(err, unavailable)) {
					t.Fatalf("shutdown errors lost: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("deregistration exceeded RPC bound")
			}
			if deregisterCalls.Load() != 1 {
				t.Fatal("deregistration was not exactly once")
			}
		})
	}
}

func TestWorkerOpenValidatesRegistrationAndReusesWorkerID(t *testing.T) {
	for _, mode := range []string{"retry", "missing-token", "expired-deadline"} {
		t.Run(mode, func(t *testing.T) {
			var calls int
			var id string
			registry := &sessionRegistry{register: func(_ context.Context, request RegistrationRequest) (RegistrationResult, error) {
				calls++
				if request.WorkerID == "" || (id != "" && id != request.WorkerID) {
					t.Fatal("retry changed worker ID")
				}
				id = request.WorkerID
				if mode == "retry" && calls == 1 {
					return RegistrationResult{}, status.Error(codes.Unavailable, "response lost")
				}
				result := testRegistrationResult()
				if mode == "missing-token" {
					result.WorkerLeaseToken = 0
				}
				if mode == "expired-deadline" {
					result.LeaseDeadlineEpochMillis = 1
				}
				return result, nil
			}}
			gateway := &workerGateway{activate: func(ctx context.Context, _ *pb.ActivateJobsRequest) (grpc.ServerStreamingClient[pb.ActivateJobsResponse], error) {
				return &blockingWorkerStream{context: ctx}, nil
			}}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			opened, err := NewRegisteredJobWorkerBuilder(gateway, registry, fakeJobClient{}, RegistryConfig{
				ClusterID: "demo", TenantID: "tenant-a",
				Registration: RegistrationConfig{RenewInterval: 10 * time.Millisecond},
			}).JobType("payment").Handler(func(JobClient, entities.Job) {}).Context(ctx).Open()
			if mode != "retry" {
				if opened != nil || status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("invalid registration accepted: %v", err)
				}
				return
			}
			if err != nil || calls != 2 {
				t.Fatalf("register did not retry: %d %v", calls, err)
			}
			if opened.WorkerID() != id {
				t.Fatal("registered identity differs from running worker")
			}
			if err := opened.Close(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHandlerCommandsBindActivatedJob(t *testing.T) {
	gateway := &handlerGateway{}
	job := entities.Job{ActivatedJob: &pb.ActivatedJob{Key: 17, WorkerId: "instance-a", WorkerLeaseToken: 42, TenantId: "tenant-a"}}
	worker := &jobWorker{client: handlerJobClient{gateway}, handler: func(client JobClient, job entities.Job) {
		if _, err := client.NewCompleteJobCommand().JobKey(job.GetKey()).Send(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := client.NewFailJobCommand().JobKey(job.GetKey()).Retries(2).Send(context.Background()); err != nil {
			t.Fatal(err)
		}
	}}
	worker.handle(job)
	if gateway.complete.GetWorkerId() != "instance-a" || gateway.complete.GetWorkerLeaseToken() != 42 ||
		gateway.fail.GetWorkerId() != "instance-a" || gateway.fail.GetWorkerLeaseToken() != 42 {
		t.Fatal("handler JobKey commands lost their session binding")
	}
}

type handlerJobClient struct{ gateway pb.GatewayClient }

func (client handlerJobClient) NewCompleteJobCommand() commands.CompleteJobCommandStep1 {
	return commands.NewCompleteJobCommand(client.gateway)
}
func (client handlerJobClient) NewFailJobCommand() commands.FailJobCommandStep1 {
	return commands.NewFailJobCommand(client.gateway)
}

type handlerGateway struct {
	pb.GatewayClient
	complete *pb.CompleteJobRequest
	fail     *pb.FailJobRequest
}

func (gateway *handlerGateway) CompleteJob(_ context.Context, request *pb.CompleteJobRequest, _ ...grpc.CallOption) (*pb.CompleteJobResponse, error) {
	gateway.complete = request
	return &pb.CompleteJobResponse{}, nil
}
func (gateway *handlerGateway) FailJob(_ context.Context, request *pb.FailJobRequest, _ ...grpc.CallOption) (*pb.FailJobResponse, error) {
	gateway.fail = request
	return &pb.FailJobResponse{}, nil
}

type sessionRegistry struct {
	register   func(context.Context, RegistrationRequest) (RegistrationResult, error)
	renew      func(context.Context, RegistrationRequest) (RegistrationResult, error)
	deregister func(context.Context, DeregisterRequest) error
}

func (registry *sessionRegistry) Register(ctx context.Context, request RegistrationRequest) (RegistrationResult, error) {
	if registry.register != nil {
		return registry.register(ctx, request)
	}
	return testRegistrationResult(), nil
}
func (registry *sessionRegistry) Renew(ctx context.Context, request RegistrationRequest) (RegistrationResult, error) {
	if registry.renew != nil {
		return registry.renew(ctx, request)
	}
	return testRegistrationResult(), nil
}
func (registry *sessionRegistry) Deregister(ctx context.Context, request DeregisterRequest) error {
	if registry.deregister != nil {
		return registry.deregister(ctx, request)
	}
	return nil
}

func TestBlockingRegistrationPreventsActivationAndRespectsContext(t *testing.T) {
	t.Parallel()

	registry := &blockingWorkerRegistry{
		registerStarted: make(chan struct{}),
		registerStopped: make(chan struct{}),
	}
	activationStarted := make(chan struct{}, 1)
	gateway := &workerGateway{
		activate: func(
			ctx context.Context,
			_ *pb.ActivateJobsRequest,
		) (grpc.ServerStreamingClient[pb.ActivateJobsResponse], error) {
			select {
			case activationStarted <- struct{}{}:
			default:
			}
			return &blockingWorkerStream{context: ctx}, nil
		},
	}

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	result := make(chan error, 1)
	go func() {
		_, err := NewRegisteredJobWorkerBuilder(
			gateway,
			registry,
			fakeJobClient{},
			RegistryConfig{
				ClusterID:     "cluster-a",
				TenantID:      "<default>",
				ClientVersion: "0.1.0",
				Registration:  RegistrationConfig{RenewInterval: time.Second},
			},
		).
			JobType("payment").
			Handler(func(JobClient, entities.Job) {}).
			Context(ctx).
			Open()
		result <- err
	}()

	select {
	case <-registry.registerStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not start its heartbeat goroutine")
	}
	select {
	case <-activationStarted:
		t.Fatal("worker activated before registration completed")
	default:
	}

	stop()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Open did not return cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Open ignored cancellation")
	}
	select {
	case <-registry.registerStopped:
	default:
		t.Fatal("worker close did not stop the heartbeat goroutine")
	}
}

func TestLongPollDeadlineIsTreatedAsEmptyActivation(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	reported := make(chan error, 1)
	gateway := &workerGateway{
		activate: func(
			ctx context.Context,
			_ *pb.ActivateJobsRequest,
		) (grpc.ServerStreamingClient[pb.ActivateJobsResponse], error) {
			calls.Add(1)
			return &deadlineWorkerStream{context: ctx}, nil
		},
	}
	openedWorker, err := NewJobWorkerBuilder(gateway, fakeJobClient{}).
		JobType("payment").
		Handler(func(JobClient, entities.Job) {}).
		PollInterval(time.Millisecond).
		ErrorHandler(func(err error) {
			reported <- err
		}).
		Open()
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if calls.Load() < 2 {
		t.Fatal("worker did not continue polling after a long-poll deadline")
	}
	select {
	case err := <-reported:
		t.Fatalf("long-poll deadline was reported as an activation failure: %v", err)
	default:
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := openedWorker.Close(closeCtx); err != nil {
		t.Fatalf("close worker: %v", err)
	}
}

func TestWorkerIsolatesHandlerPanic(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	gateway := &workerGateway{
		activate: func(
			ctx context.Context,
			_ *pb.ActivateJobsRequest,
		) (grpc.ServerStreamingClient[pb.ActivateJobsResponse], error) {
			if calls.Add(1) == 1 {
				return &workerStream{
					context: ctx,
					responses: []*pb.ActivateJobsResponse{
						{Jobs: []*pb.ActivatedJob{{Key: 10}}},
					},
				}, nil
			}
			return &blockingWorkerStream{context: ctx}, nil
		},
	}

	reported := make(chan error, 1)
	jobWorker, err := NewJobWorkerBuilder(gateway, fakeJobClient{}).
		JobType("panic").
		Handler(func(_ JobClient, _ entities.Job) {
			panic("handler failed")
		}).
		ErrorHandler(func(err error) {
			reported <- err
		}).
		Open()
	if err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-reported:
		if !strings.Contains(err.Error(), "handler failed") {
			t.Fatalf("unexpected handler error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler panic was not reported")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := jobWorker.Close(closeCtx); err != nil {
		t.Fatalf("close worker: %v", err)
	}
	if err := jobWorker.LastError(); err == nil ||
		!strings.Contains(err.Error(), "handler failed") {
		t.Fatalf("unexpected last error: %v", err)
	}
}

func TestWorkerStopsAfterPermanentActivationError(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	gateway := &workerGateway{
		activate: func(
			context.Context,
			*pb.ActivateJobsRequest,
		) (grpc.ServerStreamingClient[pb.ActivateJobsResponse], error) {
			calls.Add(1)
			return nil, status.Error(codes.InvalidArgument, "invalid job type")
		},
	}
	reported := make(chan error, 1)
	openedWorker, err := NewJobWorkerBuilder(gateway, fakeJobClient{}).
		JobType("invalid").
		Handler(func(JobClient, entities.Job) {}).
		ErrorHandler(func(err error) {
			reported <- err
		}).
		Open()
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-openedWorker.Done():
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after permanent error")
	}
	if calls.Load() != 1 {
		t.Fatalf("permanent error was retried %d times", calls.Load())
	}
	select {
	case err := <-reported:
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("unexpected reported error: %v", err)
		}
	default:
		t.Fatal("permanent error was not reported")
	}
}

func TestRegisteredWorkerStopsHeartbeatAfterPermanentActivationError(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	gateway := &workerGateway{
		activate: func(
			context.Context,
			*pb.ActivateJobsRequest,
		) (grpc.ServerStreamingClient[pb.ActivateJobsResponse], error) {
			calls.Add(1)
			return nil, status.Error(codes.InvalidArgument, "invalid job type")
		},
	}
	registry := &workerRegistry{
		registrations: make(chan RegistrationRequest, 1),
		renewals:      make(chan RegistrationRequest, 2),
		deregisters:   make(chan DeregisterRequest, 1),
	}
	openedWorker, err := NewRegisteredJobWorkerBuilder(
		gateway,
		registry,
		fakeJobClient{},
		RegistryConfig{
			ClusterID:     "cluster-a",
			TenantID:      "<default>",
			ClientVersion: "0.1.0",
			Registration:  RegistrationConfig{RenewInterval: 20 * time.Millisecond},
		},
	).
		JobType("invalid").
		Handler(func(JobClient, entities.Job) {}).
		Open()
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-openedWorker.Done():
	case <-time.After(time.Second):
		t.Fatal("registered worker did not stop after permanent activation error")
	}
	if calls.Load() != 1 {
		t.Fatalf("permanent error was retried %d times", calls.Load())
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := openedWorker.Close(closeCtx); err != nil {
		t.Fatalf("close worker: %v", err)
	}
	select {
	case <-registry.deregisters:
	case <-time.After(time.Second):
		t.Fatal("stopped worker did not deregister on close")
	}
}

func TestExponentialBackoffIsBounded(t *testing.T) {
	t.Parallel()

	backoff := ExponentialBackoff{
		MinDelay: 10 * time.Millisecond,
		MaxDelay: 25 * time.Millisecond,
		Factor:   2,
	}
	if delay := backoff.Next(0); delay != 10*time.Millisecond {
		t.Fatalf("unexpected minimum delay: %s", delay)
	}
	if delay := backoff.Next(10 * time.Millisecond); delay != 20*time.Millisecond {
		t.Fatalf("unexpected exponential delay: %s", delay)
	}
	if delay := backoff.Next(20 * time.Millisecond); delay != 25*time.Millisecond {
		t.Fatalf("unexpected maximum delay: %s", delay)
	}

	largeMinimum := ExponentialBackoff{MinDelay: 10 * time.Second}
	if delay := largeMinimum.Next(10 * time.Second); delay != 10*time.Second {
		t.Fatalf("delay must not shrink below a custom minimum: %s", delay)
	}
}

func TestRetryableActivationErrors(t *testing.T) {
	t.Parallel()

	for _, code := range []codes.Code{
		codes.Aborted,
		codes.Canceled,
		codes.DeadlineExceeded,
		codes.ResourceExhausted,
		codes.Unavailable,
	} {
		if !isRetryableActivationError(status.Error(code, "transient")) {
			t.Fatalf("expected %s to be retryable", code)
		}
	}
	if isRetryableActivationError(status.Error(codes.PermissionDenied, "permanent")) {
		t.Fatal("permission denied must not be retried")
	}
}

type fakeJobClient struct{}

func (fakeJobClient) NewCompleteJobCommand() commands.CompleteJobCommandStep1 {
	return nil
}

func (fakeJobClient) NewFailJobCommand() commands.FailJobCommandStep1 {
	return nil
}

type workerRegistry struct {
	registrations chan RegistrationRequest
	renewals      chan RegistrationRequest
	deregisters   chan DeregisterRequest
}

type blockingWorkerRegistry struct {
	registerStarted chan struct{}
	registerStopped chan struct{}
}

func (registry *blockingWorkerRegistry) Register(
	ctx context.Context,
	_ RegistrationRequest,
) (RegistrationResult, error) {
	close(registry.registerStarted)
	<-ctx.Done()
	close(registry.registerStopped)
	return RegistrationResult{}, ctx.Err()
}

func (*blockingWorkerRegistry) Renew(context.Context, RegistrationRequest) (RegistrationResult, error) {
	return testRegistrationResult(), nil
}

func (*blockingWorkerRegistry) Deregister(context.Context, DeregisterRequest) error {
	return nil
}

func (registry *workerRegistry) Register(
	_ context.Context,
	request RegistrationRequest,
) (RegistrationResult, error) {
	registry.registrations <- request
	return testRegistrationResult(), nil
}

func (registry *workerRegistry) Renew(
	_ context.Context,
	request RegistrationRequest,
) (RegistrationResult, error) {
	select {
	case registry.renewals <- request:
	default:
	}
	return testRegistrationResult(), nil
}

func testRegistrationResult() RegistrationResult {
	return RegistrationResult{WorkerLeaseToken: 1, LeaseDeadlineEpochMillis: time.Now().Add(time.Hour).UnixMilli()}
}

func (registry *workerRegistry) Deregister(
	_ context.Context,
	request DeregisterRequest,
) error {
	registry.deregisters <- request
	return nil
}

type workerGateway struct {
	pb.GatewayClient
	activate func(
		context.Context,
		*pb.ActivateJobsRequest,
	) (grpc.ServerStreamingClient[pb.ActivateJobsResponse], error)
}

func (gateway *workerGateway) ActivateJobs(
	ctx context.Context,
	request *pb.ActivateJobsRequest,
	_ ...grpc.CallOption,
) (grpc.ServerStreamingClient[pb.ActivateJobsResponse], error) {
	if gateway.activate == nil {
		return nil, errors.New("activate function is not configured")
	}
	return gateway.activate(ctx, request)
}

type workerStream struct {
	grpc.ClientStream
	context   context.Context
	responses []*pb.ActivateJobsResponse
	index     int
}

func (stream *workerStream) Recv() (*pb.ActivateJobsResponse, error) {
	if stream.index >= len(stream.responses) {
		return nil, io.EOF
	}
	response := stream.responses[stream.index]
	stream.index++
	return response, nil
}

func (stream *workerStream) Context() context.Context {
	return stream.context
}

type blockingWorkerStream struct {
	grpc.ClientStream
	context context.Context
}

func (stream *blockingWorkerStream) Recv() (*pb.ActivateJobsResponse, error) {
	<-stream.context.Done()
	return nil, stream.context.Err()
}

func (stream *blockingWorkerStream) Context() context.Context {
	return stream.context
}

type deadlineWorkerStream struct {
	grpc.ClientStream
	context context.Context
}

func (*deadlineWorkerStream) Recv() (*pb.ActivateJobsResponse, error) {
	return nil, status.Error(codes.DeadlineExceeded, "long poll expired")
}

func (stream *deadlineWorkerStream) Context() context.Context {
	return stream.context
}
