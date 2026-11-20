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
	"reflect"
	"testing"
	"time"

	"github.com/taskite-io/taskite/clients/go/pkg/entities"
	"github.com/taskite-io/taskite/clients/go/pkg/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestCreateJobPreservesExplicitDefaults(t *testing.T) {
	t.Parallel()
	gateway := &recordingGateway{}
	_, err := NewCreateJobCommand(gateway).
		JobType("charge").
		Retries(0).Priority(0).
		Variables(map[string]any{"orderId": 42}).
		Send(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if gateway.create.GetJobType() != "charge" ||
		gateway.create.Retries == nil || *gateway.create.Retries != 0 ||
		gateway.create.Priority == nil || *gateway.create.Priority != 0 ||
		gateway.create.GetVariables() != `{"orderId":42}` {
		t.Fatalf("unexpected create request: %+v", gateway.create)
	}
}

func TestCreateJobSubscriptionWithOnlyJobType(t *testing.T) {
	t.Parallel()
	for _, variables := range []string{"", "   "} {
		gateway := &recordingGateway{}
		command := NewCreateJobSubscriptionCommand(gateway).JobType("charge")
		if variables != "" {
			command.VariablesFromString(variables)
		}
		if _, err := command.Send(context.Background()); err != nil {
			t.Fatal(err)
		}
		if gateway.subscription.GetJobType() != "charge" ||
			gateway.subscription.GetName() != "" ||
			gateway.subscription.GetVariables() != "{}" ||
			gateway.subscription.Retries != nil ||
			gateway.subscription.Priority != nil {
			t.Fatalf("unexpected subscription request: %+v", gateway.subscription)
		}
	}
}

func TestCommandsMapEveryGatewayRPC(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	gateway := &recordingGateway{}

	subscription, err := NewCreateJobSubscriptionCommand(gateway).
		JobType("charge").Name("payment").
		Variables(map[string]any{"amount": 42}).
		Send(ctx)
	if err != nil || subscription.GetKey() != 101 {
		t.Fatalf("create subscription: response=%v err=%v", subscription, err)
	}
	if gateway.subscription.GetName() != "payment" ||
		gateway.subscription.GetJobType() != "charge" ||
		gateway.subscription.GetVariables() != `{"amount":42}` {
		t.Fatalf("unexpected subscription request: %+v", gateway.subscription)
	}

	timer, err := NewCreateTimerSubscriptionCommand(gateway).Name("daily").
		Type(pb.TimerType_CRON).Expression("0 * * * * *").
		JobTypes("charge", "notify").VariablesFromString(`{"amount":42}`).Send(ctx)
	if err != nil || timer.GetKey() != 9007199254740993 ||
		gateway.timer.GetType() != pb.TimerType_CRON ||
		gateway.timer.GetExpression() != "0 * * * * *" ||
		!reflect.DeepEqual(gateway.timer.GetJobTypes(), []string{"charge", "notify"}) ||
		gateway.timer.GetVariables() != `{"amount":42}` {
		t.Fatalf("timer: request=%v response=%v err=%v", gateway.timer, timer, err)
	}
	deleteTimer, err := NewDeleteTimerSubscriptionCommand(gateway).Key(timer.GetKey()).Send(ctx)
	if err != nil || deleteTimer.GetKey() != timer.GetKey() {
		t.Fatalf("delete timer: response=%v err=%v", deleteTimer, err)
	}

	deleted, err := NewDeleteJobSubscriptionCommand(gateway).Key(101).Send(ctx)
	if err != nil || deleted.GetKey() != 101 || gateway.delete.GetKey() != 101 {
		t.Fatalf("delete subscription: request=%v response=%v err=%v", gateway.delete, deleted, err)
	}

	jobCreated, err := NewCreateJobCommand(gateway).
		JobType("charge").
		Variables(map[string]any{"orderId": 42}).
		Send(ctx)
	if err != nil || jobCreated.GetJobKey() != 201 {
		t.Fatalf("create job: response=%v err=%v", jobCreated, err)
	}
	if gateway.create.GetJobType() != "charge" ||
		gateway.create.GetVariables() != `{"orderId":42}` {
		t.Fatalf("unexpected create request: %+v", gateway.create)
	}

	jobs, err := NewActivateJobsCommand(gateway).
		JobType("payment").
		WorkerName("worker-a").
		WorkerSession("instance-a", 123).
		Timeout(time.Minute).
		MaxJobsToActivate(2).
		FetchVariables("orderId").
		RequestTimeout(2 * time.Second).
		TenantIDs("tenant-a").
		Send(ctx)
	if err != nil || len(jobs) != 2 || jobs[0].GetKey() != 301 || jobs[1].GetKey() != 302 {
		t.Fatalf("activate jobs: jobs=%v err=%v", jobs, err)
	}
	if gateway.activate.GetType() != "payment" ||
		gateway.activate.GetWorkerId() != "instance-a" || gateway.activate.GetWorkerLeaseToken() != 123 ||
		gateway.activate.GetWorker() != "worker-a" ||
		gateway.activate.GetTimeout() != 60_000 ||
		gateway.activate.GetMaxJobsToActivate() != 2 ||
		!reflect.DeepEqual(gateway.activate.GetFetchVariable(), []string{"orderId"}) ||
		!reflect.DeepEqual(gateway.activate.GetTenantIds(), []string{"tenant-a"}) {
		t.Fatalf("unexpected activate request: %+v", gateway.activate)
	}

	completed, err := NewCompleteJobCommand(gateway).
		JobKey(301).
		VariablesFromString(`{"paid":true}`).
		Send(ctx)
	if err != nil || completed.GetJobKey() != 301 {
		t.Fatalf("complete job: response=%v err=%v", completed, err)
	}
	if gateway.complete.GetJobKey() != 301 ||
		gateway.complete.GetVariables() != `{"paid":true}` {
		t.Fatalf("unexpected complete request: %+v", gateway.complete)
	}

	_, err = NewFailJobCommand(gateway).
		JobKey(302).
		Retries(2).
		ErrorMessage("declined").
		RetryBackoff(3 * time.Second).
		Variables(map[string]any{"failed": true}).
		Send(ctx)
	if err != nil {
		t.Fatalf("fail job: %v", err)
	}
	if gateway.fail.GetJobKey() != 302 ||
		gateway.fail.GetRetries() != 2 ||
		gateway.fail.GetErrorMessage() != "declined" ||
		gateway.fail.GetRetryBackOff() != 3_000 ||
		gateway.fail.GetVariables() != `{"failed":true}` {
		t.Fatalf("unexpected fail request: %+v", gateway.fail)
	}

	job, err := NewQueryJobCommand(gateway).JobKey(303).Send(ctx)
	if err != nil || job.GetKey() != 303 || gateway.queryJob.GetJobKey() != 303 {
		t.Fatalf("query job: request=%v response=%v err=%v", gateway.queryJob, job, err)
	}

	canceled, err := NewCancelJobCommand(gateway).JobKey(201).Send(ctx)
	if err != nil ||
		canceled.GetJobKey() != 201 ||
		gateway.cancel.GetJobKey() != 201 {
		t.Fatalf("cancel job: request=%v response=%v err=%v", gateway.cancel, canceled, err)
	}

	incident, err := NewQueryIncidentCommand(gateway).JobKey(301).Send(ctx)
	if err != nil || incident.GetKey() != 401 || gateway.queryIncident.GetJobKey() != 301 {
		t.Fatalf(
			"query incident: request=%v response=%v err=%v",
			gateway.queryIncident,
			incident,
			err,
		)
	}

	if _, err := NewResolveIncidentCommand(gateway).IncidentKey(401).Send(ctx); err != nil {
		t.Fatalf("resolve incident: %v", err)
	}
	if gateway.resolve.GetIncidentKey() != 401 {
		t.Fatalf("unexpected resolve request: %+v", gateway.resolve)
	}

	variables, err := NewSetVariablesCommand(gateway).
		JobKey(201).
		VariablesFromString(`{"status":"paid"}`).
		Send(ctx)
	if err != nil || variables.GetJobKey() != 201 {
		t.Fatalf("set variables: response=%v err=%v", variables, err)
	}
	if gateway.setVariables.GetJobKey() != 201 ||
		gateway.setVariables.GetVariables() != `{"status":"paid"}` {
		t.Fatalf("unexpected set variables request: %+v", gateway.setVariables)
	}

	if _, err := NewUpdateJobRetriesCommand(gateway).JobKey(301).Retries(0).Send(ctx); err != nil {
		t.Fatal(err)
	}
	if gateway.updateRetries.GetJobKey() != 301 || gateway.updateRetries.GetRetries() != 0 {
		t.Fatalf("unexpected retries request: %+v", gateway.updateRetries)
	}
	if _, err := NewUpdateJobPriorityCommand(gateway).JobKey(301).Priority(-2147483648).Send(ctx); err != nil {
		t.Fatal(err)
	}
	if gateway.updatePriority.GetJobKey() != 301 || gateway.updatePriority.GetPriority() != -2147483648 {
		t.Fatalf("unexpected priority request: %+v", gateway.updatePriority)
	}
	if _, err := NewUpdateJobTimeoutCommand(gateway).JobKey(301).
		TimeoutMillis(9007199254740993).WorkerSession("owner", 9007199254741099).Send(ctx); err != nil {
		t.Fatal(err)
	}
	if gateway.updateTimeout.GetJobKey() != 301 ||
		gateway.updateTimeout.GetTimeout() != 9007199254740993 ||
		gateway.updateTimeout.GetWorkerId() != "owner" ||
		gateway.updateTimeout.GetWorkerLeaseToken() != 9007199254741099 {
		t.Fatalf("unexpected timeout request: %+v", gateway.updateTimeout)
	}
}

func TestJobSessionBinding(t *testing.T) {
	for _, mode := range []string{"job", "explicit", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			gateway := &recordingGateway{}
			job := entities.Job{ActivatedJob: &pb.ActivatedJob{Key: 3, WorkerId: "instance-a", WorkerLeaseToken: 9007199254741099, TenantId: "tenant-b"}}
			complete := NewCompleteJobCommand(gateway).JobKey(3)
			fail := NewFailJobCommand(gateway).JobKey(3)
			if mode == "job" {
				complete = NewCompleteJobCommand(gateway).Job(job)
				fail = NewFailJobCommand(gateway).Job(job)
			} else if mode == "explicit" {
				complete = complete.WorkerSession(job.GetWorkerId(), job.GetWorkerLeaseToken()).TenantID(job.GetTenantId())
				fail = fail.WorkerSession(job.GetWorkerId(), job.GetWorkerLeaseToken()).TenantID(job.GetTenantId())
			}
			ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "existing", "taskite-tenant-id", "tenant-a"))
			if _, err := complete.Send(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := fail.Send(ctx); err != nil {
				t.Fatal(err)
			}
			if mode == "legacy" {
				if gateway.complete.GetWorkerLeaseToken() != 0 || gateway.fail.GetWorkerLeaseToken() != 0 {
					t.Fatal("legacy command invented a token")
				}
				return
			}
			if gateway.complete.GetWorkerId() != job.GetWorkerId() || gateway.complete.GetWorkerLeaseToken() != job.GetWorkerLeaseToken() ||
				gateway.fail.GetWorkerId() != job.GetWorkerId() || gateway.fail.GetWorkerLeaseToken() != job.GetWorkerLeaseToken() {
				t.Fatal("command lost Job ownership")
			}
			if values := gateway.jobMetadata.Get("taskite-tenant-id"); len(values) != 1 || values[0] != "tenant-b" ||
				gateway.jobMetadata.Get("authorization")[0] != "existing" {
				t.Fatal("Job tenant metadata did not replace the selection while preserving authorization")
			}
			original, _ := metadata.FromOutgoingContext(ctx)
			if original.Get("taskite-tenant-id")[0] != "tenant-a" {
				t.Fatal("command mutated parent metadata")
			}
		})
	}
}

type recordingGateway struct {
	jobMetadata metadata.MD
	pb.GatewayClient
	subscription   *pb.CreateJobSubscriptionRequest
	timer          *pb.CreateTimerSubscriptionRequest
	deleteTimer    *pb.DeleteTimerSubscriptionRequest
	delete         *pb.DeleteJobSubscriptionRequest
	create         *pb.CreateJobRequest
	activate       *pb.ActivateJobsRequest
	complete       *pb.CompleteJobRequest
	fail           *pb.FailJobRequest
	queryJob       *pb.QueryJobRequest
	cancel         *pb.CancelJobRequest
	queryIncident  *pb.QueryIncidentRequest
	resolve        *pb.ResolveIncidentRequest
	setVariables   *pb.SetVariablesRequest
	updateRetries  *pb.UpdateJobRetriesRequest
	updatePriority *pb.UpdateJobPriorityRequest
	updateTimeout  *pb.UpdateJobTimeoutRequest
}

func (gateway *recordingGateway) CreateTimerSubscription(_ context.Context, request *pb.CreateTimerSubscriptionRequest, _ ...grpc.CallOption) (*pb.CreateTimerSubscriptionResponse, error) {
	gateway.timer = request
	return &pb.CreateTimerSubscriptionResponse{Key: 9007199254740993}, nil
}

func (gateway *recordingGateway) DeleteTimerSubscription(_ context.Context, request *pb.DeleteTimerSubscriptionRequest, _ ...grpc.CallOption) (*pb.DeleteTimerSubscriptionResponse, error) {
	gateway.deleteTimer = request
	return &pb.DeleteTimerSubscriptionResponse{Key: request.GetKey()}, nil
}

func TestTimerVariablesMustBeObjectsAndJobTypesAreCopied(t *testing.T) {
	for _, input := range []string{"[]", "null", "{}", "{} {}", " ", "", `{"amount":42}`} {
		t.Run(input, func(t *testing.T) {
			gateway := &recordingGateway{}
			types := []string{"charge", "notify"}
			command := NewCreateTimerSubscriptionCommand(gateway).Name("daily").
				Type(pb.TimerType_DURATION).Expression("PT1S").JobTypes(types...).VariablesFromString(input)
			types[0] = "changed"
			_, err := command.Send(context.Background())
			valid := input == "{}" || input == "" || input == `{"amount":42}`
			if valid {
				if err != nil || !reflect.DeepEqual(gateway.timer.GetJobTypes(), []string{"charge", "notify"}) {
					t.Fatalf("timer request=%v err=%v", gateway.timer, err)
				}
			} else if err == nil || gateway.timer != nil {
				t.Fatalf("invalid variables reached gateway: request=%v err=%v", gateway.timer, err)
			}
		})
	}
}

func (gateway *recordingGateway) UpdateJobRetries(_ context.Context, request *pb.UpdateJobRetriesRequest, _ ...grpc.CallOption) (*pb.UpdateJobRetriesResponse, error) {
	gateway.updateRetries = request
	return &pb.UpdateJobRetriesResponse{}, nil
}

func (gateway *recordingGateway) UpdateJobPriority(_ context.Context, request *pb.UpdateJobPriorityRequest, _ ...grpc.CallOption) (*pb.UpdateJobPriorityResponse, error) {
	gateway.updatePriority = request
	return &pb.UpdateJobPriorityResponse{}, nil
}

func (gateway *recordingGateway) UpdateJobTimeout(_ context.Context, request *pb.UpdateJobTimeoutRequest, _ ...grpc.CallOption) (*pb.UpdateJobTimeoutResponse, error) {
	gateway.updateTimeout = request
	return &pb.UpdateJobTimeoutResponse{}, nil
}

func (gateway *recordingGateway) CreateJobSubscription(
	_ context.Context,
	request *pb.CreateJobSubscriptionRequest,
	_ ...grpc.CallOption,
) (*pb.CreateJobSubscriptionResponse, error) {
	gateway.subscription = request
	return &pb.CreateJobSubscriptionResponse{Key: 101}, nil
}

func (gateway *recordingGateway) DeleteJobSubscription(
	_ context.Context,
	request *pb.DeleteJobSubscriptionRequest,
	_ ...grpc.CallOption,
) (*pb.DeleteJobSubscriptionResponse, error) {
	gateway.delete = request
	return &pb.DeleteJobSubscriptionResponse{Key: request.GetKey()}, nil
}

func (gateway *recordingGateway) CreateJob(
	_ context.Context,
	request *pb.CreateJobRequest,
	_ ...grpc.CallOption,
) (*pb.CreateJobResponse, error) {
	gateway.create = request
	return &pb.CreateJobResponse{JobKey: 201}, nil
}

func (gateway *recordingGateway) ActivateJobs(
	_ context.Context,
	request *pb.ActivateJobsRequest,
	_ ...grpc.CallOption,
) (grpc.ServerStreamingClient[pb.ActivateJobsResponse], error) {
	gateway.activate = request
	return &activateJobsStream{
		responses: []*pb.ActivateJobsResponse{
			{Jobs: []*pb.ActivatedJob{{Key: 301}}},
			{Jobs: []*pb.ActivatedJob{{Key: 302}}},
		},
	}, nil
}

func (gateway *recordingGateway) CompleteJob(
	ctx context.Context,
	request *pb.CompleteJobRequest,
	_ ...grpc.CallOption,
) (*pb.CompleteJobResponse, error) {
	gateway.complete = request
	gateway.jobMetadata, _ = metadata.FromOutgoingContext(ctx)
	return &pb.CompleteJobResponse{JobKey: request.GetJobKey()}, nil
}

func (gateway *recordingGateway) FailJob(
	ctx context.Context,
	request *pb.FailJobRequest,
	_ ...grpc.CallOption,
) (*pb.FailJobResponse, error) {
	gateway.fail = request
	gateway.jobMetadata, _ = metadata.FromOutgoingContext(ctx)
	return &pb.FailJobResponse{}, nil
}

func (gateway *recordingGateway) QueryJob(
	_ context.Context,
	request *pb.QueryJobRequest,
	_ ...grpc.CallOption,
) (*pb.ActivatedJob, error) {
	gateway.queryJob = request
	return &pb.ActivatedJob{Key: request.GetJobKey()}, nil
}

func (gateway *recordingGateway) CancelJob(
	_ context.Context,
	request *pb.CancelJobRequest,
	_ ...grpc.CallOption,
) (*pb.CancelJobResponse, error) {
	gateway.cancel = request
	return &pb.CancelJobResponse{
		JobKey: request.GetJobKey(),
	}, nil
}

func (gateway *recordingGateway) QueryIncident(
	_ context.Context,
	request *pb.QueryIncidentRequest,
	_ ...grpc.CallOption,
) (*pb.QueryIncidentResponse, error) {
	gateway.queryIncident = request
	return &pb.QueryIncidentResponse{Key: 401}, nil
}

func (gateway *recordingGateway) ResolveIncident(
	_ context.Context,
	request *pb.ResolveIncidentRequest,
	_ ...grpc.CallOption,
) (*pb.ResolveIncidentResponse, error) {
	gateway.resolve = request
	return &pb.ResolveIncidentResponse{}, nil
}

func (gateway *recordingGateway) SetVariables(
	_ context.Context,
	request *pb.SetVariablesRequest,
	_ ...grpc.CallOption,
) (*pb.SetVariablesResponse, error) {
	gateway.setVariables = request
	return &pb.SetVariablesResponse{JobKey: request.GetJobKey()}, nil
}

type activateJobsStream struct {
	grpc.ClientStream
	responses []*pb.ActivateJobsResponse
	index     int
}

func (stream *activateJobsStream) Recv() (*pb.ActivateJobsResponse, error) {
	if stream.index >= len(stream.responses) {
		return nil, io.EOF
	}
	response := stream.responses[stream.index]
	stream.index++
	return response, nil
}

func (stream *activateJobsStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (stream *activateJobsStream) Trailer() metadata.MD {
	return nil
}

func (stream *activateJobsStream) CloseSend() error {
	return nil
}

func (stream *activateJobsStream) Context() context.Context {
	return context.Background()
}

func (stream *activateJobsStream) SendMsg(any) error {
	return nil
}

func (stream *activateJobsStream) RecvMsg(any) error {
	return nil
}
