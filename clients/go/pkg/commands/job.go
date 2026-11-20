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
	"time"

	"github.com/taskite-io/taskite/clients/go/pkg/entities"
	"github.com/taskite-io/taskite/clients/go/pkg/pb"
)

const (
	DefaultJobTimeout     = 5 * time.Minute
	DefaultJobWorkerName  = "taskite-go"
	DefaultRequestTimeout = 10 * time.Second
	DefaultJobsToActivate = int32(100)
	DisableLongPolling    = -1 * time.Millisecond
)

type ActivateJobsCommandStep1 interface {
	JobType(jobType string) ActivateJobsCommandStep2
}

type ActivateJobsCommandStep2 interface {
	WorkerSession(workerID string, token int64) ActivateJobsCommandStep2
	WorkerName(worker string) ActivateJobsCommandStep2
	Timeout(timeout time.Duration) ActivateJobsCommandStep2
	MaxJobsToActivate(maxJobs int32) ActivateJobsCommandStep2
	FetchVariables(names ...string) ActivateJobsCommandStep2
	RequestTimeout(timeout time.Duration) ActivateJobsCommandStep2
	TenantIDs(tenantIDs ...string) ActivateJobsCommandStep2
	Send(context.Context) ([]entities.Job, error)
}

type ActivateJobsCommand struct {
	gateway pb.GatewayClient
	request pb.ActivateJobsRequest
}

func NewActivateJobsCommand(gateway pb.GatewayClient) ActivateJobsCommandStep1 {
	return &ActivateJobsCommand{
		gateway: gateway,
		request: pb.ActivateJobsRequest{
			Worker:            DefaultJobWorkerName,
			Timeout:           DefaultJobTimeout.Milliseconds(),
			MaxJobsToActivate: DefaultJobsToActivate,
			RequestTimeout:    DefaultRequestTimeout.Milliseconds(),
		},
	}
}

func (command *ActivateJobsCommand) JobType(jobType string) ActivateJobsCommandStep2 {
	command.request.Type = jobType
	return command
}

func (command *ActivateJobsCommand) WorkerName(worker string) ActivateJobsCommandStep2 {
	command.request.Worker = worker
	return command
}

func (command *ActivateJobsCommand) WorkerSession(workerID string, token int64) ActivateJobsCommandStep2 {
	command.request.WorkerId = workerID
	command.request.WorkerLeaseToken = token
	return command
}

func (command *ActivateJobsCommand) Timeout(timeout time.Duration) ActivateJobsCommandStep2 {
	command.request.Timeout = timeout.Milliseconds()
	return command
}

func (command *ActivateJobsCommand) MaxJobsToActivate(
	maxJobs int32,
) ActivateJobsCommandStep2 {
	command.request.MaxJobsToActivate = maxJobs
	return command
}

func (command *ActivateJobsCommand) FetchVariables(
	names ...string,
) ActivateJobsCommandStep2 {
	command.request.FetchVariable = append([]string(nil), names...)
	return command
}

func (command *ActivateJobsCommand) RequestTimeout(
	timeout time.Duration,
) ActivateJobsCommandStep2 {
	command.request.RequestTimeout = timeout.Milliseconds()
	return command
}

func (command *ActivateJobsCommand) TenantIDs(
	tenantIDs ...string,
) ActivateJobsCommandStep2 {
	command.request.TenantIds = append([]string(nil), tenantIDs...)
	return command
}

func (command *ActivateJobsCommand) Send(ctx context.Context) ([]entities.Job, error) {
	if len(command.request.TenantIds) > 0 {
		ctx = WithTenantID(ctx, command.request.TenantIds[0])
	}
	stream, err := command.gateway.ActivateJobs(ctx, &command.request)
	if err != nil {
		return nil, err
	}

	jobs := make([]entities.Job, 0)
	for {
		response, receiveErr := stream.Recv()
		if receiveErr == io.EOF {
			return jobs, nil
		}
		if receiveErr != nil {
			return jobs, receiveErr
		}
		for _, job := range response.GetJobs() {
			jobs = append(jobs, entities.Job{ActivatedJob: job})
		}
	}
}

type CompleteJobCommandStep1 interface {
	JobKey(key int64) CompleteJobCommandStep2
	Job(job entities.Job) CompleteJobCommandStep2
}

type CompleteJobCommandStep2 interface {
	WorkerSession(workerID string, token int64) CompleteJobCommandStep2
	TenantID(tenantID string) CompleteJobCommandStep2
	Variables(value any) CompleteJobCommandStep2
	VariablesFromString(value string) CompleteJobCommandStep2
	Send(context.Context) (*pb.CompleteJobResponse, error)
}

type CompleteJobCommand struct {
	gateway   pb.GatewayClient
	request   pb.CompleteJobRequest
	variables variablesState
	tenantID  string
}

func NewCompleteJobCommand(gateway pb.GatewayClient) CompleteJobCommandStep1 {
	return &CompleteJobCommand{
		gateway:   gateway,
		variables: defaultVariables(),
	}
}

func (command *CompleteJobCommand) JobKey(key int64) CompleteJobCommandStep2 {
	command.request.JobKey = key
	return command
}

func (command *CompleteJobCommand) Job(job entities.Job) CompleteJobCommandStep2 {
	return command.JobKey(job.GetKey()).
		WorkerSession(job.GetWorkerId(), job.GetWorkerLeaseToken()).
		TenantID(job.GetTenantId())
}

func (command *CompleteJobCommand) WorkerSession(workerID string, token int64) CompleteJobCommandStep2 {
	command.request.WorkerId = workerID
	command.request.WorkerLeaseToken = token
	return command
}

func (command *CompleteJobCommand) TenantID(tenantID string) CompleteJobCommandStep2 {
	command.tenantID = tenantID
	return command
}

func (command *CompleteJobCommand) Variables(value any) CompleteJobCommandStep2 {
	command.variables.fromValue(value)
	return command
}

func (command *CompleteJobCommand) VariablesFromString(
	value string,
) CompleteJobCommandStep2 {
	command.variables.fromString(value)
	return command
}

func (command *CompleteJobCommand) Send(
	ctx context.Context,
) (*pb.CompleteJobResponse, error) {
	if command.variables.err != nil {
		return nil, command.variables.err
	}
	command.request.Variables = command.variables.value
	return command.gateway.CompleteJob(WithTenantID(ctx, command.tenantID), &command.request)
}

type FailJobCommandStep1 interface {
	JobKey(key int64) FailJobCommandStep2
	Job(job entities.Job) FailJobCommandStep2
}

type FailJobCommandStep2 interface {
	WorkerSession(workerID string, token int64) FailJobCommandStep2
	TenantID(tenantID string) FailJobCommandStep2
	Retries(retries int32) FailJobCommandStep2
	ErrorMessage(message string) FailJobCommandStep2
	RetryBackoff(backoff time.Duration) FailJobCommandStep2
	Variables(value any) FailJobCommandStep2
	VariablesFromString(value string) FailJobCommandStep2
	Send(context.Context) (*pb.FailJobResponse, error)
}

type FailJobCommand struct {
	gateway   pb.GatewayClient
	request   pb.FailJobRequest
	variables variablesState
	tenantID  string
}

func NewFailJobCommand(gateway pb.GatewayClient) FailJobCommandStep1 {
	return &FailJobCommand{
		gateway:   gateway,
		variables: defaultVariables(),
	}
}

func (command *FailJobCommand) JobKey(key int64) FailJobCommandStep2 {
	command.request.JobKey = key
	return command
}

func (command *FailJobCommand) Job(job entities.Job) FailJobCommandStep2 {
	return command.JobKey(job.GetKey()).
		WorkerSession(job.GetWorkerId(), job.GetWorkerLeaseToken()).
		TenantID(job.GetTenantId())
}

func (command *FailJobCommand) WorkerSession(workerID string, token int64) FailJobCommandStep2 {
	command.request.WorkerId = workerID
	command.request.WorkerLeaseToken = token
	return command
}

func (command *FailJobCommand) TenantID(tenantID string) FailJobCommandStep2 {
	command.tenantID = tenantID
	return command
}

func (command *FailJobCommand) Retries(retries int32) FailJobCommandStep2 {
	command.request.Retries = retries
	return command
}

func (command *FailJobCommand) ErrorMessage(message string) FailJobCommandStep2 {
	command.request.ErrorMessage = message
	return command
}

func (command *FailJobCommand) RetryBackoff(backoff time.Duration) FailJobCommandStep2 {
	command.request.RetryBackOff = backoff.Milliseconds()
	return command
}

func (command *FailJobCommand) Variables(value any) FailJobCommandStep2 {
	command.variables.fromValue(value)
	return command
}

func (command *FailJobCommand) VariablesFromString(value string) FailJobCommandStep2 {
	command.variables.fromString(value)
	return command
}

func (command *FailJobCommand) Send(
	ctx context.Context,
) (*pb.FailJobResponse, error) {
	if command.variables.err != nil {
		return nil, command.variables.err
	}
	command.request.Variables = command.variables.value
	return command.gateway.FailJob(WithTenantID(ctx, command.tenantID), &command.request)
}

type QueryJobCommandStep1 interface {
	JobKey(key int64) QueryJobCommandStep2
}

type QueryJobCommandStep2 interface {
	Send(context.Context) (*pb.ActivatedJob, error)
}

type QueryJobCommand struct {
	gateway pb.GatewayClient
	request pb.QueryJobRequest
}

func NewQueryJobCommand(gateway pb.GatewayClient) QueryJobCommandStep1 {
	return &QueryJobCommand{gateway: gateway}
}

func (command *QueryJobCommand) JobKey(key int64) QueryJobCommandStep2 {
	command.request.JobKey = key
	return command
}

func (command *QueryJobCommand) Send(ctx context.Context) (*pb.ActivatedJob, error) {
	return command.gateway.QueryJob(ctx, &command.request)
}

type CreateJobCommandStep1 interface {
	JobType(value string) CreateJobCommandStep2
}
type CreateJobCommandStep2 interface {
	Variables(value any) CreateJobCommandStep2
	VariablesFromString(value string) CreateJobCommandStep2
	Retries(value int32) CreateJobCommandStep2
	Priority(value int32) CreateJobCommandStep2
	Send(context.Context) (*pb.CreateJobResponse, error)
}

type CreateJobCommand struct {
	gateway   pb.GatewayClient
	request   pb.CreateJobRequest
	variables variablesState
}

func NewCreateJobCommand(gateway pb.GatewayClient) CreateJobCommandStep1 {
	command := &CreateJobCommand{gateway: gateway, variables: defaultVariables()}

	return command
}

func (command *CreateJobCommand) JobType(value string) CreateJobCommandStep2 {
	command.request.JobType = value
	return command
}

func (command *CreateJobCommand) Variables(value any) CreateJobCommandStep2 {
	command.variables.fromValue(value)
	return command
}

func (command *CreateJobCommand) VariablesFromString(value string) CreateJobCommandStep2 {
	command.variables.fromString(value)
	return command
}

func (command *CreateJobCommand) Retries(value int32) CreateJobCommandStep2 {
	command.request.Retries = &value
	return command
}

func (command *CreateJobCommand) Priority(value int32) CreateJobCommandStep2 {
	command.request.Priority = &value
	return command
}

func (command *CreateJobCommand) Send(ctx context.Context) (*pb.CreateJobResponse, error) {
	if command.variables.err != nil {
		return nil, command.variables.err
	}
	command.request.Variables = command.variables.value
	return command.gateway.CreateJob(ctx, &command.request)
}

type CreateJobSubscriptionCommandStep1 interface {
	JobType(value string) CreateJobSubscriptionCommandStep2
}
type CreateJobSubscriptionCommandStep2 interface {
	Name(value string) CreateJobSubscriptionCommandStep2
	Variables(value any) CreateJobSubscriptionCommandStep2
	VariablesFromString(value string) CreateJobSubscriptionCommandStep2
	Retries(value int32) CreateJobSubscriptionCommandStep2
	Priority(value int32) CreateJobSubscriptionCommandStep2
	Send(context.Context) (*pb.CreateJobSubscriptionResponse, error)
}

type CreateJobSubscriptionCommand struct {
	gateway   pb.GatewayClient
	request   pb.CreateJobSubscriptionRequest
	variables variablesState
}

func NewCreateJobSubscriptionCommand(gateway pb.GatewayClient) CreateJobSubscriptionCommandStep1 {
	command := &CreateJobSubscriptionCommand{gateway: gateway, variables: defaultVariables()}

	return command
}

func (command *CreateJobSubscriptionCommand) Name(value string) CreateJobSubscriptionCommandStep2 {
	command.request.Name = value
	return command
}

func (command *CreateJobSubscriptionCommand) JobType(value string) CreateJobSubscriptionCommandStep2 {
	command.request.JobType = value
	return command
}

func (command *CreateJobSubscriptionCommand) Variables(value any) CreateJobSubscriptionCommandStep2 {
	command.variables.fromValue(value)
	return command
}

func (command *CreateJobSubscriptionCommand) VariablesFromString(value string) CreateJobSubscriptionCommandStep2 {
	command.variables.fromString(value)
	return command
}

func (command *CreateJobSubscriptionCommand) Retries(value int32) CreateJobSubscriptionCommandStep2 {
	command.request.Retries = &value
	return command
}

func (command *CreateJobSubscriptionCommand) Priority(value int32) CreateJobSubscriptionCommandStep2 {
	command.request.Priority = &value
	return command
}

func (command *CreateJobSubscriptionCommand) Send(ctx context.Context) (*pb.CreateJobSubscriptionResponse, error) {
	if command.variables.err != nil {
		return nil, command.variables.err
	}
	command.request.Variables = command.variables.value
	return command.gateway.CreateJobSubscription(ctx, &command.request)
}

type DeleteJobSubscriptionCommandStep1 interface {
	Key(key int64) DeleteJobSubscriptionCommandStep2
}
type DeleteJobSubscriptionCommandStep2 interface {
	Send(context.Context) (*pb.DeleteJobSubscriptionResponse, error)
}

type DeleteJobSubscriptionCommand struct {
	gateway   pb.GatewayClient
	request   pb.DeleteJobSubscriptionRequest
	variables variablesState
}

func NewDeleteJobSubscriptionCommand(gateway pb.GatewayClient) DeleteJobSubscriptionCommandStep1 {
	command := &DeleteJobSubscriptionCommand{gateway: gateway, variables: defaultVariables()}

	return command
}

func (command *DeleteJobSubscriptionCommand) Key(key int64) DeleteJobSubscriptionCommandStep2 {
	command.request.Key = key
	return command
}

func (command *DeleteJobSubscriptionCommand) Send(ctx context.Context) (*pb.DeleteJobSubscriptionResponse, error) {
	if command.variables.err != nil {
		return nil, command.variables.err
	}
	return command.gateway.DeleteJobSubscription(ctx, &command.request)
}

type CancelJobCommandStep1 interface {
	JobKey(key int64) CancelJobCommandStep2
}
type CancelJobCommandStep2 interface {
	Send(context.Context) (*pb.CancelJobResponse, error)
}

type CancelJobCommand struct {
	gateway   pb.GatewayClient
	request   pb.CancelJobRequest
	variables variablesState
}

func NewCancelJobCommand(gateway pb.GatewayClient) CancelJobCommandStep1 {
	command := &CancelJobCommand{gateway: gateway, variables: defaultVariables()}

	return command
}

func (command *CancelJobCommand) JobKey(key int64) CancelJobCommandStep2 {
	command.request.JobKey = key
	return command
}

func (command *CancelJobCommand) Send(ctx context.Context) (*pb.CancelJobResponse, error) {
	if command.variables.err != nil {
		return nil, command.variables.err
	}
	return command.gateway.CancelJob(ctx, &command.request)
}

type UpdateJobRetriesCommandStep1 interface {
	JobKey(key int64) UpdateJobRetriesCommandStep2
}
type UpdateJobRetriesCommandStep2 interface {
	Retries(value int32) UpdateJobRetriesCommandStep2
	Send(context.Context) (*pb.UpdateJobRetriesResponse, error)
}

type UpdateJobRetriesCommand struct {
	gateway   pb.GatewayClient
	request   pb.UpdateJobRetriesRequest
	variables variablesState
}

func NewUpdateJobRetriesCommand(gateway pb.GatewayClient) UpdateJobRetriesCommandStep1 {
	command := &UpdateJobRetriesCommand{gateway: gateway, variables: defaultVariables()}

	return command
}

func (command *UpdateJobRetriesCommand) JobKey(key int64) UpdateJobRetriesCommandStep2 {
	command.request.JobKey = key
	return command
}

func (command *UpdateJobRetriesCommand) Retries(value int32) UpdateJobRetriesCommandStep2 {
	command.request.Retries = value
	return command
}

func (command *UpdateJobRetriesCommand) Send(ctx context.Context) (*pb.UpdateJobRetriesResponse, error) {
	if command.variables.err != nil {
		return nil, command.variables.err
	}
	return command.gateway.UpdateJobRetries(ctx, &command.request)
}

type UpdateJobPriorityCommandStep1 interface {
	JobKey(key int64) UpdateJobPriorityCommandStep2
}
type UpdateJobPriorityCommandStep2 interface {
	Priority(value int32) UpdateJobPriorityCommandStep2
	Send(context.Context) (*pb.UpdateJobPriorityResponse, error)
}

type UpdateJobPriorityCommand struct {
	gateway   pb.GatewayClient
	request   pb.UpdateJobPriorityRequest
	variables variablesState
}

func NewUpdateJobPriorityCommand(gateway pb.GatewayClient) UpdateJobPriorityCommandStep1 {
	command := &UpdateJobPriorityCommand{gateway: gateway, variables: defaultVariables()}

	return command
}

func (command *UpdateJobPriorityCommand) JobKey(key int64) UpdateJobPriorityCommandStep2 {
	command.request.JobKey = key
	return command
}

func (command *UpdateJobPriorityCommand) Priority(value int32) UpdateJobPriorityCommandStep2 {
	command.request.Priority = value
	return command
}

func (command *UpdateJobPriorityCommand) Send(ctx context.Context) (*pb.UpdateJobPriorityResponse, error) {
	if command.variables.err != nil {
		return nil, command.variables.err
	}
	return command.gateway.UpdateJobPriority(ctx, &command.request)
}

type UpdateJobTimeoutCommandStep1 interface {
	JobKey(key int64) UpdateJobTimeoutCommandStep2
}
type UpdateJobTimeoutCommandStep2 interface {
	TimeoutMillis(value int64) UpdateJobTimeoutCommandStep2
	WorkerSession(id string, token int64) UpdateJobTimeoutCommandStep2
	Send(context.Context) (*pb.UpdateJobTimeoutResponse, error)
}

type UpdateJobTimeoutCommand struct {
	gateway   pb.GatewayClient
	request   pb.UpdateJobTimeoutRequest
	variables variablesState
}

func NewUpdateJobTimeoutCommand(gateway pb.GatewayClient) UpdateJobTimeoutCommandStep1 {
	command := &UpdateJobTimeoutCommand{gateway: gateway, variables: defaultVariables()}

	return command
}

func (command *UpdateJobTimeoutCommand) JobKey(key int64) UpdateJobTimeoutCommandStep2 {
	command.request.JobKey = key
	return command
}

func (command *UpdateJobTimeoutCommand) TimeoutMillis(value int64) UpdateJobTimeoutCommandStep2 {
	command.request.Timeout = value
	return command
}

func (command *UpdateJobTimeoutCommand) WorkerSession(id string, token int64) UpdateJobTimeoutCommandStep2 {
	command.request.WorkerId = id
	command.request.WorkerLeaseToken = token
	return command
}

func (command *UpdateJobTimeoutCommand) Send(ctx context.Context) (*pb.UpdateJobTimeoutResponse, error) {
	if command.variables.err != nil {
		return nil, command.variables.err
	}
	return command.gateway.UpdateJobTimeout(ctx, &command.request)
}
