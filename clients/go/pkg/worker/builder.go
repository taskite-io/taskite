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
	"fmt"
	"strings"
	"time"

	"github.com/taskite-io/taskite/clients/go/pkg/commands"
	"github.com/taskite-io/taskite/clients/go/pkg/entities"
	"github.com/taskite-io/taskite/clients/go/pkg/pb"
)

const (
	DefaultMaxJobsActive = 32
	DefaultConcurrency   = 4
	DefaultPollInterval  = 100 * time.Millisecond
)

type JobClient interface {
	NewCompleteJobCommand() commands.CompleteJobCommandStep1
	NewFailJobCommand() commands.FailJobCommandStep1
}

type JobHandler func(client JobClient, job entities.Job)

type RegistryConfig struct {
	ClusterID     string
	TenantID      string
	ClientVersion string
	Registration  RegistrationConfig
}

type RegistryClient interface {
	Register(ctx context.Context, request RegistrationRequest) (RegistrationResult, error)
	Renew(ctx context.Context, request RegistrationRequest) (RegistrationResult, error)
	Deregister(ctx context.Context, request DeregisterRequest) error
}

type RegistrationResult struct {
	WorkerLeaseToken         int64
	LeaseDeadlineEpochMillis int64
}

// RegistrationRequest describes one Open; WorkerID also identifies REGISTER retries.
type RegistrationRequest struct {
	WorkerLeaseToken     int64
	ClusterID            string   `json:"clusterId"`
	TenantIDs            []string `json:"tenantIds"`
	WorkerID             string   `json:"workerId"`
	Name                 string   `json:"name"`
	JobType              string   `json:"jobType"`
	MaxJobsActive        int      `json:"maxJobsActive"`
	ActiveJobs           int      `json:"activeJobs"`
	Concurrency          int      `json:"concurrency"`
	TimeoutMillis        int64    `json:"timeoutMillis"`
	RequestTimeoutMillis int64    `json:"requestTimeoutMillis"`
	PollIntervalMillis   int64    `json:"pollIntervalMillis"`
	FetchVariables       []string `json:"fetchVariables"`
	ClientVersion        string   `json:"clientVersion"`
	RenewalTimeoutMillis int64    `json:"renewalTimeoutMillis"`
}

type DeregisterRequest struct {
	WorkerLeaseToken int64    `json:"workerLeaseToken"`
	ClusterID        string   `json:"clusterId"`
	TenantIDs        []string `json:"tenantIds"`
	WorkerID         string   `json:"workerId"`
	Name             string   `json:"name"`
}

type JobWorkerBuilderStep1 interface {
	JobType(jobType string) JobWorkerBuilderStep2
}

type JobWorkerBuilderStep2 interface {
	Handler(handler JobHandler) JobWorkerBuilderStep3
}

type JobWorkerBuilderStep3 interface {
	Name(name string) JobWorkerBuilderStep3
	Timeout(timeout time.Duration) JobWorkerBuilderStep3
	RequestTimeout(timeout time.Duration) JobWorkerBuilderStep3
	MaxJobsActive(maxJobs int) JobWorkerBuilderStep3
	Concurrency(concurrency int) JobWorkerBuilderStep3
	PollInterval(interval time.Duration) JobWorkerBuilderStep3
	FetchVariables(names ...string) JobWorkerBuilderStep3
	TenantIDs(tenantIDs ...string) JobWorkerBuilderStep3
	Context(ctx context.Context) JobWorkerBuilderStep3
	Backoff(supplier BackoffSupplier) JobWorkerBuilderStep3
	ErrorHandler(handler func(error)) JobWorkerBuilderStep3
	// ShutdownHandler returns queued jobs on shutdown instead of executing the handler.
	// It may run concurrently with already executing handlers and must be bounded.
	ShutdownHandler(handler JobHandler) JobWorkerBuilderStep3
	Open() (JobWorker, error)
}

type JobWorkerBuilder struct {
	gateway         pb.GatewayClient
	client          JobClient
	request         *pb.ActivateJobsRequest
	handler         JobHandler
	shutdownHandler JobHandler
	context         context.Context

	maxJobsActive  int
	concurrency    int
	pollInterval   time.Duration
	backoff        BackoffSupplier
	errorHandler   func(error)
	registry       RegistryClient
	registryConfig RegistryConfig
	workerID       string
}

func NewJobWorkerBuilder(
	gateway pb.GatewayClient,
	client JobClient,
) JobWorkerBuilderStep1 {
	return &JobWorkerBuilder{
		gateway: gateway,
		client:  client,
		request: &pb.ActivateJobsRequest{
			Worker:         commands.DefaultJobWorkerName,
			Timeout:        commands.DefaultJobTimeout.Milliseconds(),
			RequestTimeout: commands.DefaultRequestTimeout.Milliseconds(),
		},
		context:       context.Background(),
		maxJobsActive: DefaultMaxJobsActive,
		concurrency:   DefaultConcurrency,
		pollInterval:  DefaultPollInterval,
		backoff:       ExponentialBackoff{},
	}
}

func NewRegisteredJobWorkerBuilder(
	gateway pb.GatewayClient,
	registry RegistryClient,
	client JobClient,
	config RegistryConfig,
) JobWorkerBuilderStep1 {
	builder := NewJobWorkerBuilder(gateway, client).(*JobWorkerBuilder)
	builder.registry = registry
	config.Registration = config.Registration.WithDefaults()
	builder.registryConfig = config
	return builder
}

func (builder *JobWorkerBuilder) JobType(jobType string) JobWorkerBuilderStep2 {
	builder.request.Type = jobType
	return builder
}

func (builder *JobWorkerBuilder) Handler(handler JobHandler) JobWorkerBuilderStep3 {
	builder.handler = handler
	return builder
}

func (builder *JobWorkerBuilder) Name(name string) JobWorkerBuilderStep3 {
	builder.request.Worker = name
	return builder
}

func (builder *JobWorkerBuilder) Timeout(timeout time.Duration) JobWorkerBuilderStep3 {
	builder.request.Timeout = timeout.Milliseconds()
	return builder
}

func (builder *JobWorkerBuilder) RequestTimeout(
	timeout time.Duration,
) JobWorkerBuilderStep3 {
	builder.request.RequestTimeout = timeout.Milliseconds()
	return builder
}

func (builder *JobWorkerBuilder) MaxJobsActive(maxJobs int) JobWorkerBuilderStep3 {
	builder.maxJobsActive = maxJobs
	return builder
}

func (builder *JobWorkerBuilder) Concurrency(concurrency int) JobWorkerBuilderStep3 {
	builder.concurrency = concurrency
	return builder
}

func (builder *JobWorkerBuilder) PollInterval(interval time.Duration) JobWorkerBuilderStep3 {
	builder.pollInterval = interval
	return builder
}

func (builder *JobWorkerBuilder) FetchVariables(
	names ...string,
) JobWorkerBuilderStep3 {
	builder.request.FetchVariable = append([]string(nil), names...)
	return builder
}

func (builder *JobWorkerBuilder) TenantIDs(tenantIDs ...string) JobWorkerBuilderStep3 {
	builder.request.TenantIds = append([]string(nil), tenantIDs...)
	return builder
}

func (builder *JobWorkerBuilder) Context(ctx context.Context) JobWorkerBuilderStep3 {
	builder.context = ctx
	return builder
}

func (builder *JobWorkerBuilder) Backoff(
	supplier BackoffSupplier,
) JobWorkerBuilderStep3 {
	builder.backoff = supplier
	return builder
}

func (builder *JobWorkerBuilder) ErrorHandler(
	handler func(error),
) JobWorkerBuilderStep3 {
	builder.errorHandler = handler
	return builder
}

func (builder *JobWorkerBuilder) ShutdownHandler(handler JobHandler) JobWorkerBuilderStep3 {
	builder.shutdownHandler = handler
	return builder
}

func (builder *JobWorkerBuilder) Open() (JobWorker, error) {
	if err := builder.validate(); err != nil {
		return nil, err
	}
	if builder.registry != nil {
		workerID, err := newWorkerID()
		if err != nil {
			return nil, fmt.Errorf("create worker ID: %w", err)
		}
		builder.workerID = workerID
	}

	worker := newJobWorker(builder)
	if err := worker.register(); err != nil {
		worker.cancel()
		worker.heartbeatStop()
		return nil, err
	}
	worker.start()
	return worker, nil
}

func (builder *JobWorkerBuilder) validate() error {
	switch {
	case builder.gateway == nil:
		return errors.New("gateway client must be present")
	case builder.client == nil:
		return errors.New("job client must be present")
	case strings.TrimSpace(builder.request.Type) == "":
		return errors.New("job type must be present")
	case strings.TrimSpace(builder.request.Worker) == "":
		return errors.New("worker name must be present")
	case builder.handler == nil:
		return errors.New("job handler must be present")
	case builder.context == nil:
		return errors.New("worker context must be present")
	case builder.maxJobsActive <= 0:
		return fmt.Errorf("max jobs active must be positive: %d", builder.maxJobsActive)
	case builder.maxJobsActive > int(^uint32(0)>>1):
		return fmt.Errorf("max jobs active exceeds int32: %d", builder.maxJobsActive)
	case builder.concurrency <= 0:
		return fmt.Errorf("concurrency must be positive: %d", builder.concurrency)
	case builder.pollInterval < 0:
		return fmt.Errorf("poll interval must not be negative: %s", builder.pollInterval)
	case builder.request.Timeout < 0:
		return fmt.Errorf("job timeout must not be negative: %dms", builder.request.Timeout)
	case builder.backoff == nil:
		return errors.New("backoff supplier must be present")
	case builder.registry != nil && strings.TrimSpace(builder.registryConfig.ClusterID) == "":
		return errors.New("worker registry cluster ID must be present")
	case builder.registry != nil && strings.TrimSpace(builder.registryConfig.TenantID) == "":
		return errors.New("worker registry tenant ID must be present")
	default:
		if builder.registry != nil {
			if err := builder.registryConfig.Registration.Validate(); err != nil {
				return err
			}
		}
		for _, tenantID := range builder.request.TenantIds {
			if strings.TrimSpace(tenantID) == "" {
				return errors.New("tenant IDs must not contain blank values")
			}
		}
		return nil
	}
}
