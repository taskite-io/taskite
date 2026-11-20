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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/taskite-io/taskite/clients/go/pkg/commands"
	"github.com/taskite-io/taskite/clients/go/pkg/entities"
	"github.com/taskite-io/taskite/clients/go/pkg/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const requestTimeoutOffset = 5 * time.Second

type State int32

const (
	REGISTERING State = iota
	RUNNING
	DRAINING
	CLOSED
	FENCED
)

type JobWorker interface {
	Close(ctx context.Context) error
	Done() <-chan struct{}
	LastError() error
	ActiveJobs() int
	State() State
	WorkerID() string
	WorkerLeaseToken() int64
}

type jobWorker struct {
	gateway         pb.GatewayClient
	client          JobClient
	request         *pb.ActivateJobsRequest
	handler         JobHandler
	shutdownHandler JobHandler
	maxJobs         int
	concurrency     int
	pollInterval    time.Duration
	backoff         BackoffSupplier
	errorHandler    func(error)
	registry        RegistryClient
	registryConfig  RegistryConfig
	workerID        string

	context       context.Context
	cancel        context.CancelFunc
	heartbeatCtx  context.Context
	heartbeatStop context.CancelFunc
	jobs          chan entities.Job
	ready         chan struct{}
	done          chan struct{}
	heartbeatDone chan struct{}
	active        atomic.Int64
	state         atomic.Int32
	token         atomic.Int64
	deadline      atomic.Int64
	wait          sync.WaitGroup

	closeOnce     sync.Once
	finishOnce    sync.Once
	finishDone    chan struct{}
	shutdownError error
	errorMu       sync.RWMutex
	lastError     error
}

func newJobWorker(builder *JobWorkerBuilder) *jobWorker {
	ctx, cancel := context.WithCancel(builder.context)
	// Parent cancellation stops polling; renewals continue until queued handlers drain.
	heartbeatCtx, heartbeatStop := context.WithCancel(context.WithoutCancel(builder.context))
	return &jobWorker{
		gateway:         builder.gateway,
		client:          builder.client,
		request:         proto.Clone(builder.request).(*pb.ActivateJobsRequest),
		handler:         builder.handler,
		shutdownHandler: builder.shutdownHandler,
		maxJobs:         builder.maxJobsActive,
		concurrency:     builder.concurrency,
		pollInterval:    builder.pollInterval,
		backoff:         builder.backoff,
		errorHandler:    builder.errorHandler,
		registry:        builder.registry,
		registryConfig:  builder.registryConfig,
		workerID:        builder.workerID,
		context:         ctx,
		cancel:          cancel,
		heartbeatCtx:    heartbeatCtx,
		heartbeatStop:   heartbeatStop,
		jobs:            make(chan entities.Job, builder.maxJobsActive),
		ready:           make(chan struct{}, 1),
		done:            make(chan struct{}),
		heartbeatDone:   make(chan struct{}),
		finishDone:      make(chan struct{}),
	}
}

func (worker *jobWorker) register() error {
	if worker.registry == nil {
		return nil
	}
	worker.request.TenantIds = worker.tenantIDs()
	var retryDelay time.Duration
	backoff := registrationBackoff(worker.registryConfig)
	for worker.context.Err() == nil {
		err := worker.sendRegistration(worker.context, true)
		if err == nil {
			worker.request.WorkerId = worker.workerID
			worker.request.WorkerLeaseToken = worker.token.Load()
			return nil
		}
		if !isRetryableRegistrationError(err) {
			return err
		}
		retryDelay = backoff.Next(retryDelay)
		if !worker.waitFor(retryDelay) {
			break
		}
	}
	return worker.context.Err()
}

func (worker *jobWorker) start() {
	routines := worker.concurrency + 1
	if worker.shutdownHandler != nil {
		routines++
	}
	worker.state.Store(int32(RUNNING))
	worker.wait.Add(routines)
	for range worker.concurrency {
		go worker.handleJobs()
	}
	go worker.poll()
	if worker.shutdownHandler != nil {
		go worker.returnQueuedJobs()
	}
	if worker.registry != nil {
		go worker.heartbeatLoop()
	} else {
		close(worker.heartbeatDone)
	}
	go func() {
		worker.wait.Wait()
		worker.finishSession()
		<-worker.finishDone
		worker.state.CompareAndSwap(int32(DRAINING), int32(CLOSED))
		worker.state.CompareAndSwap(int32(RUNNING), int32(CLOSED))
		close(worker.done)
	}()
}

func (worker *jobWorker) Close(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("close context must be present")
	}
	worker.closeOnce.Do(func() {
		worker.state.CompareAndSwap(int32(RUNNING), int32(DRAINING))
		worker.cancel()
	})
	select {
	case <-worker.done:
		return worker.shutdownError
	case <-ctx.Done():
		worker.fence(ctx.Err())
		// Keep the registry connection alive until the bounded deregistration completes.
		worker.finishSession()
		if worker.shutdownError != nil {
			return errors.Join(ctx.Err(), worker.shutdownError)
		}
		return ctx.Err()
	}
}

func (worker *jobWorker) finishSession() {
	worker.finishOnce.Do(func() {
		defer close(worker.finishDone)
		worker.heartbeatStop()
		<-worker.heartbeatDone
		worker.shutdownError = worker.deregister(context.Background())
	})
}

func (worker *jobWorker) fence(err error) {
	worker.state.Store(int32(FENCED))
	worker.cancel()
	worker.heartbeatStop()
	if err != nil {
		worker.recordError(err)
	}
}

func (worker *jobWorker) heartbeatLoop() {
	defer close(worker.heartbeatDone)
	backoff := registrationBackoff(worker.registryConfig)
	var retryDelay time.Duration
	delay := worker.registryConfig.Registration.RenewInterval
	for worker.heartbeatCtx.Err() == nil {
		// Never register a replacement from a renewal loop.
		if !worker.waitHeartbeat(delay) {
			return
		}
		deadline := worker.deadline.Load()
		if deadline > 0 && time.Now().UnixMilli() >= deadline {
			worker.fence(status.Error(codes.FailedPrecondition, "WORKER_LEASE_EXPIRED"))
			return
		}
		err := worker.sendRegistration(worker.heartbeatCtx, false)
		if err == nil {
			retryDelay = 0
			delay = worker.registryConfig.Registration.RenewInterval
			continue
		}
		if worker.heartbeatCtx.Err() != nil {
			return
		}
		if !isRetryableRegistrationError(err) {
			worker.fence(err)
			return
		}
		retryDelay = backoff.Next(retryDelay)
		delay = retryDelay
	}
}

func (worker *jobWorker) waitHeartbeat(delay time.Duration) bool {
	// Do not let retries hide an elapsed authoritative lease deadline.
	if deadline := worker.deadline.Load(); deadline > 0 {
		delay = min(delay, max(time.Millisecond, time.Until(time.UnixMilli(deadline))))
	}
	timer := time.NewTimer(max(time.Microsecond, delay))
	defer timer.Stop()
	select {
	case <-worker.heartbeatCtx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func isRetryableRegistrationError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

func isRegistrationBackpressure(err error) bool {
	return status.Code(err) == codes.ResourceExhausted &&
		strings.HasPrefix(status.Convert(err).Message(), "BACKPRESSURE:")
}

func (worker *jobWorker) sendRegistration(parent context.Context, register bool) error {
	timeout := worker.registryConfig.Registration.RenewalTimeout
	if !register && worker.deadline.Load() > 0 {
		timeout = min(timeout, max(time.Millisecond, time.Until(time.UnixMilli(worker.deadline.Load()))))
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	request := RegistrationRequest{
		WorkerLeaseToken:     worker.token.Load(),
		ClusterID:            worker.registryConfig.ClusterID,
		TenantIDs:            worker.tenantIDs(),
		WorkerID:             worker.workerID,
		Name:                 worker.request.GetWorker(),
		JobType:              worker.request.GetType(),
		MaxJobsActive:        worker.maxJobs,
		ActiveJobs:           worker.ActiveJobs(),
		Concurrency:          worker.concurrency,
		TimeoutMillis:        worker.request.GetTimeout(),
		RequestTimeoutMillis: worker.request.GetRequestTimeout(),
		PollIntervalMillis:   worker.pollInterval.Milliseconds(),
		FetchVariables:       append([]string(nil), worker.request.GetFetchVariable()...),
		ClientVersion:        worker.registryConfig.ClientVersion,
		RenewalTimeoutMillis: worker.registryConfig.Registration.RenewalTimeout.Milliseconds(),
	}
	var err error
	var result RegistrationResult
	if register {
		result, err = worker.registry.Register(ctx, request)
	} else {
		result, err = worker.registry.Renew(ctx, request)
	}
	if err == nil {
		if result.WorkerLeaseToken <= 0 ||
			result.LeaseDeadlineEpochMillis <= time.Now().UnixMilli() ||
			(!register && result.WorkerLeaseToken != worker.token.Load()) {
			err = status.Error(codes.FailedPrecondition, "INVALID_WORKER_SESSION")
		} else {
			worker.token.Store(result.WorkerLeaseToken)
			worker.deadline.Store(result.LeaseDeadlineEpochMillis)
		}
	}
	if err != nil && parent.Err() == nil {
		worker.recordError(fmt.Errorf("worker registration renewal: %w", err))
	}
	return err
}

func (worker *jobWorker) deregister(parent context.Context) error {
	if worker.registry == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(parent, worker.registryConfig.Registration.RenewalTimeout)
	defer cancel()
	err := worker.registry.Deregister(ctx, DeregisterRequest{
		WorkerLeaseToken: worker.token.Load(),
		ClusterID:        worker.registryConfig.ClusterID,
		TenantIDs:        worker.tenantIDs(),
		WorkerID:         worker.workerID,
		Name:             worker.request.GetWorker(),
	})
	if err != nil && parent.Err() == nil {
		worker.recordError(fmt.Errorf("deregister worker: %w", err))
	}
	return err
}

func (worker *jobWorker) tenantIDs() []string {
	if len(worker.request.GetTenantIds()) > 0 {
		return append([]string(nil), worker.request.GetTenantIds()...)
	}
	return []string{worker.registryConfig.TenantID}
}

func (worker *jobWorker) Done() <-chan struct{} {
	return worker.done
}

func (worker *jobWorker) LastError() error {
	worker.errorMu.RLock()
	defer worker.errorMu.RUnlock()
	return worker.lastError
}

func (worker *jobWorker) ActiveJobs() int {
	return int(worker.active.Load())
}

func (worker *jobWorker) State() State            { return State(worker.state.Load()) }
func (worker *jobWorker) WorkerID() string        { return worker.workerID }
func (worker *jobWorker) WorkerLeaseToken() int64 { return worker.token.Load() }

func (worker *jobWorker) poll() {
	defer worker.wait.Done()
	defer close(worker.jobs)
	defer func() {
		worker.state.CompareAndSwap(int32(RUNNING), int32(DRAINING))
		worker.cancel()
	}()

	var retryDelay time.Duration
	for worker.context.Err() == nil {
		if deadline := worker.deadline.Load(); deadline > 0 && time.Now().UnixMilli() >= deadline {
			worker.fence(status.Error(codes.FailedPrecondition, "WORKER_LEASE_EXPIRED"))
			return
		}
		available := worker.maxJobs - worker.ActiveJobs()
		if available <= 0 {
			if !worker.waitForCapacity() {
				return
			}
			continue
		}

		activated, err := worker.activate(available)
		if err != nil {
			if worker.context.Err() != nil {
				return
			}
			worker.recordError(err)
			if !isRetryableActivationError(err) {
				if status.Code(err) == codes.FailedPrecondition {
					worker.fence(err)
				}
				return
			}
			retryDelay = worker.backoff.Next(retryDelay)
			if !worker.waitFor(retryDelay) {
				return
			}
			continue
		}

		retryDelay = 0
		if activated == 0 && !worker.waitFor(worker.pollInterval) {
			return
		}
	}
}

func (worker *jobWorker) activate(maxJobs int) (int, error) {
	request := proto.Clone(worker.request).(*pb.ActivateJobsRequest)
	request.MaxJobsToActivate = int32(maxJobs)

	timeout := time.Duration(request.GetRequestTimeout())*time.Millisecond + requestTimeoutOffset
	if timeout < requestTimeoutOffset {
		timeout = requestTimeoutOffset
	}
	ctx, cancel := context.WithTimeout(worker.context, timeout)
	defer cancel()

	if len(request.TenantIds) > 0 {
		ctx = commands.WithTenantID(ctx, request.TenantIds[0])
	}
	stream, err := worker.gateway.ActivateJobs(ctx, request)
	if err != nil {
		if request.GetRequestTimeout() >= 0 &&
			isLongPollDeadline(err, worker.context) {
			return 0, nil
		}
		return 0, err
	}

	activated := 0
	for {
		response, receiveErr := stream.Recv()
		if receiveErr == io.EOF {
			return activated, nil
		}
		if receiveErr != nil {
			if request.GetRequestTimeout() >= 0 &&
				isLongPollDeadline(receiveErr, worker.context) {
				return activated, nil
			}
			return activated, receiveErr
		}
		for _, activatedJob := range response.GetJobs() {
			if activatedJob == nil {
				continue
			}
			worker.active.Add(1)
			select {
			case worker.jobs <- entities.Job{ActivatedJob: activatedJob}:
			case <-worker.context.Done():
				if worker.shutdownHandler != nil && worker.State() != FENCED {
					worker.invoke(worker.shutdownHandler, entities.Job{ActivatedJob: activatedJob})
				}
				worker.active.Add(-1)
				// Process the remaining jobs in this response as well, so none are lost.
				if worker.shutdownHandler == nil {
					return activated, worker.context.Err()
				}
			}
			activated++
		}
	}
}

func isLongPollDeadline(err error, workerContext context.Context) bool {
	return workerContext.Err() == nil &&
		(errors.Is(err, context.DeadlineExceeded) ||
			status.Code(err) == codes.DeadlineExceeded)
}

func (worker *jobWorker) waitForCapacity() bool {
	select {
	case <-worker.context.Done():
		return false
	case <-worker.ready:
		return true
	}
}

func (worker *jobWorker) waitFor(delay time.Duration) bool {
	if delay <= 0 {
		return worker.context.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-worker.context.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (worker *jobWorker) handleJobs() {
	defer worker.wait.Done()
	for job := range worker.jobs {
		if worker.State() != FENCED {
			handler := worker.handler
			if worker.context.Err() != nil && worker.shutdownHandler != nil {
				handler = worker.shutdownHandler
			}
			worker.invoke(handler, job)
		}
		worker.releaseJob()
	}
}

func (worker *jobWorker) returnQueuedJobs() {
	defer worker.wait.Done()
	<-worker.context.Done()
	for job := range worker.jobs {
		if worker.State() != FENCED {
			worker.invoke(worker.shutdownHandler, job)
		}
		worker.releaseJob()
	}
}

func (worker *jobWorker) releaseJob() {
	worker.active.Add(-1)
	select {
	case worker.ready <- struct{}{}:
	default:
	}
}

func (worker *jobWorker) handle(job entities.Job) {
	worker.invoke(worker.handler, job)
}

func (worker *jobWorker) invoke(handler JobHandler, job entities.Job) {
	defer func() {
		if recovered := recover(); recovered != nil {
			worker.recordError(fmt.Errorf("job handler panicked: %v", recovered))
		}
	}()
	handler(boundJobClient{delegate: worker.client, job: job}, job)
}

type boundJobClient struct {
	delegate JobClient
	job      entities.Job
}

func (client boundJobClient) NewCompleteJobCommand() commands.CompleteJobCommandStep1 {
	return boundCompleteCommand{client.delegate.NewCompleteJobCommand(), client.job}
}

func (client boundJobClient) NewFailJobCommand() commands.FailJobCommandStep1 {
	return boundFailCommand{client.delegate.NewFailJobCommand(), client.job}
}

type boundCompleteCommand struct {
	commands.CompleteJobCommandStep1
	job entities.Job
}

func (command boundCompleteCommand) JobKey(key int64) commands.CompleteJobCommandStep2 {
	if key == command.job.GetKey() {
		return command.CompleteJobCommandStep1.Job(command.job)
	}
	return command.CompleteJobCommandStep1.JobKey(key)
}

type boundFailCommand struct {
	commands.FailJobCommandStep1
	job entities.Job
}

func (command boundFailCommand) JobKey(key int64) commands.FailJobCommandStep2 {
	if key == command.job.GetKey() {
		return command.FailJobCommandStep1.Job(command.job)
	}
	return command.FailJobCommandStep1.JobKey(key)
}

func (worker *jobWorker) recordError(err error) {
	worker.errorMu.Lock()
	worker.lastError = err
	worker.errorMu.Unlock()
	if worker.errorHandler != nil {
		worker.notifyError(err)
	}
}

func (worker *jobWorker) notifyError(err error) {
	defer func() {
		_ = recover()
	}()
	worker.errorHandler(err)
}

func isRetryableActivationError(err error) bool {
	switch status.Code(err) {
	case codes.Aborted,
		codes.Canceled,
		codes.DeadlineExceeded,
		codes.ResourceExhausted,
		codes.Unavailable:
		return true
	default:
		return false
	}
}

func newWorkerID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}
