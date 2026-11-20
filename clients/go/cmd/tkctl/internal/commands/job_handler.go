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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/taskite-io/taskite/clients/go/pkg/entities"
	"github.com/taskite-io/taskite/clients/go/pkg/worker"
)

func (app *application) runJobHandler(
	command *cobra.Command,
	jobType string,
	handler string,
	workerName string,
	timeout time.Duration,
	longPollTimeout time.Duration,
	maxJobs int,
	concurrency int,
	pollInterval time.Duration,
	fetchVariables []string,
	tenantIDs []string,
	shutdownTimeout time.Duration,
	retryBackoff time.Duration,
) error {
	if err := app.validate(); err != nil {
		return err
	}
	arguments, err := parseHandler(handler)
	if err != nil {
		return err
	}
	client, err := app.newClient()
	if err != nil {
		return err
	}
	defer client.Close()

	workerContext, stop := signal.NotifyContext(
		command.Context(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// Signals stop polling immediately; executing handlers retain their grace period.
	handlerContext, cancelHandlers := context.WithCancel(context.Background())
	defer cancelHandlers()
	outputLogger := log.New(command.OutOrStdout(), "", log.LstdFlags)
	errorLogger := log.New(command.ErrOrStderr(), "", log.LstdFlags)
	jobWorker, err := client.
		NewJobWorker().
		JobType(jobType).
		Handler(func(jobClient worker.JobClient, job entities.Job) {
			outputLogger.Printf(
				"activated job %d with variables %s",
				job.GetKey(),
				job.GetVariables(),
			)
			jobContext, cancel := context.WithTimeout(
				handlerContext,
				app.requestTimeout,
			)
			defer cancel()
			if handlerErr := handleActivatedJob(
				jobContext,
				jobClient,
				arguments,
				job,
			); handlerErr != nil {
				errorLogger.Printf(
					"handle job %d: %v",
					job.GetKey(),
					handlerErr,
				)
				return
			}
			outputLogger.Printf("handled job %d", job.GetKey())
		}).
		ShutdownHandler(func(jobClient worker.JobClient, job entities.Job) {
			ctx, cancel := context.WithTimeout(handlerContext, app.requestTimeout)
			defer cancel()
			if err := returnQueuedJob(ctx, jobClient, job, retryBackoff); err != nil {
				errorLogger.Printf("return queued job %d: %v; pending PD session yield", job.GetKey(), err)
				return
			}
			outputLogger.Printf("returned queued job %d with retries %d and backoff %s",
				job.GetKey(), job.GetRetries(), retryBackoff)
		}).
		Name(workerName).
		Timeout(timeout).
		RequestTimeout(longPollTimeout).
		MaxJobsActive(maxJobs).
		Concurrency(concurrency).
		PollInterval(pollInterval).
		FetchVariables(fetchVariables...).
		TenantIDs(tenantIDs...).
		Context(workerContext).
		ErrorHandler(func(workerErr error) {
			errorLogger.Printf("activate jobs: %v", workerErr)
		}).
		Open()
	if err != nil {
		if workerContext.Err() != nil {
			return nil
		}
		return err
	}
	outputLogger.Printf(
		"worker started for job type %q with handler %q",
		jobType,
		handler,
	)

	select {
	case <-jobWorker.Done():
	case <-workerContext.Done():
		outputLogger.Printf("worker stopping: returning queued jobs; running handlers have %s to finish", shutdownTimeout)
	}
	shutdownContext, cancelShutdown := context.WithTimeout(
		context.Background(),
		shutdownTimeout,
	)
	defer cancelShutdown()
	stopCancelHandlers := context.AfterFunc(shutdownContext, cancelHandlers)
	defer stopCancelHandlers()
	closeErr := jobWorker.Close(shutdownContext)
	if shutdownContext.Err() != nil && workerContext.Err() != nil {
		cancelHandlers()
		// Let exec reap canceled processes and close their pipes before the CLI exits.
		cleanupTimer := time.NewTimer(time.Second)
		defer cleanupTimer.Stop()
		select {
		case <-jobWorker.Done():
		case <-cleanupTimer.C:
		}
		errorLogger.Printf("worker grace period expired; PD session deregistration attempted; unfinished jobs pending yield")
		if closeErr != nil && closeErr != shutdownContext.Err() {
			errorLogger.Printf("PD deregistration failed; session yield awaits lease expiry: %v", closeErr)
			return closeErr
		}
		return nil
	}
	if closeErr != nil {
		errorLogger.Printf("worker deregistration failed; PD lease expiry will trigger session yield: %v", closeErr)
		return closeErr
	}
	outputLogger.Printf("worker session deregistered")
	if workerContext.Err() != nil {
		return nil
	}
	if workerErr := jobWorker.LastError(); workerErr != nil {
		return workerErr
	}
	return nil
}

func parseHandler(handler string) ([]string, error) {
	arguments := strings.Fields(handler)
	if len(arguments) == 0 {
		return nil, errors.New("handler command must be present")
	}
	return arguments, nil
}

func handleActivatedJob(
	ctx context.Context,
	client worker.JobClient,
	arguments []string,
	job entities.Job,
) error {
	if len(arguments) == 0 {
		return errors.New("handler command must be present")
	}
	if ctx == nil {
		return errors.New("handler context must be present")
	}
	if client == nil {
		return errors.New("handler client must be present")
	}
	if job.ActivatedJob == nil {
		return errors.New("activated job must be present")
	}

	command := exec.CommandContext(ctx, arguments[0], arguments[1:]...)
	configureHandlerProcess(command)
	command.Stdin = strings.NewReader(job.GetVariables())

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr

	if err := command.Run(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = err.Error()
		}
		return failHandledJob(ctx, client, job, message)
	}

	variables := strings.TrimSpace(stdout.String())
	if variables == "" {
		variables = "{}"
	}
	if err := validateHandlerVariables(variables); err != nil {
		return failHandledJob(
			ctx,
			client,
			job,
			fmt.Sprintf("handler returned invalid variables: %v", err),
		)
	}

	if _, err := client.
		NewCompleteJobCommand().
		Job(job).
		VariablesFromString(variables).
		Send(ctx); err != nil {
		return fmt.Errorf("complete job %d: %w", job.GetKey(), err)
	}
	return nil
}

func returnQueuedJob(ctx context.Context, client worker.JobClient, job entities.Job, backoff time.Duration) error {
	_, err := client.NewFailJobCommand().Job(job).
		Retries(job.GetRetries()).
		RetryBackoff(backoff).
		ErrorMessage("worker shutting down before handler started").
		Send(ctx)
	return err
}

func failHandledJob(
	ctx context.Context,
	client worker.JobClient,
	job entities.Job,
	message string,
) error {
	retries := job.GetRetries() - 1
	if retries < 0 {
		retries = 0
	}
	if _, err := client.
		NewFailJobCommand().
		Job(job).
		Retries(retries).
		ErrorMessage(message).
		Send(ctx); err != nil {
		return fmt.Errorf("fail job %d: %w", job.GetKey(), err)
	}
	return nil
}

func validateHandlerVariables(value string) error {
	var variables map[string]json.RawMessage
	if err := json.Unmarshal([]byte(value), &variables); err != nil {
		return err
	}
	if variables == nil {
		return errors.New("output must be a JSON object")
	}
	return nil
}
