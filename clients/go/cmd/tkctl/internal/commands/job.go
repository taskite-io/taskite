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
	"errors"
	"time"

	"github.com/spf13/cobra"
	taskite "github.com/taskite-io/taskite/clients/go/pkg/client"
	clientcommands "github.com/taskite-io/taskite/clients/go/pkg/commands"
	"github.com/taskite-io/taskite/clients/go/pkg/pb"
	clientworker "github.com/taskite-io/taskite/clients/go/pkg/worker"
	"google.golang.org/protobuf/proto"
)

func newActivateCommand(app *application) *cobra.Command {
	command := &cobra.Command{
		Use:   "activate",
		Short: "Activate jobs",
	}
	command.AddCommand(newActivateJobsCommand(app))
	return command
}

func newActivateJobsCommand(app *application) *cobra.Command {
	var workerName string
	var workerID string
	var workerToken int64
	var timeout time.Duration
	var maxJobs int32
	var fetchVariables []string
	var longPollTimeout time.Duration
	var tenantIDs []string
	var handler string
	var concurrency int
	var pollInterval time.Duration
	var shutdownTimeout time.Duration
	var retryBackoff time.Duration

	command := &cobra.Command{
		Use:   "jobs <type>",
		Short: "Activate jobs by type",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			if timeout < 0 {
				return errors.New("job timeout must not be negative")
			}
			if maxJobs < 0 {
				return errors.New("max jobs must not be negative")
			}
			if handler != "" {
				if shutdownTimeout <= 0 || retryBackoff < 0 {
					return errors.New("shutdown timeout must be positive and retry backoff must not be negative")
				}
				if maxJobs == 0 {
					return errors.New("max jobs must be positive when a handler is configured")
				}
				return app.runJobHandler(
					command,
					args[0],
					handler,
					workerName,
					timeout,
					longPollTimeout,
					int(maxJobs),
					concurrency,
					pollInterval,
					fetchVariables,
					tenantIDs,
					shutdownTimeout,
					retryBackoff,
				)
			}
			if longPollTimeout >= app.requestTimeout {
				return errors.New(
					"request timeout must be greater than the long-poll timeout",
				)
			}
			return app.run(
				command,
				func(ctx context.Context, client taskite.Client) (proto.Message, error) {
					jobs, err := client.
						NewActivateJobsCommand().
						JobType(args[0]).
						WorkerName(workerName).
						WorkerSession(workerID, workerToken).
						Timeout(timeout).
						MaxJobsToActivate(maxJobs).
						FetchVariables(fetchVariables...).
						RequestTimeout(longPollTimeout).
						TenantIDs(tenantIDs...).
						Send(ctx)
					response := &pb.ActivateJobsResponse{
						Jobs: make([]*pb.ActivatedJob, 0, len(jobs)),
					}
					for _, job := range jobs {
						response.Jobs = append(response.Jobs, job.ActivatedJob)
					}
					return response, err
				},
			)
		},
	}
	flags := command.Flags()
	flags.StringVar(&workerID, "worker-id", "", "Registered Worker ID for one-shot activation")
	flags.Int64Var(&workerToken, "worker-lease-token", 0, "Registered worker session token")
	flags.StringVar(
		&workerName,
		"worker",
		clientcommands.DefaultJobWorkerName,
		"Worker name",
	)
	flags.DurationVar(
		&timeout,
		"timeout",
		clientcommands.DefaultJobTimeout,
		"Job activation lease",
	)
	flags.Int32Var(
		&maxJobs,
		"max-jobs",
		clientcommands.DefaultJobsToActivate,
		"Maximum jobs to activate",
	)
	flags.StringSliceVar(&fetchVariables, "fetch-variable", nil, "Variables to fetch")
	flags.DurationVar(
		&longPollTimeout,
		"long-poll-timeout",
		clientcommands.DefaultRequestTimeout,
		"Gateway long-poll duration",
	)
	flags.StringSliceVar(&tenantIDs, "tenant", nil, "Authorized tenant IDs to scan")
	flags.StringVar(
		&handler,
		"handler",
		"",
		"Run continuously and invoke this command for each job",
	)
	flags.IntVar(
		&concurrency,
		"concurrency",
		clientworker.DefaultConcurrency,
		"Maximum concurrent handlers",
	)
	flags.DurationVar(
		&pollInterval,
		"poll-interval",
		clientworker.DefaultPollInterval,
		"Delay before polling again after an empty response",
	)
	flags.DurationVar(&shutdownTimeout, "shutdown-timeout", 30*time.Second,
		"Grace period for running handlers after Ctrl+C or SIGTERM")
	flags.DurationVar(&retryBackoff, "retry-backoff", time.Second,
		"Delay before queued jobs returned on shutdown become available again")
	return command
}

func newCompleteCommand(app *application) *cobra.Command {
	command := &cobra.Command{
		Use:   "complete",
		Short: "Complete jobs",
	}
	command.AddCommand(newCompleteJobCommand(app))
	return command
}

func newCompleteJobCommand(app *application) *cobra.Command {
	var variables string
	var workerID string
	var workerToken int64
	var tenantID string
	command := &cobra.Command{
		Use:   "job <key>",
		Short: "Complete an activated job",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			key, err := parseKey(args[0])
			if err != nil {
				return err
			}
			return app.run(
				command,
				func(ctx context.Context, client taskite.Client) (proto.Message, error) {
					return client.
						NewCompleteJobCommand().
						JobKey(key).
						WorkerSession(workerID, workerToken).
						TenantID(tenantID).
						VariablesFromString(variables).
						Send(ctx)
				},
			)
		},
	}
	command.Flags().StringVar(&variables, "variables", "{}", "Variables as JSON object")
	command.Flags().StringVar(&workerID, "worker-id", "", "Worker ID returned with the activated job")
	command.Flags().Int64Var(&workerToken, "worker-lease-token", 0, "Session token returned with the activated job")
	command.Flags().StringVar(&tenantID, "tenant", "", "Tenant returned with the activated job")
	return command
}

func newFailCommand(app *application) *cobra.Command {
	command := &cobra.Command{
		Use:   "fail",
		Short: "Fail jobs",
	}
	command.AddCommand(newFailJobCommand(app))
	return command
}

func newFailJobCommand(app *application) *cobra.Command {
	var retries int32
	var message string
	var retryBackoff time.Duration
	var variables string
	var workerID string
	var workerToken int64
	var tenantID string

	command := &cobra.Command{
		Use:   "job <key>",
		Short: "Fail an activated job",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			key, err := parseKey(args[0])
			if err != nil {
				return err
			}
			return app.run(
				command,
				func(ctx context.Context, client taskite.Client) (proto.Message, error) {
					return client.
						NewFailJobCommand().
						JobKey(key).
						WorkerSession(workerID, workerToken).
						TenantID(tenantID).
						Retries(retries).
						ErrorMessage(message).
						RetryBackoff(retryBackoff).
						VariablesFromString(variables).
						Send(ctx)
				},
			)
		},
	}
	flags := command.Flags()
	flags.StringVar(&workerID, "worker-id", "", "Worker ID returned with the activated job")
	flags.Int64Var(&workerToken, "worker-lease-token", 0, "Session token returned with the activated job")
	flags.StringVar(&tenantID, "tenant", "", "Tenant returned with the activated job")
	flags.Int32Var(&retries, "retries", 0, "Remaining retries")
	flags.StringVar(&message, "error-message", "", "Failure description")
	flags.DurationVar(&retryBackoff, "retry-backoff", 0, "Delay before jobs become available again")
	flags.StringVar(&variables, "variables", "{}", "Variables as JSON object")
	return command
}

func newQueryJobCommand(app *application) *cobra.Command {
	return &cobra.Command{
		Use:   "job <key>",
		Short: "Query current job details",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			key, err := parseKey(args[0])
			if err != nil {
				return err
			}
			return app.run(
				command,
				func(ctx context.Context, client taskite.Client) (proto.Message, error) {
					return client.NewQueryJobCommand().JobKey(key).Send(ctx)
				},
			)
		},
	}
}
