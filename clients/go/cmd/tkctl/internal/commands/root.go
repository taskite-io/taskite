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
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	taskite "github.com/taskite-io/taskite/clients/go/pkg/client"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const defaultRequestTimeout = 30 * time.Second
const requestTimeoutEnvironmentVariable = "TASKITE_CLI_REQUEST_TIMEOUT"

type application struct {
	address        string
	clusterID      string
	requestTimeout time.Duration
	output         string
}

func NewRootCommand() *cobra.Command {
	app := &application{}
	root := &cobra.Command{
		Use:           "tkctl",
		Short:         "Taskite command line interface",
		SilenceErrors: true,
		SilenceUsage:  true,
		Version:       taskite.Version,
		PersistentPreRunE: func(command *cobra.Command, _ []string) error {
			if !command.Flags().Changed("request-timeout") {
				if raw := os.Getenv(requestTimeoutEnvironmentVariable); raw != "" {
					duration, err := time.ParseDuration(raw)
					if err != nil {
						return fmt.Errorf("%s must be a duration: %w", requestTimeoutEnvironmentVariable, err)
					}
					app.requestTimeout = duration
				}
			}
			return app.validate()
		},
	}

	flags := root.PersistentFlags()
	flags.StringVar(&app.address, "address", "", "Gateway host:port or [IPv6]:port; host defaults to port 26500 (env TASKITE_GATEWAY_ADDRESS)")
	flags.StringVar(&app.clusterID, "cluster", "", "Cluster ID (env TASKITE_CLUSTER_ID)")
	flags.DurationVar(
		&app.requestTimeout,
		"request-timeout",
		defaultRequestTimeout,
		"Timeout for each Gateway or handler operation (env TASKITE_CLI_REQUEST_TIMEOUT)",
	)
	flags.StringVar(&app.output, "output", "json", "Output format (json)")

	root.AddCommand(
		newDeleteCommand(app),
		newCreateCommand(app),
		newActivateCommand(app),
		newCompleteCommand(app),
		newFailCommand(app),
		newQueryCommand(app),
		newCancelCommand(app),
		newResolveCommand(app),
		newSetCommand(app),
		newUpdateCommand(app),
	)
	return root
}

func Execute() error {
	return NewRootCommand().Execute()
}

func (app *application) run(
	command *cobra.Command,
	action func(context.Context, taskite.Client) (proto.Message, error),
) error {
	if err := app.validate(); err != nil {
		return err
	}
	client, err := app.newClient()
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(command.Context(), app.requestTimeout)
	defer cancel()
	response, err := action(ctx, client)
	if err != nil {
		return err
	}
	return printJSON(response)
}

func (app *application) validate() error {
	if app.output != "json" {
		return fmt.Errorf("unsupported output format %q", app.output)
	}
	if app.requestTimeout <= 0 {
		return errors.New("request timeout must be positive")
	}
	return nil
}

func (app *application) newClient() (taskite.Client, error) {
	return taskite.NewClient(taskite.ClientConfig{
		Gateway:   taskite.GatewayConfig{Address: app.address},
		ClusterID: app.clusterID,
		UserAgent: "tkctl/" + taskite.Version,
	})
}

func printJSON(message proto.Message) error {
	encoded, err := (protojson.MarshalOptions{
		EmitUnpopulated: true,
		Indent:          "  ",
	}).Marshal(message)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(os.Stdout, string(encoded))
	return err
}

func parseKey(value string) (int64, error) {
	key, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid key %q: %w", value, err)
	}
	if key <= 0 {
		return 0, fmt.Errorf("key must be positive: %d", key)
	}
	return key, nil
}
