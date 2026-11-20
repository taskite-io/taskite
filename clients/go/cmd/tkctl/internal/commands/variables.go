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

	"github.com/spf13/cobra"
	taskite "github.com/taskite-io/taskite/clients/go/pkg/client"
	"google.golang.org/protobuf/proto"
)

func newSetCommand(app *application) *cobra.Command {
	command := &cobra.Command{
		Use:   "set",
		Short: "Update runtime state",
	}
	command.AddCommand(newSetVariablesCommand(app))
	return command
}

func newSetVariablesCommand(app *application) *cobra.Command {
	var variables string
	command := &cobra.Command{
		Use:   "variables <job-key>",
		Short: "Set job variables",
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
						NewSetVariablesCommand().
						JobKey(key).
						VariablesFromString(variables).
						Send(ctx)
				},
			)
		},
	}
	command.Flags().StringVar(&variables, "variables", "{}", "Variables as JSON object")
	return command
}
