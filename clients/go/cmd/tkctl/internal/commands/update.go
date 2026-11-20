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
	"fmt"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	taskite "github.com/taskite-io/taskite/clients/go/pkg/client"
	"google.golang.org/protobuf/proto"
)

func newUpdateCommand(app *application) *cobra.Command {
	parent := &cobra.Command{Use: "update", Short: "Update job retries, priority or activation timeout"}
	for _, field := range []string{"retries", "priority", "timeout"} {
		var workerID string
		var token int64
		command := &cobra.Command{
			Use: field + " <job-key> <value>", Args: cobra.ExactArgs(2),
			RunE: func(command *cobra.Command, args []string) error {
				key, err := parseKey(args[0])
				if err != nil {
					return err
				}
				var value int64
				if field == "timeout" {
					duration, err := time.ParseDuration(args[1])
					if err != nil || duration.Milliseconds() <= 0 {
						return fmt.Errorf("timeout must be a positive duration")
					}
					value = duration.Milliseconds()
				} else {
					value, err = strconv.ParseInt(args[1], 10, 32)
					if err != nil || field == "retries" && value < 0 {
						return fmt.Errorf("invalid %s value %q", field, args[1])
					}
				}
				return app.run(command, func(ctx context.Context, client taskite.Client) (proto.Message, error) {
					switch field {
					case "retries":
						return client.NewUpdateJobRetriesCommand().JobKey(key).Retries(int32(value)).Send(ctx)
					case "priority":
						return client.NewUpdateJobPriorityCommand().JobKey(key).Priority(int32(value)).Send(ctx)
					default:
						return client.NewUpdateJobTimeoutCommand().JobKey(key).TimeoutMillis(value).WorkerSession(workerID, token).Send(ctx)
					}
				})
			},
		}
		if field == "timeout" {
			command.Flags().StringVar(&workerID, "worker-id", "", "Owning worker instance ID")
			command.Flags().Int64Var(&token, "worker-lease-token", 0, "Owning worker lease token")
		}
		parent.AddCommand(command)
	}
	return parent
}
