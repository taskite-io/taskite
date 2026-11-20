package commands

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	taskite "github.com/taskite-io/taskite/clients/go/pkg/client"
	"github.com/taskite-io/taskite/clients/go/pkg/pb"
	"google.golang.org/protobuf/proto"
)

func newCreateTimerCommand(app *application) *cobra.Command {
	var name, expression, kind, variables string
	command := &cobra.Command{
		Use: "timer <job-type> [job-type...]", Short: "Create an independent timer subscription",
		Long: "Create a timer for job types whose subscriptions are fully created in the current cluster and tenant.",
		Args: cobra.MinimumNArgs(1),
		RunE: func(command *cobra.Command, types []string) error {
			var timerType pb.TimerType
			switch strings.ToLower(kind) {
			case "duration":
				timerType = pb.TimerType_DURATION
			case "cycle":
				timerType = pb.TimerType_CYCLE
			case "cron":
				timerType = pb.TimerType_CRON
			default:
				return fmt.Errorf("type must be duration, cycle or cron")
			}
			return app.run(command, func(ctx context.Context, client taskite.Client) (proto.Message, error) {
				return client.NewCreateTimerSubscriptionCommand().Name(name).
					Type(timerType).Expression(expression).JobTypes(types...).
					VariablesFromString(variables).Send(ctx)
			})
		},
	}
	command.Flags().StringVar(&name, "name", "", "Timer subscription name")
	command.Flags().StringVar(&kind, "type", "", "duration, cycle or cron")
	command.Flags().StringVar(&expression, "expression", "", "ISO8601 duration/cycle or six-field cron")
	command.Flags().StringVar(&variables, "variables", "{}", "JSON object passed to each job type")
	for _, flag := range []string{"name", "type", "expression"} {
		_ = command.MarkFlagRequired(flag)
	}
	return command
}

func newDeleteTimerCommand(app *application) *cobra.Command {
	return &cobra.Command{
		Use: "timer <key>", Short: "Delete an independent timer subscription",
		Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			key, err := parseKey(args[0])
			if err != nil {
				return err
			}
			return app.run(command, func(ctx context.Context, client taskite.Client) (proto.Message, error) {
				return client.NewDeleteTimerSubscriptionCommand().Key(key).Send(ctx)
			})
		},
	}
}
