package commands

import (
	"context"

	"github.com/spf13/cobra"
	taskite "github.com/taskite-io/taskite/clients/go/pkg/client"
	"google.golang.org/protobuf/proto"
)

func newCreateCommand(app *application) *cobra.Command {
	parent := &cobra.Command{Use: "create", Short: "Create jobs and subscriptions"}
	var variables string
	var retries, priority int32
	job := &cobra.Command{
		Use: "job <job-type>", Short: "Create a job", Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return app.run(command, func(ctx context.Context, client taskite.Client) (proto.Message, error) {
				return client.NewCreateJobCommand().JobType(args[0]).
					VariablesFromString(variables).
					Retries(retries).Priority(priority).Send(ctx)
			})
		},
	}
	job.Flags().StringVar(&variables, "variables", "{}", "Initial variables as JSON object")
	job.Flags().Int32Var(&retries, "retries", 3, "Initial retries")
	job.Flags().Int32Var(&priority, "priority", 50, "Job priority (nonnegative 32-bit integer)")
	parent.AddCommand(job, newCreateSubscriptionCommand(app), newCreateTimerCommand(app))
	return parent
}

func newCreateSubscriptionCommand(app *application) *cobra.Command {
	var name, variables string
	var retries, priority int32
	command := &cobra.Command{
		Use: "subscription <job-type>", Short: "Distribute a job subscription", Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return app.run(command, func(ctx context.Context, client taskite.Client) (proto.Message, error) {
				return client.NewCreateJobSubscriptionCommand().JobType(args[0]).Name(name).
					VariablesFromString(variables).Retries(retries).Priority(priority).Send(ctx)
			})
		},
	}
	command.Flags().StringVar(&name, "name", "", "Subscription name")
	command.Flags().StringVar(&variables, "variables", "{}", "Template variables as JSON object")
	command.Flags().Int32Var(&retries, "retries", 3, "Initial retries")
	command.Flags().Int32Var(&priority, "priority", 50, "Job subscription priority (1-100)")
	return command
}

func newCancelCommand(app *application) *cobra.Command {
	parent := &cobra.Command{Use: "cancel", Short: "Cancel jobs"}
	parent.AddCommand(&cobra.Command{
		Use: "job <key>", Short: "Cancel a job", Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			key, err := parseKey(args[0])
			if err != nil {
				return err
			}
			return app.run(command, func(ctx context.Context, client taskite.Client) (proto.Message, error) {
				return client.NewCancelJobCommand().JobKey(key).Send(ctx)
			})
		},
	})
	return parent
}

func newDeleteCommand(app *application) *cobra.Command {
	parent := &cobra.Command{Use: "delete", Short: "Delete job and timer subscriptions"}
	parent.AddCommand(newDeleteTimerCommand(app))
	parent.AddCommand(&cobra.Command{
		Use: "subscription <key>", Short: "Delete a job subscription", Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			key, err := parseKey(args[0])
			if err != nil {
				return err
			}
			return app.run(command, func(ctx context.Context, client taskite.Client) (proto.Message, error) {
				return client.NewDeleteJobSubscriptionCommand().Key(key).Send(ctx)
			})
		},
	})
	return parent
}
