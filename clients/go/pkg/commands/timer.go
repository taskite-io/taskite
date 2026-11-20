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
	"strings"

	"github.com/taskite-io/taskite/clients/go/pkg/pb"
)

type CreateTimerSubscriptionCommandStep1 interface {
	Name(string) CreateTimerSubscriptionCommandStep2
}

type CreateTimerSubscriptionCommandStep2 interface {
	Type(pb.TimerType) CreateTimerSubscriptionCommandStep2
	// Expression accepts a literal ISO8601 duration/cycle or six-field cron schedule.
	// An invalid schedule returns INVALID_ARGUMENT from Send.
	Expression(string) CreateTimerSubscriptionCommandStep2
	// JobTypes requires fully created subscriptions in the current cluster and tenant.
	// A missing, pending or deleted subscription returns NOT_FOUND from Send.
	JobTypes(...string) CreateTimerSubscriptionCommandStep2
	Variables(any) CreateTimerSubscriptionCommandStep2
	VariablesFromString(string) CreateTimerSubscriptionCommandStep2
	Send(context.Context) (*pb.CreateTimerSubscriptionResponse, error)
}

type CreateTimerSubscriptionCommand struct {
	gateway   pb.GatewayClient
	request   pb.CreateTimerSubscriptionRequest
	variables variablesState
}

func NewCreateTimerSubscriptionCommand(gateway pb.GatewayClient) CreateTimerSubscriptionCommandStep1 {
	return &CreateTimerSubscriptionCommand{gateway: gateway, variables: defaultVariables()}
}

func (command *CreateTimerSubscriptionCommand) Name(value string) CreateTimerSubscriptionCommandStep2 {
	command.request.Name = value
	return command
}

func (command *CreateTimerSubscriptionCommand) Type(value pb.TimerType) CreateTimerSubscriptionCommandStep2 {
	command.request.Type = value
	return command
}

func (command *CreateTimerSubscriptionCommand) Expression(value string) CreateTimerSubscriptionCommandStep2 {
	command.request.Expression = value
	return command
}

func (command *CreateTimerSubscriptionCommand) JobTypes(values ...string) CreateTimerSubscriptionCommandStep2 {
	command.request.JobTypes = append([]string(nil), values...)
	return command
}

func (command *CreateTimerSubscriptionCommand) Variables(value any) CreateTimerSubscriptionCommandStep2 {
	if value == nil {
		command.variables.err = errVariablesMustBeObject
	} else {
		command.variables.fromValue(value)
	}
	return command
}

func (command *CreateTimerSubscriptionCommand) VariablesFromString(value string) CreateTimerSubscriptionCommandStep2 {
	if value != "" && strings.TrimSpace(value) == "" {
		command.variables.err = errVariablesMustBeObject
	} else {
		command.variables.fromString(value)
	}
	return command
}

func (command *CreateTimerSubscriptionCommand) Send(ctx context.Context) (*pb.CreateTimerSubscriptionResponse, error) {
	if command.variables.err != nil {
		return nil, command.variables.err
	}
	command.request.Variables = command.variables.value
	return command.gateway.CreateTimerSubscription(ctx, &command.request)
}

type DeleteTimerSubscriptionCommandStep1 interface {
	Key(int64) DeleteTimerSubscriptionCommandStep2
}

type DeleteTimerSubscriptionCommandStep2 interface {
	Send(context.Context) (*pb.DeleteTimerSubscriptionResponse, error)
}

type DeleteTimerSubscriptionCommand struct {
	gateway pb.GatewayClient
	request pb.DeleteTimerSubscriptionRequest
}

func NewDeleteTimerSubscriptionCommand(gateway pb.GatewayClient) DeleteTimerSubscriptionCommandStep1 {
	return &DeleteTimerSubscriptionCommand{gateway: gateway}
}

func (command *DeleteTimerSubscriptionCommand) Key(value int64) DeleteTimerSubscriptionCommandStep2 {
	command.request.Key = value
	return command
}

func (command *DeleteTimerSubscriptionCommand) Send(ctx context.Context) (*pb.DeleteTimerSubscriptionResponse, error) {
	return command.gateway.DeleteTimerSubscription(ctx, &command.request)
}
