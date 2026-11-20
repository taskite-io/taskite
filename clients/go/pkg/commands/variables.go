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

	"github.com/taskite-io/taskite/clients/go/pkg/pb"
)

type SetVariablesCommandStep1 interface {
	JobKey(key int64) SetVariablesCommandStep2
}

type SetVariablesCommandStep2 interface {
	Variables(value any) SetVariablesCommandStep2
	VariablesFromString(value string) SetVariablesCommandStep2
	Send(context.Context) (*pb.SetVariablesResponse, error)
}

type SetVariablesCommand struct {
	gateway   pb.GatewayClient
	request   pb.SetVariablesRequest
	variables variablesState
}

func NewSetVariablesCommand(gateway pb.GatewayClient) SetVariablesCommandStep1 {
	return &SetVariablesCommand{
		gateway:   gateway,
		variables: defaultVariables(),
	}
}

func (command *SetVariablesCommand) JobKey(
	key int64,
) SetVariablesCommandStep2 {
	command.request.JobKey = key
	return command
}

func (command *SetVariablesCommand) Variables(value any) SetVariablesCommandStep2 {
	command.variables.fromValue(value)
	return command
}

func (command *SetVariablesCommand) VariablesFromString(
	value string,
) SetVariablesCommandStep2 {
	command.variables.fromString(value)
	return command
}

func (command *SetVariablesCommand) Send(
	ctx context.Context,
) (*pb.SetVariablesResponse, error) {
	if command.variables.err != nil {
		return nil, command.variables.err
	}
	command.request.Variables = command.variables.value
	return command.gateway.SetVariables(ctx, &command.request)
}
