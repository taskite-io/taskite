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

type QueryIncidentCommandStep1 interface {
	JobKey(key int64) QueryIncidentCommandStep2
}

type QueryIncidentCommandStep2 interface {
	Send(context.Context) (*pb.QueryIncidentResponse, error)
}

type QueryIncidentCommand struct {
	gateway pb.GatewayClient
	request pb.QueryIncidentRequest
}

func NewQueryIncidentCommand(gateway pb.GatewayClient) QueryIncidentCommandStep1 {
	return &QueryIncidentCommand{gateway: gateway}
}

func (command *QueryIncidentCommand) JobKey(key int64) QueryIncidentCommandStep2 {
	command.request.JobKey = key
	return command
}

func (command *QueryIncidentCommand) Send(
	ctx context.Context,
) (*pb.QueryIncidentResponse, error) {
	return command.gateway.QueryIncident(ctx, &command.request)
}

type ResolveIncidentCommandStep1 interface {
	IncidentKey(key int64) ResolveIncidentCommandStep2
}

type ResolveIncidentCommandStep2 interface {
	Send(context.Context) (*pb.ResolveIncidentResponse, error)
}

type ResolveIncidentCommand struct {
	gateway pb.GatewayClient
	request pb.ResolveIncidentRequest
}

func NewResolveIncidentCommand(gateway pb.GatewayClient) ResolveIncidentCommandStep1 {
	return &ResolveIncidentCommand{gateway: gateway}
}

func (command *ResolveIncidentCommand) IncidentKey(
	key int64,
) ResolveIncidentCommandStep2 {
	command.request.IncidentKey = key
	return command
}

func (command *ResolveIncidentCommand) Send(
	ctx context.Context,
) (*pb.ResolveIncidentResponse, error) {
	return command.gateway.ResolveIncident(ctx, &command.request)
}
