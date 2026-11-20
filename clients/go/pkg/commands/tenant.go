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

	"google.golang.org/grpc/metadata"
)

const TenantIDHeader = "taskite-tenant-id"

// WithTenantID selects a request tenant, subject to Gateway authorization.
// An empty tenant leaves the client's default selection in effect.
func WithTenantID(ctx context.Context, tenantID string) context.Context {
	if tenantID == "" {
		return ctx
	}
	outgoing, _ := metadata.FromOutgoingContext(ctx)
	outgoing = outgoing.Copy()
	outgoing.Set(TenantIDHeader, tenantID)
	return metadata.NewOutgoingContext(ctx, outgoing)
}
