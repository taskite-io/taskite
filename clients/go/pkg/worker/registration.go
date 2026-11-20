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

package worker

import (
	"errors"
	"time"
)

const (
	DefaultRegistrationRenewInterval  = 5 * time.Second
	DefaultRegistrationRenewalTimeout = 30 * time.Second
)

// RegistrationConfig groups the Worker registration and renewal policy.
// RenewalTimeout bounds each PD registration, renewal, and deregistration RPC.
// It is also reported to PD in registration and renewal requests.
// PD's worker lease timeout remains authoritative for offline detection.
type RegistrationConfig struct {
	RenewInterval  time.Duration
	RenewalTimeout time.Duration
	Backpressure   BackpressureConfig
}

// WithDefaults resolves omitted parameters for a custom registry builder.
func (config RegistrationConfig) WithDefaults() RegistrationConfig {
	if config.RenewInterval == 0 {
		config.RenewInterval = DefaultRegistrationRenewInterval
	}
	if config.RenewalTimeout == 0 {
		config.RenewalTimeout = DefaultRegistrationRenewalTimeout
	}
	config.Backpressure = config.Backpressure.WithDefaults()
	return config
}

// Validate checks a resolved configuration before any registration is sent.
func (config RegistrationConfig) Validate() error {
	switch {
	case config.RenewInterval < time.Millisecond:
		return errors.New("worker registration renew interval must be at least one millisecond")
	case config.RenewalTimeout < time.Millisecond:
		return errors.New("worker registration renewal timeout must be at least one millisecond")
	case config.RenewInterval >= config.RenewalTimeout:
		return errors.New("worker registration renew interval must be less than renewal timeout")
	default:
		return config.Backpressure.Validate()
	}
}
