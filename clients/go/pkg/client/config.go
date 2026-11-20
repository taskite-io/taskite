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

package client

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/taskite-io/taskite/clients/go/pkg/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	defaultGatewayPort            = "26500"
	defaultPlacementDriverPort    = "26501"
	DefaultGatewayAddress         = "127.0.0.1:" + defaultGatewayPort
	DefaultPlacementDriverAddress = "127.0.0.1:" + defaultPlacementDriverPort
	DefaultTenantID               = "<default>"
	DefaultKeepAlive              = 45 * time.Second

	AddressEnvironmentVariable                                     = "TASKITE_GATEWAY_ADDRESS"
	PlacementDriverAddressEnvironmentVariable                      = "TASKITE_PLACEMENT_DRIVER_ADDRESS"
	ClusterEnvironmentVariable                                     = "TASKITE_CLUSTER_ID"
	TenantEnvironmentVariable                                      = "TASKITE_TENANT_ID"
	KeepAliveEnvironmentVariable                                   = "TASKITE_TRANSPORT_KEEP_ALIVE"
	WorkerRegistrationRenewIntervalEnvironmentVariable             = "TASKITE_WORKER_REGISTRATION_RENEW_INTERVAL"
	WorkerRegistrationRenewalTimeoutEnvironmentVariable            = "TASKITE_WORKER_REGISTRATION_RENEWAL_TIMEOUT"
	WorkerRegistrationBackpressureMinDelayEnvironmentVariable      = "TASKITE_WORKER_REGISTRATION_BACKPRESSURE_MIN_DELAY"
	WorkerRegistrationBackpressureMaxDelayEnvironmentVariable      = "TASKITE_WORKER_REGISTRATION_BACKPRESSURE_MAX_DELAY"
	WorkerRegistrationBackpressureBackoffFactorEnvironmentVariable = "TASKITE_WORKER_REGISTRATION_BACKPRESSURE_BACKOFF_FACTOR"
	WorkerRegistrationBackpressureJitterFactorEnvironmentVariable  = "TASKITE_WORKER_REGISTRATION_BACKPRESSURE_JITTER_FACTOR"
)

var clusterIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

type ClientConfig struct {
	Gateway         GatewayConfig
	PlacementDriver PlacementDriverConfig
	ClusterID       string
	TenantID        string
	Transport       TransportConfig
	Worker          WorkerConfig
	UserAgent       string
	DialOptions     []grpc.DialOption
}

type GatewayConfig struct {
	Address string
}

type PlacementDriverConfig struct {
	Address string
}

type TransportConfig struct {
	KeepAlive time.Duration
}

type WorkerConfig struct {
	Registration worker.RegistrationConfig
}

func resolveConfig(config ClientConfig) (ClientConfig, error) {
	if config.Gateway.Address == "" {
		config.Gateway.Address = valueOrDefault(AddressEnvironmentVariable, DefaultGatewayAddress)
	}
	if config.PlacementDriver.Address == "" {
		config.PlacementDriver.Address = valueOrDefault(
			PlacementDriverAddressEnvironmentVariable,
			DefaultPlacementDriverAddress,
		)
	}
	if config.ClusterID == "" {
		config.ClusterID = os.Getenv(ClusterEnvironmentVariable)
	}
	if config.TenantID == "" {
		config.TenantID = valueOrDefault(TenantEnvironmentVariable, DefaultTenantID)
	}
	config.Gateway.Address = strings.TrimSpace(config.Gateway.Address)
	config.PlacementDriver.Address = strings.TrimSpace(config.PlacementDriver.Address)
	config.ClusterID = strings.TrimSpace(config.ClusterID)
	config.TenantID = strings.TrimSpace(config.TenantID)
	if config.Transport.KeepAlive == 0 {
		value := os.Getenv(KeepAliveEnvironmentVariable)
		if value == "" {
			config.Transport.KeepAlive = DefaultKeepAlive
		} else {
			keepAlive, err := time.ParseDuration(value)
			if err != nil {
				return config, fmt.Errorf(
					"%s must be a duration: %w",
					KeepAliveEnvironmentVariable,
					err,
				)
			}
			config.Transport.KeepAlive = keepAlive
		}
	}
	if config.UserAgent == "" {
		config.UserAgent = "taskite-client-go/" + Version
	}
	var err error
	registration := config.Worker.Registration
	registration.RenewInterval, err = configuredDuration(
		registration.RenewInterval, WorkerRegistrationRenewIntervalEnvironmentVariable,
		worker.DefaultRegistrationRenewInterval,
	)
	if err != nil {
		return config, err
	}
	registration.RenewalTimeout, err = configuredDuration(
		registration.RenewalTimeout, WorkerRegistrationRenewalTimeoutEnvironmentVariable,
		worker.DefaultRegistrationRenewalTimeout,
	)
	if err != nil {
		return config, err
	}
	backpressure := registration.Backpressure
	backpressure.MinDelay, err = configuredDuration(
		backpressure.MinDelay,
		WorkerRegistrationBackpressureMinDelayEnvironmentVariable,
		worker.DefaultBackpressureMinDelay,
	)
	if err != nil {
		return config, err
	}
	backpressure.MaxDelay, err = configuredDuration(
		backpressure.MaxDelay,
		WorkerRegistrationBackpressureMaxDelayEnvironmentVariable,
		worker.DefaultBackpressureMaxDelay,
	)
	if err != nil {
		return config, err
	}
	var factor *float64
	if backpressure.BackoffFactor != 0 {
		factor = &backpressure.BackoffFactor
	}
	factor, err = configuredFloat(factor, WorkerRegistrationBackpressureBackoffFactorEnvironmentVariable,
		worker.DefaultBackpressureBackoffFactor)
	if err != nil {
		return config, err
	}
	backpressure.BackoffFactor = *factor
	backpressure.JitterFactor, err = configuredFloat(backpressure.JitterFactor,
		WorkerRegistrationBackpressureJitterFactorEnvironmentVariable, worker.DefaultBackpressureJitterFactor)
	if err != nil {
		return config, err
	}
	registration.Backpressure = backpressure
	config.Worker.Registration = registration
	if err := validateConfig(config); err != nil {
		return config, err
	}
	config.Gateway.Address, err = normalizeAddress(config.Gateway.Address, defaultGatewayPort)
	if err != nil {
		return config, fmt.Errorf("gateway address: %w", err)
	}
	config.PlacementDriver.Address, err = normalizeAddress(config.PlacementDriver.Address, defaultPlacementDriverPort)
	if err != nil {
		return config, fmt.Errorf("placement driver address: %w", err)
	}
	return config, nil
}

func validateConfig(config ClientConfig) error {
	clusterID := strings.TrimSpace(config.ClusterID)
	switch {
	case strings.TrimSpace(config.Gateway.Address) == "":
		return errors.New("gateway address must be present")
	case strings.TrimSpace(config.PlacementDriver.Address) == "":
		return errors.New("placement driver address must be present")
	case clusterID == "":
		return fmt.Errorf(
			"cluster ID must be configured directly or through %s",
			ClusterEnvironmentVariable,
		)
	case len(clusterID) > 128 || !clusterIDPattern.MatchString(clusterID):
		return errors.New(
			"cluster ID must match [A-Za-z0-9._-]+ and contain at most 128 characters",
		)
	case config.Transport.KeepAlive <= 0:
		return errors.New("keep alive must be positive")
	case strings.TrimSpace(config.TenantID) == "":
		return errors.New("tenant ID must be present")
	case len(config.TenantID) > 256:
		return errors.New("tenant ID must contain at most 256 characters")
	default:
		return config.Worker.Registration.Validate()
	}
}

func configuredFloat(value *float64, environment string, fallback float64) (*float64, error) {
	resolved := fallback
	if value != nil {
		resolved = *value
	} else if raw := os.Getenv(environment); raw != "" {
		number, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("%s must be a number: %w", environment, err)
		}
		resolved = number
	}
	return &resolved, nil
}

func configuredDuration(value time.Duration, environment string, fallback time.Duration) (time.Duration, error) {
	if value != 0 {
		return value, nil
	}
	raw := os.Getenv(environment)
	if raw == "" {
		return fallback, nil
	}
	duration, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration: %w", environment, err)
	}
	return duration, nil
}

func dialOptions(config ClientConfig) []grpc.DialOption {
	options := append([]grpc.DialOption(nil), config.DialOptions...)
	options = append(
		options,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUserAgent(config.UserAgent),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: config.Transport.KeepAlive}),
		grpc.WithChainUnaryInterceptor(requestUnaryInterceptor(config.ClusterID, config.TenantID)),
		grpc.WithChainStreamInterceptor(requestStreamInterceptor(config.ClusterID, config.TenantID)),
	)
	return options
}

func valueOrDefault(name string, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
