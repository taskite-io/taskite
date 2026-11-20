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
	"math"
	"math/rand/v2"
	"time"
)

const (
	DefaultBackpressureMinDelay      = 50 * time.Millisecond
	DefaultBackpressureMaxDelay      = 5 * time.Second
	DefaultBackpressureBackoffFactor = 1.6
	DefaultBackpressureJitterFactor  = 0.1
)

// BackpressureConfig configures registration retries. A nil JitterFactor uses
// the default; a pointer to zero explicitly disables jitter.
type BackpressureConfig struct {
	MinDelay      time.Duration
	MaxDelay      time.Duration
	BackoffFactor float64
	JitterFactor  *float64
	// Random is only supplied in code; nil uses the concurrent-safe default.
	Random func() float64 `json:"-"`
}

// WithDefaults resolves omitted values and copies the optional jitter factor.
func (config BackpressureConfig) WithDefaults() BackpressureConfig {
	if config.MinDelay == 0 {
		config.MinDelay = DefaultBackpressureMinDelay
	}
	if config.MaxDelay == 0 {
		config.MaxDelay = DefaultBackpressureMaxDelay
	}
	if config.BackoffFactor == 0 {
		config.BackoffFactor = DefaultBackpressureBackoffFactor
	}
	jitter := DefaultBackpressureJitterFactor
	if config.JitterFactor != nil {
		jitter = *config.JitterFactor
	}
	config.JitterFactor = &jitter
	if config.Random == nil {
		config.Random = rand.Float64
	}
	return config
}

// Validate checks the resolved retry parameters before registration starts.
func (config BackpressureConfig) Validate() error {
	jitter := DefaultBackpressureJitterFactor
	if config.JitterFactor != nil {
		jitter = *config.JitterFactor
	}
	return validateBackoff(config.MinDelay, config.MaxDelay, config.BackoffFactor, jitter)
}

// BackoffSupplier calculates the delay after a failed activation request.
type BackoffSupplier interface {
	Next(previous time.Duration) time.Duration
}

// ExponentialBackoff is a bounded retry policy with optional symmetric jitter.
// Its zero value preserves the deterministic activation retry defaults.
type ExponentialBackoff struct {
	MinDelay     time.Duration
	MaxDelay     time.Duration
	Factor       float64
	JitterFactor float64
	random       func() float64
}

func (backoff ExponentialBackoff) Next(previous time.Duration) time.Duration {
	minDelay := backoff.MinDelay
	if minDelay <= 0 {
		minDelay = 100 * time.Millisecond
	}
	maxDelay := backoff.MaxDelay
	if maxDelay <= 0 {
		maxDelay = 5 * time.Second
	}
	if maxDelay < minDelay {
		maxDelay = minDelay
	}
	factor := backoff.Factor
	if factor < 1 {
		factor = 1.6
	}
	baseline := math.Max(float64(minDelay), math.Min(float64(maxDelay), float64(previous)*factor))
	if backoff.JitterFactor != 0 {
		random := backoff.random
		if random == nil {
			random = rand.Float64
		}
		baseline += baseline * backoff.JitterFactor * (2*random() - 1)
	}
	// Clamp before duration conversion to avoid overflowing for very large delays
	// or factors. Millisecond rounding matches the Broker retry policy.
	if baseline >= float64(maxDelay) {
		return maxDelay
	}
	if backoff.JitterFactor == 0 && backoff.random == nil {
		return time.Duration(baseline)
	}
	rounded := math.Round(baseline/float64(time.Millisecond)) * float64(time.Millisecond)
	if rounded >= float64(maxDelay) {
		return maxDelay
	}
	return max(time.Millisecond, time.Duration(rounded))
}

// ExponentialBackoffBuilder supplies registration defaults and validates the
// same min/max, growth and symmetric jitter parameters as the Broker builder.
type ExponentialBackoffBuilder struct {
	backoff ExponentialBackoff
}

func NewExponentialBackoffBuilder() *ExponentialBackoffBuilder {
	return &ExponentialBackoffBuilder{backoff: ExponentialBackoff{
		MinDelay: DefaultBackpressureMinDelay, MaxDelay: DefaultBackpressureMaxDelay,
		Factor: DefaultBackpressureBackoffFactor, JitterFactor: DefaultBackpressureJitterFactor,
		random: rand.Float64,
	}}
}

func (builder *ExponentialBackoffBuilder) MinDelay(delay time.Duration) *ExponentialBackoffBuilder {
	builder.backoff.MinDelay = delay
	return builder
}

func (builder *ExponentialBackoffBuilder) MaxDelay(delay time.Duration) *ExponentialBackoffBuilder {
	builder.backoff.MaxDelay = delay
	return builder
}

func (builder *ExponentialBackoffBuilder) BackoffFactor(factor float64) *ExponentialBackoffBuilder {
	builder.backoff.Factor = factor
	return builder
}

func (builder *ExponentialBackoffBuilder) JitterFactor(factor float64) *ExponentialBackoffBuilder {
	builder.backoff.JitterFactor = factor
	return builder
}

// Random injects a generator returning values in [0, 1), such as rand.Float64.
// A custom generator shared across workers must support concurrent calls.
func (builder *ExponentialBackoffBuilder) Random(random func() float64) *ExponentialBackoffBuilder {
	builder.backoff.random = random
	return builder
}

func (builder *ExponentialBackoffBuilder) Build() (ExponentialBackoff, error) {
	backoff := builder.backoff
	if err := validateBackoff(backoff.MinDelay, backoff.MaxDelay, backoff.Factor, backoff.JitterFactor); err != nil {
		return ExponentialBackoff{}, err
	}
	if backoff.random == nil {
		return ExponentialBackoff{}, errors.New("backoff random generator must be present")
	}
	return backoff, nil
}

func validateBackoff(minDelay, maxDelay time.Duration, factor, jitter float64) error {
	switch {
	case minDelay < time.Millisecond:
		return errors.New("backpressure min delay must be at least one millisecond")
	case maxDelay < minDelay:
		return errors.New("backpressure max delay must not be less than min delay")
	case math.IsNaN(factor) || math.IsInf(factor, 0) || factor < 1:
		return errors.New("backpressure backoff factor must be finite and at least 1")
	case math.IsNaN(jitter) || math.IsInf(jitter, 0) || jitter < 0 || jitter > 1:
		return errors.New("backpressure jitter factor must be finite and between 0 and 1")
	default:
		return nil
	}
}

func registrationBackoff(config RegistryConfig) ExponentialBackoff {
	parameters := config.Registration.Backpressure.WithDefaults()
	maxDelay := min(parameters.MaxDelay, config.Registration.RenewInterval)
	// The builder inputs have already been validated by the worker builder.
	return ExponentialBackoff{
		MinDelay: min(parameters.MinDelay, maxDelay), MaxDelay: maxDelay,
		Factor: parameters.BackoffFactor, JitterFactor: *parameters.JitterFactor,
		random: parameters.Random,
	}
}
