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
	"testing"
	"time"

	"github.com/spf13/cobra"
)

func TestRequestTimeoutPrecedence(t *testing.T) {
	for _, test := range []struct {
		name        string
		environment string
		args        []string
		expected    time.Duration
		fails       bool
	}{
		{"default", "", nil, 30 * time.Second, false},
		{"environment", "12s", nil, 12 * time.Second, false},
		{"flag", "12s", []string{"--request-timeout=3s"}, 3 * time.Second, false},
		{"flag-over-invalid-environment", "invalid", []string{"--request-timeout=4s"}, 4 * time.Second, false},
		{"invalid-environment", "invalid", nil, 0, true},
		{"zero-environment", "0s", nil, 0, true},
		{"negative-flag", "12s", []string{"--request-timeout=-1s"}, 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(requestTimeoutEnvironmentVariable, test.environment)
			root := NewRootCommand()
			root.AddCommand(&cobra.Command{Use: "probe", RunE: func(command *cobra.Command, _ []string) error {
				duration, err := command.Flags().GetDuration("request-timeout")
				if err != nil {
					return err
				}
				if duration != test.expected {
					t.Fatalf("timeout=%s, want %s", duration, test.expected)
				}
				return nil
			}})
			root.SetArgs(append([]string{"probe"}, test.args...))
			err := root.Execute()
			if (err != nil) != test.fails {
				t.Fatalf("unexpected timeout validation: %v", err)
			}
		})
	}
}

func TestRootContainsEveryGatewayCommand(t *testing.T) {
	t.Parallel()

	root := NewRootCommand()
	paths := [][]string{
		{"create", "subscription"},
		{"create", "timer"},
		{"delete", "timer"},
		{"delete", "subscription"},
		{"create", "job"},
		{"activate", "jobs"},
		{"complete", "job"},
		{"fail", "job"},
		{"query", "job"},
		{"query", "incident"},
		{"cancel", "job"},
		{"update", "retries"},
		{"update", "priority"},
		{"update", "timeout"},
		{"resolve", "incident"},
		{"set", "variables"},
	}
	for _, path := range paths {
		command, remaining, err := root.Find(path)
		if err != nil {
			t.Fatalf("find %v: %v", path, err)
		}
		if len(remaining) != 0 || command.Name() != path[len(path)-1] {
			t.Fatalf("command path %v did not resolve: command=%s remaining=%v", path, command.Name(), remaining)
		}
	}

	activateJobs, _, err := root.Find([]string{"activate", "jobs"})
	if err != nil {
		t.Fatal(err)
	}
	if activateJobs.Flags().Lookup("handler") == nil {
		t.Fatal("activate jobs is missing --handler")
	}
	for _, removed := range []string{"insecure", "cert-path", "authority"} {
		if root.PersistentFlags().Lookup(removed) != nil {
			t.Fatalf("removed TLS flag --%s is still registered", removed)
		}
	}
}

func TestParseKey(t *testing.T) {
	t.Parallel()

	if key, err := parseKey("9223372036854775807"); err != nil ||
		key != int64(9223372036854775807) {
		t.Fatalf("parse maximum key: key=%d err=%v", key, err)
	}
	for _, value := range []string{"", "zero", "0", "-1", "9223372036854775808"} {
		if _, err := parseKey(value); err == nil {
			t.Fatalf("expected %q to be rejected", value)
		}
	}
}
