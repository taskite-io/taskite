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
	"strings"
	"testing"
)

func TestNormalizeAddress(t *testing.T) {
	t.Parallel()

	for _, scenario := range []struct {
		address string
		want    string
	}{
		{"127.0.0.1:26500", "127.0.0.1:26500"},
		{"gateway.internal:26500", "gateway.internal:26500"},
		{"gateway.internal", "gateway.internal:26500"},
		{"127.0.0.1", "127.0.0.1:26500"},
		{"[::1]:26500", "[::1]:26500"},
		{"::1", "[::1]:26500"},
		{"[::1]", "[::1]:26500"},
		{"::1:26500", "[::1]:26500"},
		{"::1:80", "[::1:80]:26500"},
		{"0:0:0:0:0:0:0:1:80", "[0:0:0:0:0:0:0:1]:80"},
		{"2605:0340:cd50:2000:ffff:ffff:ffff:ffff", "[2605:0340:cd50:2000:ffff:ffff:ffff:ffff]:26500"},
		{"2605:0340:cd50:2000:ffff:ffff:ffff:ffff:26500", "[2605:0340:cd50:2000:ffff:ffff:ffff:ffff]:26500"},
		{"[::ffff:192.0.2.1]:26500", "[::ffff:192.0.2.1]:26500"},
		{"fe80::1%eth0", "[fe80::1%eth0]:26500"},
		{"[fe80::1%eth0]:26500", "[fe80::1%eth0]:26500"},
		{"fe80::1%eth0:26500", "[fe80::1%eth0]:26500"},
		{"dns:///[::1]:26500", "dns:///[::1]:26500"},
		{"dns:///[fe80::1%25eth0]:26500", "dns:///[fe80::1%25eth0]:26500"},
		{"passthrough:///custom-target", "passthrough:///custom-target"},
		{"unix:///tmp/taskite.sock", "unix:///tmp/taskite.sock"},
	} {
		t.Run(scenario.address, func(t *testing.T) {
			address, err := normalizeAddress(scenario.address, defaultGatewayPort)
			if err != nil || address != scenario.want {
				t.Fatalf("address=%q err=%v, want %q", address, err, scenario.want)
			}
		})
	}
}

func TestNormalizeAddressRejectsMalformedEndpoints(t *testing.T) {
	t.Parallel()

	for _, address := range []string{
		"", "host name:26500", "host\tname", ":26500", "host:", "host:zero",
		"host:0", "host:-1", "host:+80", "host:65536", "[::1]:65536",
		"[::1]:0", "[::1]:", "[::1]:abc", "[::1", "::1]", "[[::1]]:26500",
		"[127.0.0.1]:26500", "[hostname]:26500", "2001:db8::xyz:26500",
		"[fe80::1%]:26500", "[fe80::1%eth0:26500]:26500", "host/path:26500",
		"host?query:26500", "user@host:26500",
	} {
		t.Run(address, func(t *testing.T) {
			if _, err := normalizeAddress(address, defaultGatewayPort); err == nil {
				t.Fatalf("expected %q to be rejected", address)
			}
		})
	}
}

func TestResolveIPv6AddressesFromConfigAndEnvironment(t *testing.T) {
	t.Setenv(AddressEnvironmentVariable, " ::1:26500 ")
	t.Setenv(PlacementDriverAddressEnvironmentVariable, " [::2] ")
	t.Setenv(ClusterEnvironmentVariable, "demo")

	resolved, err := resolveConfig(ClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Gateway.Address != "[::1]:26500" || resolved.PlacementDriver.Address != "[::2]:26501" {
		t.Fatalf("unexpected environment addresses: %+v", resolved)
	}
	resolved, err = resolveConfig(ClientConfig{
		Gateway:         GatewayConfig{Address: " [2001:db8::3]:8080 "},
		PlacementDriver: PlacementDriverConfig{Address: " 2001:db8::4 "},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Gateway.Address != "[2001:db8::3]:8080" || resolved.PlacementDriver.Address != "[2001:db8::4]:26501" {
		t.Fatalf("environment replaced explicit addresses: %+v", resolved)
	}
}

func TestResolveConfigDefaultsAndAddressErrors(t *testing.T) {
	t.Setenv(AddressEnvironmentVariable, "")
	t.Setenv(PlacementDriverAddressEnvironmentVariable, "")
	t.Setenv(KeepAliveEnvironmentVariable, "")
	t.Setenv(WorkerRegistrationRenewIntervalEnvironmentVariable, "")
	t.Setenv(WorkerRegistrationRenewalTimeoutEnvironmentVariable, "")

	resolved, err := resolveConfig(ClientConfig{ClusterID: "demo"})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Gateway.Address != "127.0.0.1:26500" || resolved.PlacementDriver.Address != "127.0.0.1:26501" {
		t.Fatalf("unexpected default addresses: %+v", resolved)
	}
	for _, scenario := range []struct {
		config ClientConfig
		field  string
	}{
		{ClientConfig{ClusterID: "demo", Gateway: GatewayConfig{Address: "[::1]:65536"}}, "gateway address"},
		{ClientConfig{ClusterID: "demo", PlacementDriver: PlacementDriverConfig{Address: "[::1]:0"}}, "placement driver address"},
	} {
		if _, err := NewClient(scenario.config); err == nil || !strings.Contains(err.Error(), scenario.field) {
			t.Fatalf("expected %s error before dialing, got %v", scenario.field, err)
		}
	}
}
