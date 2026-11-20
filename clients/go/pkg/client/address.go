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
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"unicode"
)

// normalizeAddress formats TCP endpoints without resolving DNS. A complete bare
// IPv6 literal uses the default port; explicit IPv6 ports should use brackets.
// Legacy bare IPv6:port is accepted when it cannot be a complete IPv6 literal.
func normalizeAddress(address, defaultPort string) (string, error) {
	// Resolver targets, including custom DialOptions/bufconn targets, retain their
	// gRPC semantics and URI escaping.
	if strings.Contains(address, "://") {
		return address, nil
	}
	invalid := func() (string, error) {
		return "", fmt.Errorf(
			"%q must use a host, host:port or [ipv6]:port with a port between 1 and 65535",
			address,
		)
	}
	if address == "" || strings.ContainsAny(address, "/?#@") ||
		strings.IndexFunc(address, unicode.IsSpace) >= 0 {
		return invalid()
	}

	host, port := address, defaultPort
	if strings.HasPrefix(address, "[") && strings.HasSuffix(address, "]") {
		host = address[1 : len(address)-1]
		if !isIPv6Host(host) {
			return invalid()
		}
	} else if !isIPv6Host(address) {
		var err error
		if strings.Contains(address, ":") {
			host, port, err = net.SplitHostPort(address)
			if err != nil {
				// A legacy endpoint may contain an unbracketed IPv6 host.
				separator := strings.LastIndexByte(address, ':')
				host, port = address[:separator], address[separator+1:]
				if !isIPv6Host(host) || strings.ContainsAny(address, "[]") {
					return invalid()
				}
			}
		}
		if strings.ContainsAny(host, ":[]%") {
			if !isIPv6Host(host) {
				return invalid()
			}
		} else if strings.ContainsAny(address, "[]") {
			return invalid()
		}
	}
	if host == "" {
		return invalid()
	}
	number, err := strconv.ParseUint(port, 10, 16)
	if err != nil || number == 0 {
		return invalid()
	}
	return net.JoinHostPort(host, strconv.FormatUint(number, 10)), nil
}

func isIPv6Host(host string) bool {
	address, err := netip.ParseAddr(host)
	return err == nil && address.Is6() &&
		!strings.ContainsAny(address.Zone(), ":[]%")
}

func grpcAddress(address string) string {
	if strings.Contains(address, "://") || !strings.Contains(address, "%") {
		return address
	}
	// gRPC parses the endpoint as a URL path. The IPv6 zone delimiter must be
	// escaped here so the dialer receives the original %interface suffix.
	// gRPC's DNS resolver cannot parse scoped literals, so dial them directly.
	return "passthrough:///" + strings.ReplaceAll(address, "%", "%25")
}
