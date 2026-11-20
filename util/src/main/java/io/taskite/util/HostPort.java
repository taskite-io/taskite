/*
 * Copyright (c) 2026 lzgabel and Taskite contributors
 *
 * Licensed under the Taskite Community License, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     https://github.com/taskite-io/taskite/blob/main/LICENSE
 *
 * The Software is source-available for non-commercial use and limited evaluation
 * use. Other commercial use requires a separate written license. The Software is
 * provided "AS IS", without warranties or conditions of any kind.
 */
package io.taskite.util;

import java.util.Objects;

/**
 * Host fields are unbracketed; IPv6 endpoint text always includes brackets. No DNS lookup occurs.
 */
public record HostPort(String host, int port) {
  public HostPort {
    host = normalizeHost(host);
  }

  public static String normalizeHost(final String host) {
    final String value = Objects.requireNonNull(host, "host").trim();
    if (value.startsWith("[") && value.endsWith("]")) {
      return value.substring(1, value.length() - 1);
    }
    return value;
  }

  /** Also used for JRaft's Endpoint, whose string encoding does not add IPv6 brackets. */
  public static String formatHost(final String host) {
    final String value = normalizeHost(host);
    return value.contains(":") ? "[" + value + "]" : value;
  }

  public static String format(final String host, final int port) {
    return formatHost(host) + ":" + port;
  }

  public static HostPort parse(final String raw) {
    String value = Objects.requireNonNull(raw, "endpoint").trim();
    if (value.startsWith("http://")) {
      value = value.substring("http://".length());
    } else if (value.startsWith("https://")) {
      value = value.substring("https://".length());
    }
    final int separator = value.lastIndexOf(':');
    if (separator <= 0 || separator == value.length() - 1) {
      throw invalidEndpoint(raw);
    }
    final String host = value.substring(0, separator);
    // Accept legacy unbracketed IPv6 endpoints, but reject partial or nested brackets.
    if (host.contains("[") || host.contains("]")) {
      if (!host.startsWith("[")
          || !host.endsWith("]")
          || host.indexOf(']') != host.length() - 1
          || host.lastIndexOf('[') != 0
          || !host.contains(":")) {
        throw invalidEndpoint(raw);
      }
    }
    final String normalized = normalizeHost(host);
    if (normalized.isBlank()
        || normalized.chars().anyMatch(Character::isWhitespace)
        || normalized.contains("/")
        || normalized.contains("@")) {
      throw invalidEndpoint(raw);
    }
    try {
      final int port = Integer.parseInt(value.substring(separator + 1));
      if (port < 0 || port > 65535) {
        throw invalidEndpoint(raw);
      }
      return new HostPort(normalized, port);
    } catch (final NumberFormatException error) {
      throw invalidEndpoint(raw);
    }
  }

  private static IllegalArgumentException invalidEndpoint(final String raw) {
    return new IllegalArgumentException(
        "endpoint must use host:port or [ipv6]:port format: " + raw);
  }

  @Override
  public String toString() {
    return format(host, port);
  }
}
