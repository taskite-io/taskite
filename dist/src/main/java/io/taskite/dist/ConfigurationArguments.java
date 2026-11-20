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
package io.taskite.dist;

/** Validates complete Spring property arguments for all distribution entrypoints. */
final class ConfigurationArguments {
  private ConfigurationArguments() {}

  static String[] validate(final String[] args) {
    for (final String arg : args) {
      final int separator = arg.indexOf('=');
      if (!arg.startsWith("--") || separator < 3 || !arg.substring(2, separator).contains(".")) {
        throw new IllegalArgumentException(
            "arguments must use a complete --property.path=value: " + arg);
      }
      final String key = arg.substring(2, separator);
      if (key.startsWith("pd.") || key.startsWith("broker.") || key.startsWith("gateway.")) {
        throw new IllegalArgumentException("component properties must start with taskite: " + arg);
      }
    }
    return args.clone();
  }
}
