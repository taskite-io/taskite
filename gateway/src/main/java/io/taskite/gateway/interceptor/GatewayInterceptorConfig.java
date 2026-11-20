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
package io.taskite.gateway.interceptor;

/** Framework-independent configuration for one user-provided gRPC server interceptor. */
public record GatewayInterceptorConfig(String id, String className, String jarPath) {
  public GatewayInterceptorConfig {
    id = id == null ? "" : id.trim();
    className = className == null ? "" : className.trim();
    jarPath = jarPath == null ? "" : jarPath.trim();
  }

  public String resolvedId() {
    return id.isBlank() ? className : id;
  }
}
