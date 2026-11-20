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
package io.taskite.webapp.api;

import io.taskite.webapp.bootstrap.ConsoleProperties;
import java.io.IOException;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Fixed display contract; intentionally contains no Grafana settings or runtime internals. */
@RestController
public class ConsoleBootstrapController {
  private final ConsoleProperties properties;
  private final Environment environment;
  private final String buildVersion;

  public ConsoleBootstrapController(
      final ConsoleProperties properties, final Environment environment) {
    this.properties = properties;
    this.environment = environment;
    buildVersion = readBuildVersion();
  }

  @GetMapping("/api/v1/console/bootstrap")
  public ResponseEntity<ConsoleBootstrapResponse> bootstrap() {
    return ResponseEntity.ok()
        .cacheControl(CacheControl.noStore())
        .body(
            new ConsoleBootstrapResponse(
                "Taskite Console",
                "/console/",
                "/api/v1",
                buildVersion,
                properties.deploymentId(),
                environment.getProperty("taskite.pd.node-id", "")));
  }

  private static String readBuildVersion() {
    try {
      return PropertiesLoaderUtils.loadProperties(
              new ClassPathResource("META-INF/taskite-console/build.properties"))
          .getProperty("version", "development");
    } catch (final IOException unavailable) {
      return "development";
    }
  }

  public record ConsoleBootstrapResponse(
      String productName,
      String consoleBasePath,
      String apiBasePath,
      String buildVersion,
      String deploymentId,
      String nodeId) {}
}
