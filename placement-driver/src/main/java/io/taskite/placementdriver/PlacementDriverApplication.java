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
package io.taskite.placementdriver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Spring Boot entry point that owns the placement-driver process lifecycle. The embedded Tomcat
 * serves the admin/observability HTTP API (see {@link PlacementDriverAdminController}); the
 * actor/raft/gRPC runtime is started and stopped by {@link
 * io.taskite.placementdriver.bootstrap.PlacementDriverSpringLifecycle} in a lifecycle status that
 * runs before Tomcat accepts and after it stops accepting.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class PlacementDriverApplication {
  public static void main(final String[] args) {
    final SpringApplication application = new SpringApplication(PlacementDriverApplication.class);
    application.setAdditionalProfiles("placement-driver");
    application.run(args);
  }
}
