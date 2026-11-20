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
package io.taskite.dist.broker;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

/** Spring Boot entry point for the standalone Broker and its Actuator management server. */
@SpringBootApplication
@EnableConfigurationProperties(BrokerConfiguration.class)
@Profile("broker")
public class BrokerApplication {

  public static void main(final String[] args) {
    final SpringApplication application = new SpringApplication(BrokerApplication.class);
    application.setAdditionalProfiles("broker");
    application.run(args);
  }

  @Bean
  MeterRegistryCustomizer<MeterRegistry> taskiteMetricTags(
      final BrokerConfiguration configuration) {
    return registry ->
        registry
            .config()
            .commonTags(
                "cluster",
                configuration.cluster().clusterId(),
                "broker",
                configuration.cluster().brokerId());
  }
}
