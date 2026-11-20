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
package io.taskite.webapp.bootstrap;

import io.taskite.webapp.api.ConsoleBootstrapController;
import io.taskite.webapp.runtime.ConsoleIndexController;
import io.taskite.webapp.runtime.ConsoleWebMvcConfiguration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

/** Loaded by the distribution classpath without changing the PD component scan or lifecycle. */
@AutoConfiguration
@Profile("placement-driver")
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnProperty(prefix = "taskite.console", name = "enabled", matchIfMissing = true)
@EnableConfigurationProperties(ConsoleProperties.class)
@Import({
  ConsoleBootstrapController.class,
  ConsoleIndexController.class,
  ConsoleWebMvcConfiguration.class
})
public class ConsoleAutoConfiguration {}
