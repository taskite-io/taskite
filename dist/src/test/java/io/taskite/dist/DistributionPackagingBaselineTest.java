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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.dist.broker.BrokerApplication;
import io.taskite.dist.broker.BrokerSpringLifecycle;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Profile;

final class DistributionPackagingBaselineTest {

  @Test
  void shadedJarMergesSpringAutoConfigurationIndexes() throws Exception {
    final String pom = Files.readString(Path.of("pom.xml"));

    assertTrue(
        pom.contains(
            "<resource>META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports</resource>"));
    assertTrue(pom.contains("<resource>META-INF/spring.factories</resource>"));
  }

  @Test
  void brokerSpringComponentsAreIsolatedFromGatewayComponentScanning() {
    assertArrayEquals(
        new String[] {"broker"}, BrokerApplication.class.getAnnotation(Profile.class).value());
    assertArrayEquals(
        new String[] {"broker"}, BrokerSpringLifecycle.class.getAnnotation(Profile.class).value());
  }
}
