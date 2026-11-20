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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.taskite.gateway.GatewayConfig;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

final class GatewayConfigurationTest {
  @Test
  void appliesDefaults() {
    final var config =
        new GatewayConfiguration(null, null, null, null, null, null).toGatewayConfig();
    assertEquals("0.0.0.0", config.network().host());
    assertEquals(26500, config.network().port());
    assertEquals(Duration.ofSeconds(30), config.network().minKeepAliveInterval());
    assertEquals("127.0.0.1:26501", config.placementDriver().endpoints());
    assertEquals(Duration.ofSeconds(10), config.placementDriver().requestTimeout());
    assertEquals(Duration.ofSeconds(5), config.placementDriver().topologyRefreshInterval());
    assertEquals(Duration.ofSeconds(30), config.broker().requestTimeout());
    assertEquals(3, config.broker().maxRetries());
    assertEquals(GatewayConfig.DEFAULT_MANAGEMENT_THREADS, config.threads().management());
    assertFalse(config.multiTenancy().enabled());
    assertEquals(List.of(), config.multiTenancy().interceptors());
  }

  @Test
  void bindsIndependentClientsAndInterceptorList() {
    final var environment = new StandardEnvironment();
    environment
        .getPropertySources()
        .addFirst(
            new MapPropertySource(
                "test",
                Map.ofEntries(
                    Map.entry("taskite.gateway.network.host", "10.0.0.1"),
                    Map.entry("taskite.gateway.network.port", "27500"),
                    Map.entry("taskite.gateway.network.min-keep-alive-interval", "45s"),
                    Map.entry("taskite.gateway.placement-driver.endpoints", "10.0.0.2:26501"),
                    Map.entry("taskite.gateway.placement-driver.request-timeout", "11s"),
                    Map.entry("taskite.gateway.placement-driver.topology-refresh-interval", "6s"),
                    Map.entry("taskite.gateway.broker.request-timeout", "7s"),
                    Map.entry("taskite.gateway.broker.max-retries", "4"),
                    Map.entry("taskite.gateway.threads.management", "6"),
                    Map.entry("taskite.gateway.multi-tenancy.enabled", "true"),
                    Map.entry("taskite.gateway.interceptors[0].id", "authentication"),
                    Map.entry(
                        "taskite.gateway.interceptors[0].class-name",
                        "com.example.AuthenticationInterceptor"),
                    Map.entry(
                        "taskite.gateway.interceptors[0].jar-path",
                        "plugins/authentication.jar"))));
    final var config = bind(environment);
    assertEquals("10.0.0.1", config.network().host());
    assertEquals(27500, config.network().port());
    assertEquals(Duration.ofSeconds(45), config.network().minKeepAliveInterval());
    assertEquals("10.0.0.2:26501", config.placementDriver().endpoints());
    assertEquals(Duration.ofSeconds(11), config.placementDriver().requestTimeout());
    assertEquals(Duration.ofSeconds(6), config.placementDriver().topologyRefreshInterval());
    assertEquals(Duration.ofSeconds(7), config.broker().requestTimeout());
    assertEquals(4, config.broker().maxRetries());
    assertEquals(6, config.threads().management());
    assertEquals(
        "com.example.AuthenticationInterceptor",
        config.multiTenancy().interceptors().getFirst().className());
    assertEquals(
        "plugins/authentication.jar", config.multiTenancy().interceptors().getFirst().jarPath());
  }

  @Test
  void partialClientGroupsKeepTheirDefaults() {
    final var environment = new StandardEnvironment();
    environment
        .getPropertySources()
        .addFirst(
            new MapPropertySource(
                "partial",
                Map.of(
                    "taskite.gateway.placement-driver.request-timeout", "2s",
                    "taskite.gateway.broker.max-retries", "0")));
    final var config = bind(environment);
    assertEquals(Duration.ofSeconds(2), config.placementDriver().requestTimeout());
    assertEquals(Duration.ofSeconds(5), config.placementDriver().topologyRefreshInterval());
    assertEquals(Duration.ofSeconds(30), config.broker().requestTimeout());
    assertEquals(0, config.broker().maxRetries());
    assertEquals(20, config.threads().management());
  }

  @Test
  void rejectsInvalidValuesAndAcceptsCompleteCliProperties() {
    for (final var values :
        List.of(
            Map.of("taskite.gateway.network.port", "70000"),
            Map.of("taskite.gateway.threads.management", "0"),
            Map.of("taskite.gateway.placement-driver.request-timeout", "0s"),
            Map.of("taskite.gateway.broker.request-timeout", "-1s"),
            Map.of("taskite.gateway.broker.max-retries", "-1"),
            Map.of("taskite.gateway.multi-tenancy.enabled", "true"))) {
      final var environment = new StandardEnvironment();
      environment
          .getPropertySources()
          .addFirst(new MapPropertySource("invalid", new LinkedHashMap<>(values)));
      assertThrows(RuntimeException.class, () -> bind(environment));
    }
    final String[] args = {"--taskite.gateway.network.port=0", "--server.port=-1"};
    assertArrayEquals(args, ConfigurationArguments.validate(args));
    assertThrows(
        IllegalArgumentException.class,
        () -> ConfigurationArguments.validate(new String[] {"--port=26501"}));
    assertThrows(
        IllegalArgumentException.class,
        () -> ConfigurationArguments.validate(new String[] {"--gateway.network.port=26500"}));
  }

  private static GatewayConfig bind(final StandardEnvironment environment) {
    return Binder.get(environment)
        .bind("taskite.gateway", Bindable.of(GatewayConfiguration.class))
        .orElseThrow(() -> new IllegalStateException("gateway configuration was not bound"))
        .toGatewayConfig();
  }
}
