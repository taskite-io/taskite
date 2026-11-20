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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.dist.broker.BrokerConfiguration;
import io.taskite.placementdriver.PlacementDriverConfiguration;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.core.io.FileSystemResource;

final class ConfigurationHierarchyTest {
  @Test
  void catalogCoversEveryPublicModelFieldAndMatchesTemplates() throws Exception {
    final var catalog = TaskiteEnvironmentPostProcessor.catalog();
    final Set<String> fields = new HashSet<>();
    collect("taskite.broker", BrokerConfiguration.class, fields);
    collect("taskite.pd", PlacementDriverConfiguration.class, fields);
    collect("taskite.gateway", GatewayConfiguration.class, fields);
    assertEquals(
        fields,
        catalog.keySet().stream()
            .filter(path -> !path.startsWith("taskite.console."))
            .collect(Collectors.toSet()));
    final var loader = new YamlPropertySourceLoader();
    for (final String component : List.of("broker", "gateway", "placement-driver")) {
      final var source =
          loader
              .load(
                  component,
                  new FileSystemResource("src/main/config/" + component + ".yaml.template"))
              .getFirst();
      final var packaged =
          loader
              .load(
                  "defaults",
                  new FileSystemResource(
                      component.equals("broker")
                          ? "src/main/resources/application-broker.yaml"
                          : "src/main/resources/application.yaml"))
              .getFirst();
      final String prefix =
          "taskite." + (component.equals("placement-driver") ? "pd" : component) + ".";
      for (final String path : catalog.keySet()) {
        if (path.startsWith(prefix)) {
          assertEquals(packaged.getProperty(path), source.getProperty(path), path);
        }
      }
    }
  }

  @Test
  void everyEnvironmentNameOverridesAnExternalValue() {
    final var catalog = TaskiteEnvironmentPostProcessor.catalog();
    final Map<String, Object> values = new LinkedHashMap<>();
    final Map<String, Object> external = new LinkedHashMap<>();
    catalog.forEach(
        (path, variable) -> {
          values.put(variable, "env-" + path);
          external.put(path, "file-" + path);
        });
    final var environment = environment(values);
    environment.getPropertySources().addLast(new MapPropertySource("external", external));
    new TaskiteEnvironmentPostProcessor().postProcessEnvironment(environment, null);
    catalog.forEach(
        (path, variable) ->
            assertEquals(values.get(variable), environment.getProperty(path), path));
  }

  @Test
  void realProfilesExternalYamlEnvironmentAndCliBindTogether(@TempDir final Path directory)
      throws Exception {
    final var file = directory.resolve("application.yaml");
    Files.writeString(
        file,
        """
        taskite:
          broker:
            cluster:
              cluster-id: external
            storage:
              snapshot:
                interval: 4m
          pd:
            node-id: external-pd
            broker:
              client:
                request-timeout: 8s
          gateway:
            placement-driver:
              request-timeout: 12s
            broker:
              request-timeout: 14s
            threads:
              management: 9
            interceptors:
              - id: external
                class-name: external.Class
                jar-path: external.jar
        """);
    final var environment =
        environment(
            Map.ofEntries(
                Map.entry("TASKITE_BROKER_CLUSTER_CLUSTER_ID", "env"),
                Map.entry("TASKITE_BROKER_REGISTRATION_PLACEMENT_DRIVERS", "127.0.0.1:26501"),
                Map.entry("TASKITE_BROKER_STORAGE_SNAPSHOT_INTERVAL", "6m"),
                Map.entry("TASKITE_PD_NODE_ID", "env-pd"),
                Map.entry("TASKITE_PD_BROKER_CLIENT_REQUEST_TIMEOUT", "16s"),
                Map.entry("TASKITE_PD_BROKER_CLIENT_MAX_RETRIES", "4"),
                Map.entry("TASKITE_GATEWAY_PLACEMENT_DRIVER_REQUEST_TIMEOUT", "18s"),
                Map.entry("TASKITE_GATEWAY_BROKER_REQUEST_TIMEOUT", "20s"),
                Map.entry("TASKITE_GATEWAY_THREADS_MANAGEMENT", "11"),
                Map.entry("TASKITE_GATEWAY_INTERCEPTORS_0_ID", "env"),
                Map.entry("TASKITE_GATEWAY_INTERCEPTORS_0_CLASS_NAME", "env.Class"),
                Map.entry("TASKITE_GATEWAY_INTERCEPTORS_0_JAR_PATH", "env.jar"),
                Map.entry("TASKITE_GATEWAY_INTERCEPTORS_1_ID", "second"),
                Map.entry("TASKITE_GATEWAY_INTERCEPTORS_1_CLASS_NAME", "env.Second"),
                Map.entry("TASKITE_GATEWAY_INTERCEPTORS_1_JAR_PATH", "second.jar")));
    final var application = new SpringApplication(Bindings.class);
    application.setWebApplicationType(WebApplicationType.NONE);
    application.setEnvironment(environment);
    try (final var context =
        application.run(
            "--spring.profiles.active=broker,placement-driver",
            "--spring.config.additional-location=" + file.toUri(),
            "--spring.main.banner-mode=off",
            "--taskite.broker.cluster.cluster-id=cli",
            "--taskite.gateway.broker.request-timeout=22s")) {
      final var broker = context.getBean(BrokerConfiguration.class).toBrokerConfig();
      final var pd = context.getBean(PlacementDriverConfiguration.class);
      final var gateway = context.getBean(GatewayConfiguration.class).toGatewayConfig();
      pd.validate();
      assertEquals("cli", broker.clusterId());
      assertEquals(Duration.ofMinutes(6), broker.snapshot().interval());
      assertEquals("env-pd", pd.nodeId());
      assertEquals(Path.of("data/meta-cluster/env-pd"), pd.storage().dataDir());
      assertEquals(Duration.ofSeconds(16), pd.broker().client().requestTimeout());
      assertEquals(4, pd.broker().client().maxRetries());
      assertEquals(Duration.ofSeconds(18), gateway.placementDriver().requestTimeout());
      assertEquals(Duration.ofSeconds(22), gateway.broker().requestTimeout());
      assertEquals(11, gateway.threads().management());
      assertEquals("env.Class", gateway.multiTenancy().interceptors().getFirst().className());
      assertEquals("env.jar", gateway.multiTenancy().interceptors().getFirst().jarPath());
      assertEquals(2, gateway.multiTenancy().interceptors().size());
      assertEquals("env.Second", gateway.multiTenancy().interceptors().get(1).className());
      assertTrue(context.getEnvironment().getProperty("taskite.console.enabled", Boolean.class));
      assertFalse(gateway.multiTenancy().enabled());
    }
  }

  private static StandardEnvironment environment(final Map<String, Object> values) {
    final var environment = new StandardEnvironment();
    environment
        .getPropertySources()
        .replace(
            StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
            new SystemEnvironmentPropertySource(
                StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME, values));
    return environment;
  }

  private static void collect(final String path, final Type type, final Set<String> paths) {
    if (type instanceof ParameterizedType list) {
      if (path.endsWith(".bootstrap-peers")) {
        paths.add(path);
      } else {
        collect(path + "[0]", list.getActualTypeArguments()[0], paths);
      }
      return;
    }
    final Class<?> model = (Class<?>) type;
    if (!model.getPackageName().startsWith("io.taskite") || model.isEnum()) {
      paths.add(path);
      return;
    }
    for (final var field : model.getDeclaredFields()) {
      if (!Modifier.isStatic(field.getModifiers())) {
        collect(
            path
                + "."
                + field.getName().replaceAll("([a-z0-9])([A-Z])", "$1-$2").toLowerCase(Locale.ROOT),
            field.getGenericType(),
            paths);
      }
    }
  }

  @Configuration(proxyBeanMethods = false)
  @EnableConfigurationProperties({
    BrokerConfiguration.class,
    GatewayConfiguration.class,
    PlacementDriverConfiguration.class
  })
  static class Bindings {}
}
