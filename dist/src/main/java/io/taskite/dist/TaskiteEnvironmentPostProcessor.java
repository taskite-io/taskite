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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;

/**
 * Maps complete public environment names to their YAML paths, including kebab-case and arrays.
 * Packaged profile placeholders are the catalog; external files need not repeat those placeholders.
 */
public final class TaskiteEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {
  private static final Pattern PLACEHOLDER = Pattern.compile("^\\$\\{(TASKITE_[A-Z0-9_]+):.*}$");
  static final String SOURCE_NAME = "taskiteEnvironment";
  static final String[] PROFILES = {
    "application.yaml", "application-broker.yaml", "application-placement-driver.yaml"
  };

  @Override
  public void postProcessEnvironment(
      final ConfigurableEnvironment environment, final SpringApplication application) {
    final var source =
        environment
            .getPropertySources()
            .get(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
    if (source == null) {
      return;
    }
    final Map<String, Object> values = new LinkedHashMap<>();
    catalog()
        .forEach(
            (path, variable) -> {
              if (path.contains("[0]")
                  && source instanceof EnumerablePropertySource<?> properties) {
                final var indexed = Pattern.compile(variable.replace("_0_", "_([0-9]+)_"));
                for (final String name : properties.getPropertyNames()) {
                  final var match = indexed.matcher(name);
                  if (match.matches()) {
                    values.put(
                        path.replace("[0]", "[" + match.group(1) + "]"), source.getProperty(name));
                  }
                }
                return;
              }
              final Object value = source.getProperty(variable);
              if (value != null) {
                values.put(path, value);
              }
            });
    environment
        .getPropertySources()
        .addBefore(
            StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
            new MapPropertySource(SOURCE_NAME, values));
  }

  static Map<String, String> catalog() {
    final Map<String, String> catalog = new LinkedHashMap<>();
    final var loader = new YamlPropertySourceLoader();
    try {
      for (final String profile : PROFILES) {
        for (final var source : loader.load(profile, new ClassPathResource(profile))) {
          final var properties = (EnumerablePropertySource<?>) source;
          for (final String path : properties.getPropertyNames()) {
            if (!path.startsWith("taskite.")) {
              continue;
            }
            final var matcher = PLACEHOLDER.matcher(String.valueOf(properties.getProperty(path)));
            if (!matcher.matches()) {
              throw new IllegalStateException(
                  "public configuration needs an ENV placeholder: " + path);
            }
            final String expected =
                path.replaceAll("[.\\-\\[\\]]+", "_").replaceAll("_$", "").toUpperCase(Locale.ROOT);
            if (!expected.equals(matcher.group(1))) {
              throw new IllegalStateException("environment path mismatch: " + path);
            }
            catalog.put(path, expected);
          }
        }
      }
      return Map.copyOf(catalog);
    } catch (final IOException error) {
      throw new IllegalStateException("cannot read public configuration catalog", error);
    }
  }

  @Override
  public int getOrder() {
    return Ordered.LOWEST_PRECEDENCE;
  }
}
