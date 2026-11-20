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
package io.taskite.engine.util.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

final class Variables {
  private static final ObjectMapper JSON =
      new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private static final TypeReference<Map<String, Object>> OBJECT = new TypeReference<>() {};

  private Variables() {}

  static Map<String, Object> read(final String json) {
    try {
      return Objects.requireNonNull(
          JSON.readValue(json, OBJECT), "Variables must be a JSON object");
    } catch (final JsonProcessingException | NullPointerException error) {
      throw new IllegalArgumentException("Variables must be a JSON object", error);
    }
  }

  static Map<String, Object> copy(final Map<String, Object> variables) {
    return new LinkedHashMap<>(Objects.requireNonNull(variables, "variables"));
  }

  static Map<String, Object> add(
      final Map<String, Object> variables, final String name, final Object value) {
    final var updated = copy(variables);
    updated.put(Objects.requireNonNull(name, "name"), value);
    return updated;
  }
}
