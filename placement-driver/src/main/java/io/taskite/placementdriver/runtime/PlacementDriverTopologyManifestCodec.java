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
package io.taskite.placementdriver.runtime;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

final class PlacementDriverTopologyManifestCodec {
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .findAndRegisterModules()
          .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

  private PlacementDriverTopologyManifestCodec() {}

  static byte[] encode(final PlacementDriverTopologyManifest manifest) {
    final PlacementDriverTopologyManifest checksummed = manifest.withChecksum(checksum(manifest));
    try {
      return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(checksummed);
    } catch (final JsonProcessingException error) {
      throw new IllegalStateException("failed to encode placement-driver topology manifest", error);
    }
  }

  static PlacementDriverTopologyManifest decode(final byte[] bytes) {
    try {
      final PlacementDriverTopologyManifest manifest =
          MAPPER.readValue(bytes, PlacementDriverTopologyManifest.class);
      final String expected = checksum(manifest);
      if (manifest.checksum() == null
          || !"SHA-256".equals(manifest.checksum().algorithm())
          || !MessageDigest.isEqual(
              expected.getBytes(StandardCharsets.US_ASCII),
              manifest.checksum().value().getBytes(StandardCharsets.US_ASCII))) {
        throw new IllegalArgumentException("placement-driver topology manifest checksum mismatch");
      }
      return manifest;
    } catch (final IOException error) {
      throw new IllegalArgumentException(
          "failed to decode placement-driver topology manifest", error);
    }
  }

  static String checksum(final PlacementDriverTopologyManifest manifest) {
    final JsonNode tree = MAPPER.valueToTree(manifest);
    ((ObjectNode) tree).remove("checksum");
    final byte[] canonical;
    try {
      canonical = MAPPER.writeValueAsBytes(canonicalValue(tree));
    } catch (final JsonProcessingException error) {
      throw new IllegalStateException("failed to canonicalize topology manifest", error);
    }
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical));
    } catch (final NoSuchAlgorithmException error) {
      throw new IllegalStateException("SHA-256 is not available", error);
    }
  }

  private static Object canonicalValue(final JsonNode node) {
    if (node.isObject()) {
      final Map<String, Object> sorted = new TreeMap<>();
      node.fields()
          .forEachRemaining(entry -> sorted.put(entry.getKey(), canonicalValue(entry.getValue())));
      return sorted;
    }
    if (node.isArray()) {
      final List<Object> values = new ArrayList<>();
      node.forEach(value -> values.add(canonicalValue(value)));
      return values;
    }
    if (node.isTextual()) {
      return node.textValue();
    }
    if (node.isBoolean()) {
      return node.booleanValue();
    }
    if (node.isIntegralNumber()) {
      return node.longValue();
    }
    if (node.isFloatingPointNumber()) {
      return node.decimalValue();
    }
    return null;
  }
}
