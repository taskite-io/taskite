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
package io.taskite.placementdriver.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Descriptors;
import io.taskite.placementdriver.protocol.PlacementDriverOuterClass;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

final class PlacementDriverClientArchitectureTest {
  private static final Path CLIENT_SOURCE =
      Path.of("src/main/java/io/taskite/placementdriver/client");

  @Test
  void rootPackageContainsOnlyClientEntryPoints() throws IOException {
    try (Stream<Path> sources = Files.list(CLIENT_SOURCE)) {
      assertEquals(
          Set.of("Client.java", "Endpoint.java", "GrpcClient.java", "TopologyChangeListener.java"),
          sources
              .filter(PlacementDriverClientArchitectureTest::isJava)
              .map(path -> path.getFileName().toString())
              .collect(Collectors.toSet()));
    }
  }

  @Test
  void grpcClientDelegatesProtocolConversions() throws IOException {
    final String client = Files.readString(CLIENT_SOURCE.resolve("GrpcClient.java"));

    assertTrue(client.contains("PlacementDriverGrpc"));
    assertTrue(client.contains("PlacementDriverResponseMapper"));
    assertTrue(!client.contains(".setClusterId("));
    assertTrue(!client.contains("ExecuteCommand"));
    assertTrue(!client.contains("byte[]"));
    assertTrue(!client.contains("new ClusterConfig("));
    assertTrue(!client.contains("new Partition("));
  }

  @Test
  void implementationResponsibilitiesRemainSeparated() throws IOException {
    assertTrue(Files.isDirectory(CLIENT_SOURCE.resolve("mapper")));
    assertTrue(Files.isDirectory(CLIENT_SOURCE.resolve("request")));
    assertTrue(Files.isDirectory(CLIENT_SOURCE.resolve("response")));
    assertTrue(Files.isDirectory(CLIENT_SOURCE.resolve("topology")));
    assertTrue(Files.notExists(CLIENT_SOURCE.resolve("model")));
    assertTrue(Files.notExists(CLIENT_SOURCE.resolve("impl")));
    assertTrue(Files.notExists(CLIENT_SOURCE.resolve("transport")));
    assertTrue(Files.notExists(CLIENT_SOURCE.resolve("command")));
    assertTrue(Files.notExists(CLIENT_SOURCE.resolve("WireCodec.java")));
    assertTrue(Files.notExists(CLIENT_SOURCE.resolve("PlacementDriverRegistryRequestMapper.java")));
  }

  @Test
  void publicProtocolContainsOnlyReadableTypedFields() {
    final var descriptor = PlacementDriverOuterClass.getDescriptor();
    assertEquals(
        List.of("PlacementDriver"),
        descriptor.getServices().stream().map(service -> service.getName()).toList());
    descriptor
        .getMessageTypes()
        .forEach(
            message ->
                message
                    .getFields()
                    .forEach(
                        field -> {
                          assertTrue(
                              !field.getName().contains("_"),
                              () -> message.getName() + "." + field.getName());
                          assertTrue(
                              field.getJavaType()
                                  != Descriptors.FieldDescriptor.JavaType.BYTE_STRING,
                              () -> message.getName() + "." + field.getName());
                        }));
  }

  @Test
  void grpcClientRemainsAnOrchestrator() throws IOException {
    try (Stream<String> lines = Files.lines(CLIENT_SOURCE.resolve("GrpcClient.java"))) {
      assertTrue(lines.count() <= 500, "GrpcClient must remain within 500 lines");
    }
  }

  private static boolean isJava(final Path path) {
    return Files.isRegularFile(path) && path.getFileName().toString().endsWith(".java");
  }
}
