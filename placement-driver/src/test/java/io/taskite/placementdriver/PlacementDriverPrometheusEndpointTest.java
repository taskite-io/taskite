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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

final class PlacementDriverPrometheusEndpointTest {

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void exposesPlacementDriverMetricsThroughActuator(@TempDir final Path dataDirectory)
      throws Exception {
    final int raftPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      raftPort = socket.getLocalPort();
    }
    try (ConfigurableApplicationContext context =
        new SpringApplicationBuilder(PlacementDriverApplication.class)
            .run(
                "--server.port=0",
                "--spring.main.banner-mode=off",
                "--management.endpoints.enabled-by-default=false",
                "--management.endpoints.web.exposure.include=health,prometheus",
                "--management.endpoint.health.enabled=true",
                "--management.endpoint.prometheus.enabled=true",
                "--management.prometheus.metrics.export.enabled=true",
                "--taskite.pd.node-id=meta-metrics",
                "--taskite.pd.network.host=127.0.0.1",
                "--taskite.pd.network.grpc-port=0",
                "--taskite.pd.network.raft-port=" + raftPort,
                "--taskite.pd.consensus.election-timeout=300ms",
                "--taskite.pd.consensus.heartbeat-interval=100ms",
                "--taskite.pd.storage.data-dir=" + dataDirectory,
                "--taskite.pd.consensus.bootstrap-peers=")) {
      final int port =
          context.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
      final HttpResponse<String> response =
          HttpClient.newHttpClient()
              .send(
                  HttpRequest.newBuilder(
                          URI.create("http://127.0.0.1:" + port + "/actuator/prometheus"))
                      .GET()
                      .build(),
                  HttpResponse.BodyHandlers.ofString());

      assertEquals(200, response.statusCode());
      assertTrue(response.body().contains("taskite_pd_cluster_health"), response::body);
      assertTrue(response.body().contains("taskite_pd_raft_role"), response::body);
      assertTrue(response.body().contains("taskite_pd_cluster_member_role"), response::body);
      assertTrue(response.body().contains("taskite_pd_managed_clusters"), response::body);
      assertTrue(response.body().contains("pd_node=\"meta-metrics\""), response::body);
    }
  }
}
