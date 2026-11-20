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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.MeterRegistry;
import io.taskite.engine.metrics.MicrometerEngineMetrics;
import io.taskite.snapshot.SnapshotResult;
import io.taskite.snapshot.SnapshotTriggerReason;
import io.taskite.streamplatform.api.metrics.MicrometerCommandDispatchMetrics;
import io.taskite.streamplatform.api.metrics.MicrometerLogAppendMetrics;
import io.taskite.streamplatform.api.metrics.MicrometerSnapshotMetrics;
import io.taskite.streamplatform.api.metrics.MicrometerStreamProcessorMetrics;
import io.taskite.streamplatform.api.metrics.ProcessingMetrics;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

final class BrokerPrometheusEndpointTest {

  @Test
  void exposesTaskiteMetricsThroughActuatorPrometheus() throws Exception {
    final SpringApplication application = new SpringApplication(BrokerApplication.class);
    application.setAdditionalProfiles("broker");
    try (ConfigurableApplicationContext context =
        application.run(
            "--taskite.broker.enabled=false",
            "--taskite.broker.cluster.cluster-id=metrics-test",
            "--taskite.broker.cluster.broker-id=broker-test",
            "--server.port=-1",
            "--management.server.port=0",
            "--spring.main.banner-mode=off")) {
      final MeterRegistry registry = context.getBean(MeterRegistry.class);
      final var streamMetrics = new MicrometerStreamProcessorMetrics(registry, "metrics-test", 1);
      final var logMetrics = new MicrometerLogAppendMetrics(registry, "metrics-test", 1);
      final var engineMetrics = new MicrometerEngineMetrics(registry, "metrics-test", 1, () -> 3);
      final var commandMetrics = new MicrometerCommandDispatchMetrics(registry, "metrics-test", 1);
      final var snapshotMetrics = new MicrometerSnapshotMetrics(registry, "metrics-test", 1);
      final List<AutoCloseable> collectors =
          new ArrayList<>(
              List.of(streamMetrics, logMetrics, engineMetrics, commandMetrics, snapshotMetrics));
      // Use the same partition count as a loaded Broker, including all snapshot outcomes.
      for (int partition = 2; partition <= 15; partition++) {
        collectors.addAll(partitionMetrics(registry, partition));
      }
      streamMetrics.onProcessed(List.of());
      streamMetrics.onBatchProcessed(2, 1, 0, 0, 1_000_000);
      streamMetrics.onBatchRetried();
      streamMetrics.onBatchResult(ProcessingMetrics.BatchResultState.RECORDS);
      streamMetrics.onPostCommitCompleted(1_000_000);
      streamMetrics.onPhaseCompleted(ProcessingMetrics.Phase.STATE_COMMIT, 1_000_000);
      streamMetrics.onRecordPositions(17, 23);
      logMetrics.onBatchAppended(3, 4096);
      logMetrics.onBatchAppended(100_000, 32 * 1024 * 1024);
      commandMetrics.onSubmitted(0);
      commandMetrics.onSubmitted(100_000);
      commandMetrics.onRaftAppendCompleted(Duration.ofMinutes(2).toNanos());
      commandMetrics.onProcessingCompleted(Duration.ofMinutes(2).toNanos());
      streamMetrics.onBatchProcessed(100_000, 0, 0, 0, Duration.ofMinutes(2).toNanos());
      streamMetrics.onPostCommitCompleted(Duration.ofMinutes(2).toNanos());
      for (final var phase : ProcessingMetrics.Phase.values()) {
        streamMetrics.onPhaseCompleted(phase, Duration.ofMinutes(2).toNanos());
      }
      snapshotMetrics.onTriggered(SnapshotTriggerReason.SIZE);
      snapshotMetrics.onCompleted(
          SnapshotTriggerReason.SIZE,
          Duration.ofMillis(25),
          SnapshotResult.persisted(20, 2, 16_384),
          null);
      snapshotMetrics.onCompleted(
          SnapshotTriggerReason.INTERVAL,
          Duration.ofHours(1),
          SnapshotResult.persisted(20, 2, 16_384),
          null);
      snapshotMetrics.onCompleted(
          SnapshotTriggerReason.SIZE,
          Duration.ofHours(1),
          null,
          new IllegalStateException("failed"));
      snapshotMetrics.onCompleted(
          SnapshotTriggerReason.INTERVAL,
          Duration.ofMillis(1),
          SnapshotResult.skipped(20, 2),
          null);
      engineMetrics.onRecovered();
      registry
          .counter(
              MicrometerEngineMetrics.JOB_EVENTS,
              "cluster",
              "metrics-test",
              "partition",
              "1",
              "action",
              "completed",
              "job_type",
              "payment")
          .increment();
      registry
          .counter(
              MicrometerEngineMetrics.INCIDENT_EVENTS,
              "cluster",
              "metrics-test",
              "partition",
              "1",
              "action",
              "created")
          .increment();
      final int port =
          context.getEnvironment().getRequiredProperty("local.management.port", Integer.class);
      final HttpClient client = HttpClient.newHttpClient();
      final HttpRequest scrape =
          HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/actuator/prometheus"))
              .GET()
              .build();
      final HttpResponse<String> response =
          client.send(scrape, HttpResponse.BodyHandlers.ofString());

      assertEquals(200, response.statusCode());
      assertTrue(
          response.body().contains("taskite_stream_processor_records_total"), response::body);
      assertTrue(response.body().contains("taskite_stream_processor_record_count"), response::body);
      assertMetricExposed(response.body(), "taskite_stream_processor_batch_commands_bucket");
      assertMetricExposed(
          response.body(), "taskite_stream_processor_batch_duration_seconds_bucket");
      assertMetricExposed(response.body(), "taskite_stream_processor_batch_continuations_total");
      assertMetricExposed(response.body(), "taskite_stream_processor_batch_retries_total");
      assertMetricExposed(response.body(), "taskite_stream_processor_batch_results_total");
      assertMetricExposed(
          response.body(), "taskite_stream_processor_post_commit_duration_seconds_bucket");
      assertMetricExposed(
          response.body(), "taskite_stream_processor_phase_duration_seconds_bucket");
      assertMetricExposed(response.body(), "taskite_logstream_append_batch_records_bucket");
      assertMetricExposed(response.body(), "taskite_logstream_append_batch_bytes_bucket");
      assertMetricExposed(response.body(), "taskite_stream_partition_command_pending_bucket");
      assertMetricExposed(
          response.body(), "taskite_stream_partition_command_duration_seconds_bucket");
      assertMetricExposed(response.body(), "taskite_snapshot_duration_seconds_bucket");
      assertTrue(
          response.body().lines().filter(line -> line.startsWith("taskite_")).count() <= 6_000,
          "15 partitions must stay within the business metric sample budget");
      assertTrue(
          response.body().getBytes(StandardCharsets.UTF_8).length < 900_000,
          "15 partitions including JVM metrics must stay below the export size budget");
      assertOverflowPreserved(
          response.body(),
          "taskite_stream_partition_command_pending",
          Map.of(),
          "65536.0",
          100_000);
      assertOverflowPreserved(
          response.body(), "taskite_logstream_append_batch_records", Map.of(), "65536.0", 100_003);
      assertOverflowPreserved(
          response.body(),
          "taskite_logstream_append_batch_bytes",
          Map.of(),
          "1.6777216E7",
          32 * 1024 * 1024 + 4096);
      assertOverflowPreserved(
          response.body(), "taskite_stream_processor_batch_commands", Map.of(), "65536.0", 100_002);
      assertOverflowPreserved(
          response.body(),
          "taskite_stream_processor_batch_duration_seconds",
          Map.of(),
          "60.0",
          120.001);
      assertOverflowPreserved(
          response.body(),
          "taskite_stream_processor_post_commit_duration_seconds",
          Map.of(),
          "60.0",
          120.001);
      assertOverflowPreserved(
          response.body(),
          "taskite_stream_processor_phase_duration_seconds",
          Map.of("phase", "state_commit"),
          "60.0",
          120.001);
      assertOverflowPreserved(
          response.body(),
          "taskite_snapshot_duration_seconds",
          Map.of("result", "success"),
          "1800.0",
          3600.025);
      assertEquals(
          3600,
          sample(
              response.body(),
              "taskite_snapshot_duration_seconds_sum",
              Map.of("result", "failure")));
      assertEquals(
          1,
          sample(response.body(), "taskite_snapshot_attempt_total", Map.of("result", "skipped")));
      assertTrue(
          response
              .body()
              .lines()
              .filter(line -> line.startsWith("taskite_snapshot_duration_"))
              .noneMatch(line -> line.contains("reason=") || line.contains("result=\"skipped\"")));
      assertFalse(response.body().contains("phase=\"engine_batch\""));
      assertTrue(response.body().contains("state=\"pending\"} 6.0"), response::body);
      assertFalse(
          response.body().contains("taskite_element_instance_events_total"), response::body);
      assertTrue(response.body().contains("taskite_job_events_total"), response::body);
      assertTrue(response.body().contains("job_type=\"payment\""), response::body);
      assertTrue(response.body().contains("taskite_incident_events_total"), response::body);
      assertTrue(response.body().contains("taskite_pending_incidents"), response::body);
      assertTrue(response.body().contains("cluster=\"metrics-test\""), response::body);
      assertTrue(response.body().contains("broker=\"broker-test\""), response::body);
      assertMetricHasBrokerIdentity(response.body(), "process_cpu_usage");
      assertMetricHasBrokerIdentity(response.body(), "jvm_memory_used_bytes");
      assertMetricHasBrokerIdentity(response.body(), "jvm_memory_committed_bytes");
      assertMetricHasBrokerIdentity(response.body(), "jvm_memory_max_bytes");
      assertMetricHasBrokerIdentity(response.body(), "disk_total_bytes");
      assertMetricHasBrokerIdentity(response.body(), "disk_free_bytes");

      final var readiness =
          HttpClient.newHttpClient()
              .send(
                  HttpRequest.newBuilder(
                          URI.create("http://127.0.0.1:" + port + "/actuator/health/readiness"))
                      .GET()
                      .build(),
                  HttpResponse.BodyHandlers.ofString());
      assertEquals(503, readiness.statusCode(), readiness.body());
      assertTrue(readiness.body().contains("OUT_OF_SERVICE"), readiness.body());

      for (final AutoCloseable collector : collectors) {
        collector.close();
      }
      final var removed = client.send(scrape, HttpResponse.BodyHandlers.ofString());
      assertEquals(200, removed.statusCode());
      assertFalse(removed.body().contains("taskite_stream_processor_"));
      assertFalse(removed.body().contains("taskite_stream_partition_command_"));
      assertFalse(removed.body().contains("taskite_logstream_append_"));
      assertFalse(removed.body().contains("taskite_snapshot_"));
      assertFalse(removed.body().contains("taskite_pending_incidents"));
      // Leadership changes recreate collectors with the same labels in the same registry.
      final var recreated = partitionMetrics(registry, 1);
      final var fresh = client.send(scrape, HttpResponse.BodyHandlers.ofString());
      assertEquals(200, fresh.statusCode());
      assertEquals(
          0, sample(fresh.body(), "taskite_stream_processor_batch_commands_count", Map.of()));
      assertEquals(
          0,
          sample(
              fresh.body(),
              "taskite_snapshot_duration_seconds_count",
              Map.of("result", "success")));
      for (final AutoCloseable collector : recreated) {
        collector.close();
      }
    }
  }

  private static List<AutoCloseable> partitionMetrics(
      final MeterRegistry registry, final int partition) {
    return List.of(
        new MicrometerStreamProcessorMetrics(registry, "metrics-test", partition),
        new MicrometerLogAppendMetrics(registry, "metrics-test", partition),
        new MicrometerEngineMetrics(registry, "metrics-test", partition, () -> 0),
        new MicrometerCommandDispatchMetrics(registry, "metrics-test", partition),
        new MicrometerSnapshotMetrics(registry, "metrics-test", partition));
  }

  private static void assertOverflowPreserved(
      final String body,
      final String name,
      final Map<String, String> labels,
      final String finalBoundary,
      final double sum) {
    final var finite = new java.util.HashMap<>(labels);
    finite.put("le", finalBoundary);
    final var infinite = new java.util.HashMap<>(labels);
    infinite.put("le", "+Inf");
    assertEquals(1, sample(body, name + "_bucket", finite));
    assertEquals(2, sample(body, name + "_bucket", infinite));
    assertEquals(2, sample(body, name + "_count", labels));
    assertEquals(sum, sample(body, name + "_sum", labels), 0.000001);
  }

  private static double sample(
      final String body, final String name, final Map<String, String> labels) {
    final var samples =
        body.lines()
            .filter(line -> line.startsWith(name + "{"))
            .filter(line -> line.contains("partition=\"1\""))
            .filter(
                line ->
                    labels.entrySet().stream()
                        .allMatch(
                            label ->
                                line.contains(label.getKey() + "=\"" + label.getValue() + "\"")))
            .toList();
    assertEquals(1, samples.size(), () -> "Expected one sample for " + name + " " + labels);
    return Double.parseDouble(
        samples.getFirst().substring(samples.getFirst().lastIndexOf(' ') + 1));
  }

  private static void assertMetricHasBrokerIdentity(final String body, final String metricName) {
    assertTrue(
        body.lines()
            .anyMatch(
                line ->
                    line.startsWith(metricName + "{")
                        && line.contains("cluster=\"metrics-test\"")
                        && line.contains("broker=\"broker-test\"")),
        () -> metricName + " must expose cluster and broker labels");
  }

  private static void assertMetricExposed(final String body, final String metricName) {
    assertTrue(
        body.lines().anyMatch(line -> line.startsWith(metricName + "{")),
        () -> metricName + " must be exposed");
  }
}
