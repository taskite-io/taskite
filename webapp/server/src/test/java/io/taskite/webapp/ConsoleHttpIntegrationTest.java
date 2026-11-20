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
package io.taskite.webapp;

import static org.assertj.core.api.Assertions.assertThat;

import io.taskite.webapp.bootstrap.ConsoleAutoConfiguration;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.regex.Pattern;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Exercises real HTTP routing, resource JARs and activation conditions together. */
class ConsoleHttpIntegrationTest {
  private final HttpClient client = HttpClient.newHttpClient();

  @ParameterizedTest
  @CsvSource({"placement-driver,true,true", "placement-driver,false,false", "broker,true,false"})
  void consoleRoutesFollowActivationWithoutChangingExistingApi(
      final String profile, final boolean enabled, final boolean expected) throws Exception {
    try (final var context =
        new SpringApplicationBuilder(TestApplication.class)
            .profiles(profile)
            .properties(
                "server.port=0",
                "spring.main.banner-mode=off",
                "spring.web.resources.add-mappings=false",
                "taskite.console.enabled=" + enabled,
                "taskite.console.deployment-id=isolated",
                "taskite.pd.node-id=fixture-pd")
            .run()) {
      final int port = ((ServletWebServerApplicationContext) context).getWebServer().getPort();
      assertThat(get(port, "/api/v1/pd/health").statusCode()).isEqualTo(200);
      final var bootstrap = get(port, "/api/v1/console/bootstrap");
      assertThat(bootstrap.statusCode()).isEqualTo(expected ? 200 : 404);
      if (!expected) {
        assertThat(get(port, "/console/").statusCode()).isEqualTo(404);
        return;
      }
      assertThat(bootstrap.body())
          .contains("\"deploymentId\":\"isolated\"", "\"nodeId\":\"fixture-pd\"")
          .doesNotContain("uiReadOnly")
          .doesNotContainIgnoringCase("grafana");
      assertThat(bootstrap.headers().firstValue("cache-control")).hasValue("no-store");
      final var redirect = get(port, "/console");
      assertThat(redirect.statusCode()).isEqualTo(307);
      assertThat(redirect.headers().firstValue("location")).hasValue("/console/");
      for (final String path :
          new String[] {
            "/console/",
            "/console/pd",
            "/console/clusters/demo/partitions/1",
            "/console/workers",
            "/console/subscriptions/mirrors/9007199254741046",
            "/console/subscriptions/distributions/42",
            "/console/changes/pd/change-1",
            "/console/changes/clusters/demo/change-1"
          }) {
        final var page = get(port, path);
        assertThat(page.statusCode()).as(path).isEqualTo(200);
        assertThat(page.headers().firstValue("cache-control")).hasValue("no-store");
        assertThat(page.body()).contains("id=\"root\"");
      }
      final var index = get(port, "/console/");
      assertThat(get(port, "/console/favicon.svg").statusCode()).isEqualTo(200);
      for (final String path :
          new String[] {
            "/console/favicon.ico",
            "/console/apple-touch-icon.png",
            "/console/android-chrome-512x512.svg",
            "/console/android-chrome-maskable-512x512.svg"
          }) {
        final var icon = get(port, path);
        assertThat(icon.statusCode()).as(path).isEqualTo(200);
        assertThat(icon.headers().firstValue("content-type").orElse("")).startsWith("image/");
        assertThat(icon.headers().firstValue("cache-control")).hasValue("no-cache");
      }
      final var manifest = get(port, "/console/site.webmanifest");
      assertThat(manifest.statusCode()).isEqualTo(200);
      assertThat(manifest.headers().firstValue("content-type").orElse(""))
          .contains("application/manifest+json");
      assertThat(manifest.body()).contains("Taskite Console", "android-chrome-512x512.svg");
      assertThat(get(port, "/console/missing.svg").statusCode()).isEqualTo(404);
      assertThat(get(port, "/console/licenses.txt").body())
          .contains("MIT", "lossless-json", "react -");
      final var matcher =
          Pattern.compile("src=\"(/console/assets/[^\"]+\\.js)\"").matcher(index.body());
      assertThat(matcher.find()).isTrue();
      final var asset = get(port, matcher.group(1));
      assertThat(asset.statusCode()).isEqualTo(200);
      assertThat(asset.headers().firstValue("cache-control").orElse(""))
          .contains("max-age=31536000", "immutable");
      assertThat(get(port, "/console/assets/missing.js").statusCode()).isEqualTo(404);
      assertThat(get(port, "/api/v1/missing").statusCode()).isEqualTo(404);
    }
  }

  private HttpResponse<String> get(final int port, final String path) throws Exception {
    return client.send(
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).GET().build(),
        HttpResponse.BodyHandlers.ofString());
  }

  @SpringBootConfiguration
  @EnableAutoConfiguration
  @Import({ConsoleAutoConfiguration.class, ExistingApi.class})
  static class TestApplication {}

  @RestController
  static class ExistingApi {
    @GetMapping("/api/v1/pd/health")
    String health() {
      return "existing API";
    }
  }
}
