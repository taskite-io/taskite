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
package io.taskite.webapp.runtime;

import java.net.URI;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.server.ResponseStatusException;

/** Only known HTML navigation prefixes reach the SPA; assets and API failures remain real 404s. */
@Controller
public class ConsoleIndexController {
  private final Resource index = new ClassPathResource("META-INF/resources/console/index.html");

  @GetMapping("/console")
  public ResponseEntity<Void> redirect() {
    return ResponseEntity.status(HttpStatus.TEMPORARY_REDIRECT)
        .location(URI.create("/console/"))
        .cacheControl(CacheControl.noStore())
        .build();
  }

  @GetMapping(value = "/console/licenses.txt", produces = MediaType.TEXT_PLAIN_VALUE)
  public ResponseEntity<Resource> licenses() {
    return ResponseEntity.ok()
        .contentType(MediaType.TEXT_PLAIN)
        .cacheControl(CacheControl.noStore())
        .body(new ClassPathResource("META-INF/resources/console/licenses.txt"));
  }

  @GetMapping(value = "/console/favicon.svg", produces = "image/svg+xml")
  public ResponseEntity<Resource> favicon() {
    return ResponseEntity.ok()
        .contentType(MediaType.parseMediaType("image/svg+xml"))
        .cacheControl(CacheControl.noCache())
        .body(new ClassPathResource("META-INF/resources/console/favicon.svg"));
  }

  @GetMapping(value = "/console/site.webmanifest", produces = "application/manifest+json")
  public ResponseEntity<Resource> manifest() {
    return ResponseEntity.ok()
        .contentType(MediaType.parseMediaType("application/manifest+json"))
        .cacheControl(CacheControl.noCache())
        .body(new ClassPathResource("META-INF/resources/console/site.webmanifest"));
  }

  @GetMapping(
      value = {
        "/console/",
        "/console/index.html",
        "/console/pd",
        "/console/clusters",
        "/console/clusters/**",
        "/console/workers",
        "/console/subscriptions/**",
        "/console/changes",
        "/console/changes/**"
      },
      produces = MediaType.TEXT_HTML_VALUE)
  public ResponseEntity<Resource> index() {
    if (!index.exists()) {
      throw new ResponseStatusException(
          HttpStatus.SERVICE_UNAVAILABLE, "Console assets are missing from the distribution");
    }
    return ResponseEntity.ok()
        .contentType(MediaType.TEXT_HTML)
        .cacheControl(CacheControl.noStore())
        .body(index);
  }
}
