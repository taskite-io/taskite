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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

final class PlacementDriverEndpointTest {
  @Test
  void endpointsSeparateHostFromPort() {
    assertEquals(
        new PlacementDriverEndpoint("pd.example", 26501),
        PlacementDriverEndpoint.parse("https://pd.example:26501"));
    assertEquals(
        new PlacementDriverEndpoint("::1", 26501), PlacementDriverEndpoint.parse(" [::1]:26501 "));
    assertEquals(
        new PlacementDriverEndpoint("2001:db8::1", 26501),
        PlacementDriverEndpoint.parse("2001:db8::1:26501"));
  }

  @Test
  void incompleteEndpointIsRejected() {
    assertThrows(IllegalArgumentException.class, () -> PlacementDriverEndpoint.parse("pd.example"));
    assertThrows(
        IllegalArgumentException.class, () -> PlacementDriverEndpoint.parse("pd.example:grpc"));
  }
}
