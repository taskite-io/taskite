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
package io.taskite.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

final class HostPortTest {
  @Test
  void hostAndEndpointTextRoundTripWithoutDnsResolution() {
    for (final String host :
        List.of("127.0.0.1", "pd.example", "::1", "2001:db8::1", "fe80::1%en0")) {
      final var endpoint = new HostPort(host, 26501);
      assertEquals(endpoint, HostPort.parse(endpoint.toString()));
      assertEquals(host, HostPort.normalizeHost(HostPort.formatHost(host)));
    }
    assertEquals("[::1]:26501", HostPort.format("[::1]", 26501));
    assertEquals(new HostPort("::1", 26501), HostPort.parse("https://[::1]:26501"));
    assertEquals(new HostPort("2001:db8::1", 26501), HostPort.parse("2001:db8::1:26501"));
  }

  @Test
  void malformedEndpointIsRejected() {
    for (final String value :
        List.of(
            "",
            ":26501",
            "[::1:26501",
            "::1]:26501",
            "[[::1]]:26501",
            "pd:65536",
            "pd:-1",
            "pd:grpc",
            "pd:26501/path",
            "pd host:26501")) {
      assertThrows(IllegalArgumentException.class, () -> HostPort.parse(value), value);
    }
  }
}
