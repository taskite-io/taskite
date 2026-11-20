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
package io.taskite.cluster.raft.jraft.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.util.Endpoint;
import io.taskite.util.HostPort;
import org.junit.jupiter.api.Test;

final class JRaftSnapshotFileClientTest {
  @Test
  void aliasesPreserveIpv6PhysicalAddressAndDistinguishReusedPorts() {
    final var files = new JRaftSnapshotFileClient(new NodeOptions());
    final var ipv6 = files.register(new HostPort("::1", 26502));
    assertEquals(new Endpoint("[::1]", 26502), files.physical(ipv6));
    assertEquals(ipv6, files.register(new HostPort("[::1]", 26502)));
    final var ipv4 = files.register(new HostPort("127.0.0.1", 26502));
    assertNotEquals(ipv6, ipv4);
    assertEquals(new Endpoint("127.0.0.1", 26502), files.physical(ipv4));
    assertEquals(new Endpoint("[::1]", 26502), files.physical(ipv6));
  }

  @Test
  void aliasesAreLocalToEachFileClientAndUnknownTargetsAreRejected() {
    final var first = new JRaftSnapshotFileClient(new NodeOptions());
    final var second = new JRaftSnapshotFileClient(new NodeOptions());
    final var firstAlias = first.register(new HostPort("::1", 26502));
    assertThrows(IllegalStateException.class, () -> second.physical(firstAlias));
    final var secondAlias = second.register(new HostPort("127.0.0.1", 36502));
    assertEquals(new Endpoint("[::1]", 26502), first.physical(firstAlias));
    assertEquals(new Endpoint("127.0.0.1", 36502), second.physical(secondAlias));
    assertThrows(
        IllegalStateException.class, () -> first.physical(new Endpoint("127.0.0.1", 26502)));
  }
}
