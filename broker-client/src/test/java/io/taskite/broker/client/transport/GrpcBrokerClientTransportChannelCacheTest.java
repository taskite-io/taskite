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
package io.taskite.broker.client.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.taskite.topology.BrokerEndpoint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** G2: the broker-client channel cache partitions and evicts channels by fenced-recovery epoch. */
final class GrpcBrokerClientTransportChannelCacheTest {
  private static final BrokerEndpoint ENDPOINT_A =
      new BrokerEndpoint("broker-1", "127.0.0.1", 26501, 26601);
  private static final BrokerEndpoint ENDPOINT_B =
      new BrokerEndpoint("broker-2", "127.0.0.1", 26502, 26602);

  private final GrpcBrokerClientTransport transport = new GrpcBrokerClientTransport();

  @AfterEach
  void tearDown() {
    transport.close();
  }

  @Test
  void reusesChannelForSameEndpointAndEpoch() {
    transport.openChannelForTesting(ENDPOINT_A, 0L);
    transport.openChannelForTesting(ENDPOINT_A, 0L);
    assertEquals(1, transport.openChannelCount());
  }

  @Test
  void keepsSeparateChannelsPerEndpoint() {
    transport.openChannelForTesting(ENDPOINT_A, 0L);
    transport.openChannelForTesting(ENDPOINT_B, 0L);
    assertEquals(2, transport.openChannelCount());
  }

  @Test
  void newerEpochEvictsSupersededChannelForSameEndpoint() {
    transport.openChannelForTesting(ENDPOINT_A, 0L);
    transport.openChannelForTesting(ENDPOINT_A, 1L);
    // The epoch-0 channel to the superseded generation is evicted; only the epoch-1 channel
    // remains.
    assertEquals(1, transport.openChannelCount());

    // A second endpoint is untouched by another endpoint's recovery.
    transport.openChannelForTesting(ENDPOINT_B, 0L);
    transport.openChannelForTesting(ENDPOINT_A, 2L);
    assertEquals(2, transport.openChannelCount());
  }

  @Test
  void staleEpochDoesNotDisturbCurrentChannel() {
    transport.openChannelForTesting(ENDPOINT_A, 2L);
    // A late request carrying an older epoch opens its own channel but must not evict the current
    // generation's channel (eviction only ever removes strictly-older epochs).
    transport.openChannelForTesting(ENDPOINT_A, 1L);
    assertEquals(2, transport.openChannelCount());
  }
}
