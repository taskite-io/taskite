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
package io.taskite.broker;

import io.taskite.placementdriver.protocol.Broker;

/** PD protocol records used by Broker topology fixtures. */
final class BrokerTopologyTestRecords {
  private BrokerTopologyTestRecords() {}

  static Broker broker(final String brokerId, final int port, final int raftPort) {
    return Broker.newBuilder()
        .setBrokerId(brokerId)
        .setHost("127.0.0.1")
        .setPort(port)
        .setRaftPort(raftPort)
        .build();
  }
}
