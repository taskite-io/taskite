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
package io.taskite.placementdriver.broker.registry;

import io.taskite.topology.BrokerPartitionState;

/** Current complete report evidence; lease validity is controlled by PD's registration state. */
public final class BrokerPartitionReports {
  private BrokerPartitionReports() {}

  public static boolean current(
      final BrokerRegistration registration, final BrokerPartitionState report) {
    final long observed = report.election().observedAtEpochMillis();
    return registration.status() != BrokerStatus.OFFLINE
        && observed > 0
        && observed >= registration.registeredAt().toEpochMilli()
        && observed == registration.lastRenewal().toEpochMilli();
  }
}
