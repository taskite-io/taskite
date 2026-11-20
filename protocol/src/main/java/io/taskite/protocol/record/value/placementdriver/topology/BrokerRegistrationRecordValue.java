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
package io.taskite.protocol.record.value.placementdriver.topology;

import io.taskite.protocol.record.RecordValue;
import io.taskite.topology.MemberRole;
import java.util.List;

public interface BrokerRegistrationRecordValue extends RecordValue {
  String getClusterId();

  String getBrokerId();

  String getHost();

  int getPort();

  int getRaftPort();

  long getRenewalTimeoutMillis();

  long getExpectedLastRenewalEpochMillis();

  long getEventTimestampEpochMillis();

  String getVersion();

  List<? extends PartitionStateRecordValue> getPartitions();

  interface PartitionStateRecordValue {
    int getPartitionId();

    MemberRole getRole();

    long getTerm();
  }
}
