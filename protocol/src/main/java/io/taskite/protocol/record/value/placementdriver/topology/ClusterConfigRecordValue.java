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
import java.util.List;

public interface ClusterConfigRecordValue extends RecordValue {
  String getClusterId();

  long getClusterKey();

  int getClusterSize();

  int getPartitionSize();

  int getReplicationFactor();

  List<String> getVoterRoster();

  List<String> getRemovedBrokers();

  String getChangeId();

  long getSourceTopologyVersion();

  int getSourceClusterSize();

  int getSourcePartitionSize();

  int getSourceReplicationFactor();

  List<String> getSourceVoterRoster();

  List<String> getEncodedChangePlans();

  boolean isRosterManaged();
}
