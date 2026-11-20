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
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import io.taskite.topology.RegistrationStatus;
import java.util.List;
import java.util.Map;

public interface TopologyRecordValue extends RecordValue {
  String getClusterId();

  long getVersion();

  int getClusterSize();

  int getPartitionSize();

  int getReplicationFactor();

  long getUpdatedAtEpochMillis();

  List<? extends BrokerEndpointRecordValue> getBrokers();

  List<? extends PartitionRecordValue> getPartitions();

  interface BrokerEndpointRecordValue {
    String getBrokerId();

    String getHost();

    int getPort();

    int getRaftPort();

    String getVersion();

    long getRegisteredAtEpochMillis();

    long getLastRenewalEpochMillis();

    long getRenewalTimeoutMillis();

    RegistrationStatus getStatus();

    List<? extends BrokerPartitionRecordValue> getPartitions();
  }

  interface BrokerPartitionRecordValue {
    int getPartitionId();

    MemberRole getRole();

    long getTerm();

    BrokerPartitionLifecycle getLifecycleState();

    long getMatchIndex();

    long getLag();

    boolean isReadyForPromotion();

    int getPriority();

    String getLastOpId();

    BrokerTopologyOperationStatus getLastOpStatus();

    long getRecoveryEpoch();
  }

  interface PartitionRecordValue {
    boolean isServiceReady();

    int getPartitionId();

    String getLeaderId();

    List<String> getReplicas();

    List<String> getLearners();

    long getTerm();

    long getRecoveryEpoch();

    PartitionStatus getStatus();

    Map<String, Integer> getPriorities();
  }
}
