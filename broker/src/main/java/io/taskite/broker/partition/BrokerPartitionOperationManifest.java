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
package io.taskite.broker.partition;

import io.taskite.topology.RaftConfigurationReference;
import java.util.Objects;

public record BrokerPartitionOperationManifest(
    Identity identity, Result result, Evidence evidence) {

  public BrokerPartitionOperationManifest {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(result, "result");
    Objects.requireNonNull(evidence, "evidence");
  }

  public int partitionId() {
    return identity.partitionId();
  }

  public String opId() {
    return identity.opId();
  }

  public BrokerPartitionOperationStatus status() {
    return result.status();
  }

  public long completedAtEpochMillis() {
    return result.completedAtEpochMillis();
  }

  public String errorMessage() {
    return result.errorMessage();
  }

  public long configurationIndex() {
    return evidence.configuration().index();
  }

  public String configurationDigest() {
    return evidence.configuration().digest();
  }

  public long recoveryEpoch() {
    return evidence.recoveryEpoch();
  }

  public record Identity(int partitionId, String opId) {
    public Identity {
      opId = opId == null ? "" : opId;
    }
  }

  public record Result(
      BrokerPartitionOperationStatus status, long completedAtEpochMillis, String errorMessage) {
    public Result {
      status = status == null ? BrokerPartitionOperationStatus.NONE : status;
      errorMessage = errorMessage == null ? "" : errorMessage;
    }
  }

  public record Evidence(RaftConfigurationReference configuration, long recoveryEpoch) {
    public static final Evidence UNKNOWN = new Evidence(RaftConfigurationReference.UNKNOWN, 0);

    public Evidence(
        final long configurationIndex, final String configurationDigest, final long recoveryEpoch) {
      this(new RaftConfigurationReference(configurationIndex, configurationDigest), recoveryEpoch);
    }

    public Evidence {
      Objects.requireNonNull(configuration, "configuration");
    }
  }
}
