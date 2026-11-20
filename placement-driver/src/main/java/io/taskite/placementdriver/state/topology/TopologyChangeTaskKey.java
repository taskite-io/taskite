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
package io.taskite.placementdriver.state.topology;

import io.taskite.db.DbKey;
import io.taskite.db.impl.DbInt;
import io.taskite.db.impl.DbString;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyChangeTaskRecord.Key;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyChangeTaskRecord.Scope;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/** [scope, clusterId, changeId, partitionId, operationId], without delimiter ambiguity. */
final class TopologyChangeTaskKey implements DbKey {
  private final DbInt scope = new DbInt();
  private final DbString cluster = new DbString();
  private final DbString change = new DbString();
  private final DbInt partition = new DbInt();
  private final DbString operation = new DbString();
  private final DbKey[] parts = {scope, cluster, change, partition, operation};

  TopologyChangeTaskKey() {
    wrapKey(Scope.BROKER_PARTITION, "", "", 0, "");
  }

  TopologyChangeTaskKey wrapKey(final Key key) {
    return wrapKey(
        key.scope(), key.clusterId(), key.changeId(), key.partitionId(), key.operationId());
  }

  TopologyChangeTaskKey wrapKey(
      final Scope taskScope,
      final String clusterId,
      final String changeId,
      final int partitionId,
      final String operationId) {
    scope.wrapInt(taskScope.ordinal());
    cluster.wrapString(clusterId);
    change.wrapString(changeId);
    partition.wrapInt(partitionId);
    operation.wrapString(operationId);
    return this;
  }

  Key key() {
    return new Key(
        Scope.values()[scope.getValue()],
        cluster.toString(),
        change.toString(),
        partition.getValue(),
        operation.toString());
  }

  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    int position = offset;
    for (final var part : parts) {
      part.wrap(buffer, position, length - (position - offset));
      position += part.getLength();
    }
  }

  @Override
  public int getLength() {
    int length = 0;
    for (final var part : parts) {
      length += part.getLength();
    }
    return length;
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    int position = offset;
    for (final var part : parts) {
      part.write(buffer, position);
      position += part.getLength();
    }
  }
}
