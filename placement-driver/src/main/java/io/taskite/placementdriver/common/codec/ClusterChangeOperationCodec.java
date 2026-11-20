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
package io.taskite.placementdriver.common.codec;

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeStatus;
import io.taskite.placementdriver.broker.cluster.ClusterChangeOperation;
import io.taskite.protocol.impl.encoding.MsgPackConverter;

/** MessagePack codec for the durable aggregate cluster-change manifest. */
public final class ClusterChangeOperationCodec {

  private ClusterChangeOperationCodec() {}

  public static byte[] toBytes(final ClusterChangeOperation operation) {
    return MsgPackConverter.convertToMsgPack(operation);
  }

  public static ClusterChangeOperation fromBytes(final byte[] bytes) {
    try {
      final ClusterChangeOperation operation =
          MsgPackConverter.convertToObject(ClusterChangeOperation.class, bytes);
      // CONFIGURED applies all configuration changes and the complete, frozen plan list together.
      // Older writers left zero-plan operations PENDING even though no further work could advance
      // them.
      return operation.status() == BrokerClusterChangeStatus.PENDING
              && operation.planChangeIds().isEmpty()
          ? operation.withStatus(BrokerClusterChangeStatus.COMPLETED)
          : operation;
    } catch (final Exception error) {
      throw new IllegalStateException("failed to decode cluster change operation", error);
    }
  }
}
