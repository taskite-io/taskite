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
package io.taskite.placementdriver.processing.broker.registry;

import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.BrokerIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;
import java.time.Duration;

public final class BrokerProcessors {
  private BrokerProcessors() {}

  public static TypedRecordProcessors registerBrokerProcessors(
      final TypedRecordProcessors processors,
      final TypedRecordProcessorContext context,
      final Duration leaseTimeout) {
    return processors
        .onCommand(
            ValueType.BROKER_REGISTRATION,
            BrokerIntent.REGISTER,
            new BrokerRegisterProcessor(context))
        .onCommand(
            ValueType.BROKER_REGISTRATION, BrokerIntent.RENEW, new BrokerRenewProcessor(context))
        .onCommand(
            ValueType.BROKER_REGISTRATION,
            BrokerIntent.DEREGISTER,
            new BrokerDeregisterProcessor(context))
        .onCommand(
            ValueType.BROKER_REGISTRATION,
            BrokerIntent.MARK_OFFLINE,
            new BrokerMarkOfflineProcessor(context, leaseTimeout))
        .onCommand(
            ValueType.BROKER_REGISTRATION, BrokerIntent.DRAIN, new BrokerDrainProcessor(context));
  }
}
