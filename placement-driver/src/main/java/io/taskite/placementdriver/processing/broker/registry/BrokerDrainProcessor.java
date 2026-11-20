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

import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.BrokerIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;

public final class BrokerDrainProcessor implements TypedRecordProcessor<BrokerRegistrationRecord> {
  private final Writers writers;

  public BrokerDrainProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
  }

  @Override
  public void processRecord(final TypedRecord<BrokerRegistrationRecord> record) {
    writers
        .state()
        .appendFollowUpEvent(
            record.getKey(),
            BrokerIntent.DRAINED,
            record.getValue().setEventTimestampEpochMillis(record.getTimestamp()));
  }
}
