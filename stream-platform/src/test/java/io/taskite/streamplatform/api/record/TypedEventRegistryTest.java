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
package io.taskite.streamplatform.api.record;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.streamplatform.logstream.LogAppendEntry;
import io.taskite.streamplatform.logstream.LoggedEvent;
import org.junit.jupiter.api.Test;

final class TypedEventRegistryTest {
  @Test
  void exactIntentRegistrationOverridesValueTypeDefault() {
    final TypedEventRegistry registry =
        TypedEventRegistry.builder()
            .register(ValueType.TOPOLOGY, UnifiedRecordValue::new)
            .register(ValueType.TOPOLOGY, TopologyIntent.QUERY, TopologyQueryRecord::new)
            .build();

    assertInstanceOf(
        TopologyQueryRecord.class, registry.create(ValueType.TOPOLOGY, TopologyIntent.QUERY));
    assertInstanceOf(
        UnifiedRecordValue.class,
        registry.create(ValueType.TOPOLOGY, TopologyIntent.CONFIGURE_CLUSTER));
  }

  @Test
  void createsFreshInstances() {
    final TypedEventRegistry registry =
        TypedEventRegistry.builder().register(ValueType.JOB, JobRecord::new).build();

    assertNotSame(
        registry.create(ValueType.JOB, JobIntent.COMPLETE),
        registry.create(ValueType.JOB, JobIntent.COMPLETE));
  }

  @Test
  void rejectsDuplicateRegistration() {
    final TypedEventRegistry.Builder builder =
        TypedEventRegistry.builder().register(ValueType.JOB, JobRecord::new);

    assertThrows(
        IllegalArgumentException.class, () -> builder.register(ValueType.JOB, JobRecord::new));
  }

  @Test
  void rejectionUsesEmptyValueWithoutDomainFactory() {
    final TypedEventRegistry registry =
        TypedEventRegistry.builder()
            .register(
                ValueType.TOPOLOGY,
                () -> {
                  throw new AssertionError("rejection must not create a domain value");
                })
            .build();
    final RecordMetadata metadata =
        new RecordMetadata()
            .recordType(RecordType.COMMAND_REJECTION)
            .valueType(ValueType.TOPOLOGY)
            .intent(TopologyIntent.QUERY)
            .rejectionType(RejectionType.NOT_FOUND);
    final LoggedEvent event =
        LoggedEvent.of(LogAppendEntry.of(1L, metadata, new TopologyQueryRecord()), 2L, 1L, 0L);

    assertEquals("{}", registry.read(event, metadata).toJson());
  }
}
