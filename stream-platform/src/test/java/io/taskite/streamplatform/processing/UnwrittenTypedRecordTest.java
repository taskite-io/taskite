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
package io.taskite.streamplatform.processing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.logstream.LogAppendEntry;
import org.junit.jupiter.api.Test;

final class UnwrittenTypedRecordTest {
  @Test
  void exposesTypedCommandWithoutInventingLogCoordinates() {
    final JobRecord value = new JobRecord().setJobType("payment").setTenantId("tenant");
    final RecordMetadata metadata =
        new RecordMetadata()
            .requestStreamId("stream-1")
            .requestId("request-1")
            .recordType(RecordType.COMMAND)
            .valueType(ValueType.JOB)
            .intent(JobIntent.FAIL);
    final UnwrittenTypedRecord record =
        new UnwrittenTypedRecord(
            3,
            LogAppendEntry.of(42L, metadata, value),
            TypedEventRegistry.builder().register(ValueType.JOB, JobRecord::new).build());

    assertEquals(42L, record.getKey());
    assertEquals(JobIntent.FAIL, record.getIntent());
    assertEquals("payment", ((JobRecord) record.getValue()).getType());
    assertThrows(UnsupportedOperationException.class, record::getPosition);
    assertThrows(UnsupportedOperationException.class, record::getTimestamp);
  }
}
