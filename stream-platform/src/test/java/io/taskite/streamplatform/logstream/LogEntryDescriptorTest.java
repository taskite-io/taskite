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
package io.taskite.streamplatform.logstream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import java.nio.ByteOrder;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class LogEntryDescriptorTest {
  @Test
  void preservesLittleEndianHeaderLayout() {
    final RecordMetadata metadata =
        new RecordMetadata()
            .recordType(RecordType.EVENT)
            .valueType(ValueType.JOB)
            .intent(JobIntent.CREATED);
    final LogAppendEntry entry =
        LogAppendEntry.ofProcessed(
            LogAppendEntry.of(
                0x0102030405060708L,
                metadata,
                new JobRecord().setJobType("type").setWorker("worker").setTenantId("tenant")));

    final LoggedEvent event =
        LoggedEvent.of(entry, 0x1112131415161718L, 0x2122232425262728L, 0x3132333435363738L);
    final byte[] actualHeader = new byte[LogEntryDescriptor.HEADER_LENGTH];
    event.valueBuffer().getBytes(0, actualHeader);

    final byte[] expectedHeader = new byte[LogEntryDescriptor.HEADER_LENGTH];
    final UnsafeBuffer expected = new UnsafeBuffer(expectedHeader);
    int offset = 0;
    expected.putByte(offset, LogEntryDescriptor.FLAG_SKIP_PROCESSING);
    offset += Byte.BYTES;
    expected.putLong(offset, 0x1112131415161718L, ByteOrder.LITTLE_ENDIAN);
    offset += Long.BYTES;
    expected.putLong(offset, 0x2122232425262728L, ByteOrder.LITTLE_ENDIAN);
    offset += Long.BYTES;
    expected.putLong(offset, 0x0102030405060708L, ByteOrder.LITTLE_ENDIAN);
    offset += Long.BYTES;
    expected.putLong(offset, 0x3132333435363738L, ByteOrder.LITTLE_ENDIAN);
    offset += Long.BYTES;
    expected.putInt(offset, metadata.getLength(), ByteOrder.LITTLE_ENDIAN);

    assertArrayEquals(expectedHeader, actualHeader);
    assertEquals(0x1112131415161718L, event.getPosition());
    assertEquals(0x2122232425262728L, event.getSourceEventPosition());
    assertEquals(0x0102030405060708L, event.getKey());
    assertEquals(0x3132333435363738L, event.getTimestamp());
    assertTrue(event.shouldSkipProcessing());
  }
}
