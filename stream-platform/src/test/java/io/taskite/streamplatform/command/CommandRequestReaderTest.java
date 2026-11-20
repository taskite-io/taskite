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
package io.taskite.streamplatform.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.encoding.ExecuteCommandRequest;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.logstream.LoggedEvent;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class CommandRequestReaderTest {
  @Test
  void routingPlaceholderUsesTheResolvedClusterAndExplicitPartition() {
    final var reader =
        new CommandRequestReader(
            TypedEventRegistry.builder()
                .register(ValueType.COMMAND_DISTRIBUTION, NoopValue::new)
                .build(),
            12);
    final var request =
        new ExecuteCommandRequest()
            .setKey(Protocol.encodeKey(Protocol.PLACEMENT_DRIVER_CLUSTER_KEY, 1, 1))
            .setPartitionId(9)
            .setValueType(ValueType.COMMAND_DISTRIBUTION)
            .setIntent(CommandDistributionIntent.ACKNOWLEDGE);
    final var bytes = new byte[request.getLength()];
    request.write(new UnsafeBuffer(bytes), 0);
    reader.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
    assertEquals(Protocol.encodeKey(12, 9, 0), reader.key());
    assertEquals(1, reader.metadata().getRecordVersion());
  }

  @Test
  void entityKeyMustMatchResolvedClusterAndExplicitPartition() {
    final var reader =
        new CommandRequestReader(
            TypedEventRegistry.builder().register(ValueType.JOB, NoopValue::new).build(), 12);
    final var request =
        new ExecuteCommandRequest()
            .setKey(Protocol.encodeKey(11, 9, 1))
            .setPartitionId(9)
            .setValueType(ValueType.JOB);
    final var bytes = new byte[request.getLength()];
    request.write(new UnsafeBuffer(bytes), 0);
    assertThrows(
        IllegalArgumentException.class,
        () -> reader.wrap(new UnsafeBuffer(bytes), 0, bytes.length));
  }

  @Test
  void recordKeepsMetadataSnapshotWhenReaderIsReused() {
    final CommandRequestReader reader =
        new CommandRequestReader(
            TypedEventRegistry.builder()
                .register(ValueType.COMMAND_DISTRIBUTION, NoopValue::new)
                .build(),
            0);
    final byte[] firstRequest =
        requestBytes(ValueType.COMMAND_DISTRIBUTION, CommandDistributionIntent.START);

    reader.reset();
    reader.wrap(new UnsafeBuffer(firstRequest), 0, firstRequest.length);
    reader.metadata().requestId("first-request");
    final LoggedEvent firstRecord = reader.record();
    final int firstMetadataLength = firstRecord.metadata().getLength();

    final byte[] secondRequest =
        requestBytes(ValueType.COMMAND_DISTRIBUTION, CommandDistributionIntent.ACKNOWLEDGE);
    reader.reset();
    reader.wrap(new UnsafeBuffer(secondRequest), 0, secondRequest.length);
    reader.metadata().requestId("second-request-with-a-different-length");

    assertEquals("first-request", firstRecord.metadata().getRequestId());
    assertEquals(ValueType.COMMAND_DISTRIBUTION, firstRecord.metadata().getValueType());
    assertEquals(CommandDistributionIntent.START, firstRecord.metadata().getIntent());
    assertEquals(firstMetadataLength, firstRecord.metadata().getLength());
  }

  private static byte[] requestBytes(final ValueType valueType, final Intent intent) {
    final DirectBuffer emptyValue = new UnsafeBuffer(new byte[0]);
    final ExecuteCommandRequest request =
        new ExecuteCommandRequest()
            .setPartitionId(1)
            .setValueType(valueType)
            .setIntent(intent)
            .setValue(emptyValue, 0, 0);
    final byte[] bytes = new byte[request.getLength()];
    request.write(new UnsafeBuffer(bytes), 0);
    return bytes;
  }

  private static final class NoopValue extends UnifiedRecordValue {
    @Override
    public void wrap(final DirectBuffer buffer, final int offset, final int length) {}
  }
}
