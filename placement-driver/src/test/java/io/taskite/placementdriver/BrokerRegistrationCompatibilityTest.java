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
package io.taskite.placementdriver;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.record.MessageHeaderDecoder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class BrokerRegistrationCompatibilityTest {
  @Test
  void rejectsVersionEightRecordWithoutExplicitRaftPort() {
    final BrokerRegistrationRecord current =
        new BrokerRegistrationRecord()
            .setClusterId("demo")
            .setBrokerId("broker-0")
            .setHost("127.0.0.1")
            .setPort(26501)
            .setRaftPort(26601)
            .setVersion("test")
            .setEventTimestampEpochMillis(1234);
    final byte[] versionEight = removeRaftPort(encode(current));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new BrokerRegistrationRecord()
                .wrap(new UnsafeBuffer(versionEight), 0, versionEight.length));
  }

  private static byte[] removeRaftPort(final byte[] current) {
    final int headerLength = MessageHeaderDecoder.ENCODED_LENGTH;
    final ByteBuffer header = ByteBuffer.wrap(current).order(ByteOrder.LITTLE_ENDIAN);
    final int currentBlockLength = Short.toUnsignedInt(header.getShort(0));
    final int versionEightBlockLength = currentBlockLength - Integer.BYTES;
    final byte[] versionEight = Arrays.copyOf(current, current.length - Integer.BYTES);
    System.arraycopy(
        current,
        headerLength + currentBlockLength,
        versionEight,
        headerLength + versionEightBlockLength,
        current.length - headerLength - currentBlockLength);
    final ByteBuffer versionEightHeader =
        ByteBuffer.wrap(versionEight).order(ByteOrder.LITTLE_ENDIAN);
    versionEightHeader.putShort(0, (short) versionEightBlockLength);
    versionEightHeader.putShort(6, (short) 8);
    return versionEight;
  }

  private static byte[] encode(final BrokerRegistrationRecord record) {
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(record.getLength());
    record.write(buffer, 0);
    return Arrays.copyOf(buffer.byteArray(), record.getLength());
  }
}
