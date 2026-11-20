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

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Round-trips the batch payload framing that lets one raft entry carry multiple business records.
 */
class BatchPayloadCodecTest {

  @Test
  void roundTripsMultipleRecords() {
    final List<byte[]> records =
        List.of(
            "first".getBytes(StandardCharsets.UTF_8),
            new byte[] {0, 1, 2, 3, 4},
            "third-record".getBytes(StandardCharsets.UTF_8));

    final byte[] payload = BatchPayloadCodec.encode(records);
    final List<byte[]> decoded = BatchPayloadCodec.decode(payload);

    assertEquals(records.size(), decoded.size());
    for (int i = 0; i < records.size(); i++) {
      assertArrayEquals(records.get(i), decoded.get(i), "record " + i);
    }
  }

  @Test
  void roundTripsSingleRecord() {
    final List<byte[]> records = List.of("only".getBytes(StandardCharsets.UTF_8));

    final List<byte[]> decoded = BatchPayloadCodec.decode(BatchPayloadCodec.encode(records));

    assertEquals(1, decoded.size());
    assertArrayEquals(records.get(0), decoded.get(0));
  }

  @Test
  void roundTripsEmptyBatch() {
    final List<byte[]> decoded = BatchPayloadCodec.decode(BatchPayloadCodec.encode(List.of()));
    assertEquals(0, decoded.size());
  }
}
