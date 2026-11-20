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
package io.taskite.placementdriver.state.broker;

import io.taskite.db.DbValue;
import java.nio.charset.StandardCharsets;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/** Persists the removed incarnation and preserves the legacy voter-only exclusion marker. */
public final class BrokerPlacementExclusionRecordValue implements DbValue {
  private Exclusion record = new Exclusion("", false);
  private byte[] bytes = new byte[] {2};

  public Exclusion getRecord() {
    return record;
  }

  public void setRecord(final Exclusion record) {
    if (record.legacy()) {
      bytes = new byte[] {1};
    } else {
      final byte[] incarnation = record.incarnationId().getBytes(StandardCharsets.UTF_8);
      bytes = new byte[incarnation.length + 1];
      bytes[0] = 2;
      System.arraycopy(incarnation, 0, bytes, 1, incarnation.length);
    }
    this.record = record;
  }

  @Override
  public int getLength() {
    return bytes.length;
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    buffer.putBytes(offset, bytes);
  }

  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    final byte[] encoded = new byte[length];
    buffer.getBytes(offset, encoded);
    final boolean legacy = length == 1 && encoded[0] == 1;
    final var decoded =
        new Exclusion(
            legacy ? "" : new String(encoded, 1, length - 1, StandardCharsets.UTF_8), legacy);
    bytes = encoded;
    record = decoded;
  }

  public record Exclusion(String incarnationId, boolean legacy) {}
}
