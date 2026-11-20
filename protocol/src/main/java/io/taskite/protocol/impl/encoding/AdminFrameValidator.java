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
package io.taskite.protocol.impl.encoding;

import io.taskite.protocol.record.MessageHeaderDecoder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.agrona.concurrent.UnsafeBuffer;

/** Checks variable lengths before generated SBE decoders allocate strings or arrays. */
public final class AdminFrameValidator {
  private final ByteBuffer buffer;

  private AdminFrameValidator(final byte[] bytes, final int offset) {
    buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    buffer.position(offset);
  }

  public static AdminFrameValidator frame(
      final byte[] bytes, final int templateId, final int blockLength, final int version) {
    if (bytes.length < MessageHeaderDecoder.ENCODED_LENGTH) {
      throw new IllegalArgumentException("truncated SBE header");
    }
    final var header = new MessageHeaderDecoder().wrap(new UnsafeBuffer(bytes), 0);
    if (header.schemaId() != 900
        || header.templateId() != templateId
        || header.version() != version
        || header.blockLength() != blockLength
        || bytes.length < MessageHeaderDecoder.ENCODED_LENGTH + blockLength) {
      throw new IllegalArgumentException("invalid administration SBE header");
    }
    return new AdminFrameValidator(bytes, MessageHeaderDecoder.ENCODED_LENGTH + blockLength);
  }

  public AdminFrameValidator data(final int count) {
    for (int i = 0; i < count; i++) {
      require(4);
      final long length = Integer.toUnsignedLong(buffer.getInt());
      if (length > buffer.remaining()) {
        throw new IllegalArgumentException("truncated SBE variable data");
      }
      buffer.position(buffer.position() + (int) length);
    }
    return this;
  }

  public void finish() {
    if (buffer.hasRemaining()) {
      throw new IllegalArgumentException("trailing administration data");
    }
  }

  private void require(final int length) {
    if (buffer.remaining() < length) {
      throw new IllegalArgumentException("truncated administration data");
    }
  }
}
