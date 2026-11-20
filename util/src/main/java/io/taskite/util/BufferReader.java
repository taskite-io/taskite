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
package io.taskite.util;

import org.agrona.DirectBuffer;

/**
 * Implementations may expose methods for access to properties from the buffer that is read. The
 * reader is a <em>view</em> on the buffer Any concurrent changes to the underlying buffer become
 * immediately visible to the reader.
 */
public interface BufferReader {
  /**
   * Wraps a buffer for read access.
   *
   * @param buffer the buffer to read from
   * @param offset the offset at which to start reading
   * @param length the length of the values to read
   */
  void wrap(DirectBuffer buffer, int offset, int length);

  /**
   * Copies the contents of {@code source} into a newly allocated buffer before reading it back.
   *
   * @param source the source writer
   * @throws NullPointerException if source is null
   */
  default void copyFrom(final BufferWriter source) {
    BufferUtil.copy(source, this);
  }
}
